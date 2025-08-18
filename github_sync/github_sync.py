from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import asyncio
import re
import contextlib
import logging
import functools
import json
import hashlib

import discord
import aiohttp
from redbot.core import commands, Config
from redbot.core.bot import Red
from discord.ext import tasks
from github import Github, GithubException
try:
    from github import Auth as GithubAuth  # PyGithub >= 2.0
except Exception:  # pragma: no cover - fallback for older versions
    GithubAuth = None  # type: ignore
from .helpers import map_github_labels_to_discord_tags


DISCORD_MESSAGE_LINK_RE = re.compile(r"https://discord\.com/channels/(\d+)/(\d+)/(\d+)")

# GraphQL queries for efficient data fetching
REPOSITORY_QUERY = """
query($owner: String!, $repo: String!, $issuesCursor: String, $prsCursor: String) {
  repository(owner: $owner, name: $repo) {
    owner {
      login
      avatarUrl
    }
    labels(first: 100) {
      nodes {
        name
      }
    }
    issues(first: 50, after: $issuesCursor, states: [OPEN, CLOSED], orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        title
        state
        stateReason
        locked
        url
        body
        createdAt
        updatedAt
        author {
          login
          avatarUrl
        }
        labels(first: 20) {
          nodes {
            name
          }
        }
        comments(first: 100) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            author {
              login
              avatarUrl
            }
            body
            url
            updatedAt
          }
        }
      }
    }
    pullRequests(first: 50, after: $prsCursor, states: [OPEN, CLOSED, MERGED], orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        number
        title
        state
        merged
        locked
        url
        body
        createdAt
        updatedAt
        author {
          login
          avatarUrl
        }
        labels(first: 20) {
          nodes {
            name
          }
        }
        comments(first: 100) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            author {
              login
              avatarUrl
            }
            body
            url
            updatedAt
          }
        }
      }
    }
  }
}
"""


class GitHubSync(commands.Cog):
    """
    Sync GitHub issues and pull requests with Discord forum posts.

    - Creates forum posts for GitHub issues/PRs with correct initial state (archived if closed/merged)
    - Syncs comments between GitHub and Discord (configurable direction)
    - Syncs Discord forum tags with GitHub labels (configurable direction)
    - Auto-applies status tags: Issues get "open"/"closed"/"not resolved", PRs get "open"/"closed"/"merged"
    - Prevents feedback loops: only user-initiated Discord changes sync back to GitHub
    - Configurable sync direction: GitHub→Discord only, or bidirectional
    """

    def __init__(self, bot: Red) -> None:
        self.bot = bot
        self.config = Config.get_conf(self, identifier=908039527271104514, force_registration=True)
        self.log = logging.getLogger(f"red.{__name__}")

        # Batched config system to prevent excessive file writes during sync
        self._pending_config_updates: Dict[str, Any] = {}
        self._batch_config_mode = False

        # Main group: shared configs
        self.config.register_guild(
            github_token=None,  # GitHub PAT
            github_owner=None,  # owner or org
            github_repo=None,  # repo name only
            poll_enabled=False,  # disabled by default
            poll_interval=60,  # seconds (faster default to keep threads fresh)
            discord_to_github_enabled=True,  # enable Discord → GitHub sync by default
            last_sync_iso=None,
            # optional webhook secret if needed later
        )

        # Issues-specific group
        self.config.init_custom("issues", 1)
        self.config.register_custom("issues", forum_channel=None)  # forum channel id for issues

        # PRs-specific group
        self.config.init_custom("prs", 1)
        self.config.register_custom("prs", forum_channel=None)  # forum channel id for PRs
        # Status tags are Discord-only and must NOT be created as GitHub labels
        self._status_tag_names: set[str] = {"open", "closed", "merged", "not resolved"}

        # Define required status tags for each type
        self._required_issue_tags = {"open", "closed", "not resolved"}
        self._required_pr_tags = {"open", "closed", "merged"}

        # Track threads being updated by the bot to prevent sync loops
        self._bot_updating_threads: set[int] = set()

        # Snapshot storage
        self.config.init_custom("state", 1)

        # Content tracking for hash-based change detection
        self.config.init_custom("content_hashes", 1)
        self.config.register_custom("content_hashes",
            issues={},  # number -> {"title_hash": str, "body_hash": str, "comments": {comment_id: hash}}
            prs={}      # number -> {"title_hash": str, "body_hash": str, "comments": {comment_id: hash}}
        )

        # Discord message tracking for editing
        self.config.init_custom("discord_messages", 1)
        self.config.register_custom("discord_messages",
            issues={},  # number -> {"thread_id": int, "embed_message_id": int, "comments": {comment_id: message_id}}
            prs={}      # number -> {"thread_id": int, "embed_message_id": int, "comments": {comment_id: message_id}}
        )

        # Origin tracking for conflict resolution
        self.config.init_custom("content_origins", 1)
        self.config.register_custom("content_origins",
            issues={},  # number -> {"origin": "github"|"discord", "discord_message_id": int, "github_comment_id": str}
            prs={},     # number -> {"origin": "github"|"discord", "discord_message_id": int, "github_comment_id": str}
            comments={} # "kind_number_comment_id" -> {"origin": "github"|"discord", "discord_message_id": int, "github_comment_id": str}
        )

    # ----------------------
    # Batched config management
    # ----------------------
    def _start_batch_config_mode(self) -> None:
        """Start batching config updates to reduce file I/O during sync operations."""
        self._batch_config_mode = True
        self._pending_config_updates = {}
        self.log.debug("Started batch config mode")

    async def _end_batch_config_mode(self, guild: discord.Guild) -> None:
        """End batch mode and apply all pending config updates at once."""
        if not self._batch_config_mode:
            return

        self.log.info("Ending batch config mode, applying %d pending updates", len(self._pending_config_updates))

        try:
            # Group updates by config type for efficiency
            grouped_updates: Dict[str, Dict[str, Any]] = {}

            for config_path, value in self._pending_config_updates.items():
                path_parts = config_path.split('.')
                if path_parts[0] == 'custom':
                    config_type = f"custom.{path_parts[1]}"
                    if config_type not in grouped_updates:
                        grouped_updates[config_type] = {}

                    # Build nested structure
                    current = grouped_updates[config_type]
                    for part in path_parts[2:-1]:
                        if part not in current:
                            current[part] = {}
                        current = current[part]
                    current[path_parts[-1]] = value

            # Apply grouped updates
            for config_type, data in grouped_updates.items():
                if config_type.startswith('custom.'):
                    config_name = config_type.split('.')[1]
                    config_obj = self.config.custom(config_name, guild.id)

                    # Apply each top-level key
                    for key, value in data.items():
                        await config_obj.set_raw(key, value=value)

            self.log.info("Successfully applied %d batched config updates", len(self._pending_config_updates))

        except Exception as e:
            self.log.exception("Failed to apply batched config updates: %s", e)
        finally:
            self._batch_config_mode = False
            self._pending_config_updates = {}

    def _queue_config_update(self, config_path: str, value: Any) -> None:
        """Queue a config update for later batch processing."""
        if self._batch_config_mode:
            self._pending_config_updates[config_path] = value
            self.log.debug("Queued config update: %s", config_path)
        else:
            self.log.warning("Attempted to queue config update outside of batch mode: %s", config_path)

    # ----------------------
    # GraphQL helpers
    # ----------------------
    async def _graphql_request(self, guild: discord.Guild, query: str, variables: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Make a GraphQL request to GitHub's API."""
        token = await self.config.guild(guild).github_token()
        if not token:
            return None

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        payload = {
            "query": query,
            "variables": variables or {}
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.github.com/graphql",
                    headers=headers,
                    json=payload
                ) as response:
                    if response.status == 429:
                        # Rate limited by GitHub
                        retry_after = response.headers.get('Retry-After', '60')
                        self.log.warning("GitHub GraphQL rate limited, pausing for %s seconds", retry_after)
                        await asyncio.sleep(int(retry_after))
                        return None
                    elif response.status != 200:
                        self.log.error("GraphQL request failed with status %d: %s", response.status, await response.text())
                        return None

                    data = await response.json()
                    if "errors" in data:
                        self.log.error("GraphQL errors: %s", data["errors"])
                        return None

                    return data.get("data")
        except Exception:
            self.log.exception("Failed to make GraphQL request")
            return None

    # ----------------------
    # Blocking-to-thread helpers for PyGithub calls (kept for some operations)
    # ----------------------
    async def _gh_list(self, fn_noargs):
        return await asyncio.to_thread(lambda: list(fn_noargs()))

    async def _gh_call(self, fn_noargs):
        return await asyncio.to_thread(fn_noargs)

    # ----------------------
    # Content tracking utilities
    # ----------------------
    def _hash_content(self, content: str) -> str:
        """Generate a hash for content to detect changes."""
        if not content:
            return ""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]

    def _get_status_color(self, data: Dict[str, Any], kind: str) -> discord.Color:
        """Get Discord embed color based on issue/PR status."""
        if kind == "issues":
            state = data.get("state", "").lower()
            state_reason = data.get("state_reason")
            state_reason_lower = state_reason.lower() if state_reason else ""

            if state == "open":
                return discord.Color.green()
            elif state == "closed":
                if state_reason_lower in ["not_planned", "duplicate"]:
                    return discord.Color.greyple()  # Not resolved
                else:
                    return discord.Color.red()  # Closed
            else:
                return discord.Color.default()

        elif kind == "prs":
            if data.get("merged"):
                return discord.Color.purple()  # Merged
            elif data.get("state", "").lower() == "closed":
                return discord.Color.red()  # Closed
            elif data.get("state", "").lower() == "open":
                return discord.Color.green()  # Open
            else:
                return discord.Color.default()

        return discord.Color.default()

    # ----------------------
    # Utilities
    # ----------------------
    async def _get_github_client(self, guild: discord.Guild) -> Optional[Github]:
        token = await self.config.guild(guild).github_token()
        if not token:
            self.log.debug("No GitHub token for guild %s", getattr(guild, 'id', None))
            return None
        if GithubAuth is not None:
            self.log.debug("Creating GitHub client with modern auth (guild=%s)", getattr(guild, 'id', None))
            return Github(auth=GithubAuth.Token(token))
        # Fallback for older PyGithub
        self.log.debug("Creating GitHub client with legacy token (guild=%s)", getattr(guild, 'id', None))
        return Github(token)

    async def _get_repo(self, guild: discord.Guild):
        gh = await self._get_github_client(guild)
        if not gh:
            return None
        owner = await self.config.guild(guild).github_owner()
        repo = await self.config.guild(guild).github_repo()
        if not owner or not repo:
            self.log.debug("Owner/repo not set for guild %s", getattr(guild, 'id', None))
            return None
        try:
            self.log.debug("Fetching repo %s/%s for guild %s", owner, repo, getattr(guild, 'id', None))
            return gh.get_repo(f"{owner}/{repo}")
        except Exception:
            self.log.exception("Failed to fetch repo %s/%s (guild=%s)", owner, repo, getattr(guild, 'id', None))
            return None

    # ----------------------
    # Configuration Commands
    # ----------------------
    @commands.group(name="ghsyncset")
    @commands.admin_or_permissions(manage_guild=True)
    async def ghsyncset(self, ctx: commands.Context) -> None:
        """Configure GitHub Sync."""

    @ghsyncset.command(name="token")
    async def ghsyncset_token(self, ctx: commands.Context, token: str) -> None:
        """Set the GitHub Personal Access Token (fine-grained recommended)."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return
        # minimal validation
        if len(token) < 40:
            await ctx.send("❌ Token looks invalid.")
            return
        # validate via PyGithub
        try:
            if GithubAuth is not None:
                gh = Github(auth=GithubAuth.Token(token))
            else:
                gh = Github(token)
            self.log.debug("Validating GitHub token by fetching user login")
            _ = gh.get_user().login
        except GithubException:
            await ctx.send("❌ Token validation failed.")
            self.log.warning("GitHub token validation failed")
            return
        except Exception:
            await ctx.send("❌ Error validating token.")
            self.log.exception("Error validating GitHub token")
            return
        await self.config.guild(ctx.guild).github_token.set(token)
        await ctx.send("✅ GitHub token set.")
        try:
            await ctx.message.delete()
        except Exception:
            pass

    @ghsyncset.command(name="repo")
    async def ghsyncset_repo(self, ctx: commands.Context, owner: str, repo: str) -> None:
        """Set the GitHub repository as OWNER REPO (space separated)."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return
        await self.config.guild(ctx.guild).github_owner.set(owner)
        await self.config.guild(ctx.guild).github_repo.set(repo)
        self.log.info("Repo configured to %s/%s (guild=%s)", owner, repo, getattr(ctx.guild, 'id', None))
        await ctx.send(f"✅ Repository set to `{owner}/{repo}`.")

    @ghsyncset.command(name="issues_forum")
    async def ghsyncset_issues_forum(self, ctx: commands.Context, channel: discord.ForumChannel) -> None:
        """Set the forum channel to mirror GitHub Issues."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return
        await self.config.custom("issues", ctx.guild.id).forum_channel.set(channel.id)
        self.log.info("Issues forum set: %s (%s) guild=%s", channel.name, channel.id, ctx.guild.id)
        await ctx.send(f"✅ Issues forum set to {channel.mention}.")
        # Ensure status tags exist and reconcile tag/label sets immediately
        await self._ensure_status_tags_exist(channel, "issues")
        repo = await self._get_repo(ctx.guild)
        if repo:
            await self._reconcile_forum_and_labels(ctx.guild, channel, repo)

    @ghsyncset.command(name="prs_forum")
    async def ghsyncset_prs_forum(self, ctx: commands.Context, channel: discord.ForumChannel) -> None:
        """Set the forum channel to mirror GitHub Pull Requests."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return
        await self.config.custom("prs", ctx.guild.id).forum_channel.set(channel.id)
        self.log.info("PRs forum set: %s (%s) guild=%s", channel.name, channel.id, ctx.guild.id)
        await ctx.send(f"✅ PRs forum set to {channel.mention}.")
        # Ensure status tags exist and reconcile tag/label sets immediately
        await self._ensure_status_tags_exist(channel, "prs")
        repo = await self._get_repo(ctx.guild)
        if repo:
            await self._reconcile_forum_and_labels(ctx.guild, channel, repo)

    @ghsyncset.command(name="show")
    async def ghsyncset_show(self, ctx: commands.Context) -> None:
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return
        data = await self.config.guild(ctx.guild).all()
        issues_forum = await self.config.custom("issues", ctx.guild.id).forum_channel()
        prs_forum = await self.config.custom("prs", ctx.guild.id).forum_channel()
        issues_mention = (
            f"<#{issues_forum}>" if issues_forum and ctx.guild.get_channel(issues_forum) else "Not set"
        )
        prs_mention = (
            f"<#{prs_forum}>" if prs_forum and ctx.guild.get_channel(prs_forum) else "Not set"
        )
        embed = discord.Embed(
            title="GitHub Sync Configuration", color=await ctx.embed_color()
        )
        embed.add_field(
            name="Repository",
            value=f"{data.get('github_owner') or 'Not set'}/{data.get('github_repo') or 'Not set'}",
            inline=False,
        )
        embed.add_field(name="Token", value=("Set" if data.get("github_token") else "Not set"), inline=False)
        embed.add_field(name="Issues Forum", value=issues_mention, inline=False)
        embed.add_field(name="PRs Forum", value=prs_mention, inline=False)

        # Sync settings
        poll_status = "✅ Enabled" if data.get("poll_enabled") else "❌ Disabled"
        discord_to_github_status = "✅ Enabled" if data.get("discord_to_github_enabled", True) else "❌ Disabled"
        embed.add_field(name="GitHub → Discord Sync (Polling)", value=poll_status, inline=True)
        embed.add_field(name="Discord → GitHub Sync", value=discord_to_github_status, inline=True)

        # Task status
        task_status = "🟢 Running" if self.github_poll_task.is_running() else "🔴 Stopped"
        embed.add_field(name="Polling Task", value=task_status, inline=True)

        if data.get("poll_enabled"):
            embed.add_field(name="Guild Poll Interval", value=f"{data.get('poll_interval', 60)}s", inline=True)

        if self.github_poll_task.is_running():
            embed.add_field(name="Active Task Interval", value=f"{self.github_poll_task.seconds}s", inline=True)

        await ctx.send(embed=embed)

    @ghsyncset.command(name="poll")
    async def ghsyncset_poll(self, ctx: commands.Context, enabled: Optional[bool] = None, interval: Optional[int] = None) -> None:
        """
        Enable/disable polling and/or set interval in seconds.

        The polling task automatically adjusts to use the shortest interval among all enabled guilds.
        Minimum interval is 30 seconds.
        """
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        changes_made = False

        if enabled is not None:
            await self.config.guild(ctx.guild).poll_enabled.set(bool(enabled))
            changes_made = True

        if interval is not None:
            if interval >= 30:
                await self.config.guild(ctx.guild).poll_interval.set(int(interval))
                changes_made = True
            else:
                await ctx.send("❌ Minimum interval is 30 seconds.")
                return

        if changes_made:
            # Show current settings
            current_enabled = await self.config.guild(ctx.guild).poll_enabled()
            current_interval = await self.config.guild(ctx.guild).poll_interval()

            status = "✅ Enabled" if current_enabled else "❌ Disabled"

            embed = discord.Embed(
                title="📊 Polling Configuration Updated",
                color=discord.Color.green() if current_enabled else discord.Color.red()
            )
            embed.add_field(name="Status", value=status, inline=True)
            embed.add_field(name="Interval", value=f"{current_interval}s", inline=True)

            if current_enabled:
                embed.add_field(
                    name="ℹ️ Note",
                    value="The polling task uses the shortest interval among all enabled guilds.",
                    inline=False
                )

                # Show task status
                task_status = "🟢 Running" if self.github_poll_task.is_running() else "🔴 Stopped"
                current_task_interval = self.github_poll_task.seconds if self.github_poll_task.is_running() else "N/A"
                embed.add_field(name="Task Status", value=task_status, inline=True)
                embed.add_field(name="Current Task Interval", value=f"{current_task_interval}s" if current_task_interval != "N/A" else "N/A", inline=True)

            await ctx.send(embed=embed)
        else:
            # Just show current settings
            await ctx.tick()

    @ghsyncset.command(name="task")
    async def ghsyncset_task(self, ctx: commands.Context, action: Optional[str] = None) -> None:
        """
        Control the background polling task.

        Actions:
            start   - Start the polling task
            stop    - Stop the polling task
            restart - Restart the polling task
            status  - Show task status (default)
        """
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        if action is None or action.lower() == "status":
            # Show task status
            is_running = self.github_poll_task.is_running()
            status = "🟢 Running" if is_running else "🔴 Stopped"

            embed = discord.Embed(
                title="📊 Polling Task Status",
                color=discord.Color.green() if is_running else discord.Color.red()
            )
            embed.add_field(name="Status", value=status, inline=True)

            if is_running:
                embed.add_field(name="Current Interval", value=f"{self.github_poll_task.seconds}s", inline=True)

                next_iteration = self.github_poll_task.next_iteration
                if next_iteration:
                    embed.add_field(name="Next Run", value=f"<t:{int(next_iteration.timestamp())}:R>", inline=True)
                else:
                    embed.add_field(name="Next Run", value="Unknown", inline=True)

                # Count enabled guilds
                enabled_count = 0
                for guild in self.bot.guilds:
                    try:
                        if await self.config.guild(guild).poll_enabled():
                            enabled_count += 1
                    except Exception:
                        continue

                embed.add_field(name="Enabled Guilds", value=str(enabled_count), inline=True)

            await ctx.send(embed=embed)
            return

        action = action.lower()

        if action == "start":
            if self.github_poll_task.is_running():
                await ctx.send("❌ Polling task is already running.")
                return

            self.github_poll_task.start()
            await ctx.send("✅ Polling task started.")

        elif action == "stop":
            if not self.github_poll_task.is_running():
                await ctx.send("❌ Polling task is not running.")
                return

            self.github_poll_task.cancel()
            await ctx.send("✅ Polling task stopped.")

        elif action == "restart":
            if self.github_poll_task.is_running():
                self.github_poll_task.cancel()

            self.github_poll_task.restart()
            await ctx.send("✅ Polling task restarted.")

        else:
            await ctx.send("❌ Invalid action. Use `start`, `stop`, `restart`, or `status`.")

    @ghsyncset.command(name="test_batch", hidden=True)
    async def ghsyncset_test_batch(self, ctx: commands.Context) -> None:
        """Test the batch config system (debug command)."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        await ctx.send("🧪 Testing batch config system...")

        try:
            # Start batch mode
            self._start_batch_config_mode()

            # Queue some test updates
            self._queue_config_update("custom.test.item1", "value1")
            self._queue_config_update("custom.test.item2", "value2")

            # End batch mode and apply
            await self._end_batch_config_mode(ctx.guild)

            await ctx.send("✅ Batch config test completed successfully!")

        except Exception as e:
            await ctx.send(f"❌ Batch config test failed: {str(e)}")
            self.log.exception("Batch config test failed")

    @ghsyncset.command(name="discord_to_github", aliases=["d2g"])
    async def ghsyncset_discord_to_github(self, ctx: commands.Context, enabled: Optional[bool] = None) -> None:
        """
        Enable/disable Discord → GitHub synchronization.

        When enabled (default): Discord posts, comments, edits sync to GitHub
        When disabled: Only GitHub → Discord sync works (one-way)
        """
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        if enabled is None:
            # Show current setting
            current = await self.config.guild(ctx.guild).discord_to_github_enabled()
            status = "✅ Enabled" if current else "❌ Disabled"
            await ctx.send(f"Discord → GitHub sync is currently: {status}")
            return

        await self.config.guild(ctx.guild).discord_to_github_enabled.set(bool(enabled))
        status = "✅ Enabled" if enabled else "❌ Disabled"
        await ctx.send(f"Discord → GitHub sync is now: {status}")

        if not enabled:
            await ctx.send(
                "⚠️ **Note**: Discord → GitHub sync is now disabled. This means:\n"
                "• New Discord threads won't create GitHub issues\n"
                "• Discord comments won't sync to GitHub\n"
                "• Discord tag/state changes won't sync to GitHub\n"
                "• GitHub → Discord sync will continue working normally"
            )

    @ghsyncset.command(name="syncall")
    async def ghsyncset_syncall(self, ctx: commands.Context) -> None:
        """
        Force-sync all issues and PRs from GitHub to Discord using the complete 5-step process:
        1. Ensure tag parity (status tags + GitHub labels)
        2. Create missing forum posts
        3. Edit existing forum posts with changed content
        4. Sync new/updated comments
        5. Update post status (archived/locked state)
        """
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return
        repo = await self._get_repo(ctx.guild)
        if not repo:
            await ctx.send("Repo not configured or token invalid.")
            return
        await ctx.send("Starting full 5-step sync... This may take a while.")
        self.log.info("Manual syncall triggered (guild=%s)", ctx.guild.id)
        try:
            # Force sync even if polling is disabled
            await self._sync_guild(ctx.guild, force=True)
            await ctx.tick()
        except Exception:
            self.log.exception("syncall failed (guild=%s)", ctx.guild.id)
            await ctx.send("Sync failed. Check logs.")

    @ghsyncset.command(name="fix_status_tags")
    async def ghsyncset_fix_status_tags(self, ctx: commands.Context) -> None:
        """Retroactively apply status tags (open/closed/merged/not resolved) to existing threads."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        issues_forum_id = await self.config.custom("issues", ctx.guild.id).forum_channel()
        prs_forum_id = await self.config.custom("prs", ctx.guild.id).forum_channel()
        issues_forum = ctx.guild.get_channel(issues_forum_id) if issues_forum_id else None
        prs_forum = ctx.guild.get_channel(prs_forum_id) if prs_forum_id else None

        if not isinstance(issues_forum, discord.ForumChannel) and not isinstance(prs_forum, discord.ForumChannel):
            await ctx.send("❌ No forum channels configured.")
            return

        await ctx.send("🔧 Applying status tags to existing threads...")

        try:
            # Get current snapshot to determine correct status for each issue/PR
            repo = await self._get_repo(ctx.guild)
            if not repo:
                await ctx.send("❌ Repository not configured.")
                return

            snapshot = await self._build_github_snapshot(ctx.guild)
            fixed_count = 0

            # Fix issues forum
            if isinstance(issues_forum, discord.ForumChannel):
                await self._ensure_status_tags_exist(issues_forum, "issues")
                async for thread in issues_forum.archived_threads(limit=None):
                    await self._fix_thread_status_tags(thread, snapshot, "issues")
                    fixed_count += 1
                for thread in issues_forum.threads:
                    await self._fix_thread_status_tags(thread, snapshot, "issues")
                    fixed_count += 1

            # Fix PRs forum
            if isinstance(prs_forum, discord.ForumChannel):
                await self._ensure_status_tags_exist(prs_forum, "prs")
                async for thread in prs_forum.archived_threads(limit=None):
                    await self._fix_thread_status_tags(thread, snapshot, "prs")
                    fixed_count += 1
                for thread in prs_forum.threads:
                    await self._fix_thread_status_tags(thread, snapshot, "prs")
                    fixed_count += 1

            await ctx.send(f"✅ Applied status tags to {fixed_count} threads.")

        except Exception:
            self.log.exception("fix_status_tags failed")
            await ctx.send("❌ Failed to apply status tags. Check logs.")

    @ghsyncset.command(name="retry_failed")
    async def ghsyncset_retry_failed(self, ctx: commands.Context) -> None:
        """Retry creating threads for issues/PRs that don't have Discord threads yet."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        repo = await self._get_repo(ctx.guild)
        if not repo:
            await ctx.send("❌ Repository not configured.")
            return

        await ctx.send("🔄 Checking for missing threads and retrying creation...")

        try:
            # Get current snapshot
            snapshot = await self._build_github_snapshot(ctx.guild)
            missing_count = 0

            # Check issues
            issues_forum_id = await self.config.custom("issues", ctx.guild.id).forum_channel()
            if issues_forum_id:
                for num_str in snapshot.get("issues", {}):
                    thread_id = await self._get_thread_id_by_number(ctx.guild, number=int(num_str), kind="issues")
                    if not thread_id:
                        missing_count += 1

            # Check PRs
            prs_forum_id = await self.config.custom("prs", ctx.guild.id).forum_channel()
            if prs_forum_id:
                for num_str in snapshot.get("prs", {}):
                    thread_id = await self._get_thread_id_by_number(ctx.guild, number=int(num_str), kind="prs")
                    if not thread_id:
                        missing_count += 1

            if missing_count == 0:
                await ctx.send("✅ No missing threads found.")
                return

            await ctx.send(f"Found {missing_count} missing threads. Attempting to create them...")

            # Force a full 5-step sync which will attempt to create missing threads
            await self._sync_guild(ctx.guild, force=True)
            await ctx.send("✅ Retry completed. Check logs for any remaining failures.")

        except Exception:
            self.log.exception("retry_failed command failed")
            await ctx.send("❌ Failed to retry thread creation. Check logs.")

    @ghsyncset.command(name="show_tags")
    async def ghsyncset_show_tags(self, ctx: commands.Context) -> None:
        """Show current forum tags and status tag requirements."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        issues_forum_id = await self.config.custom("issues", ctx.guild.id).forum_channel()
        prs_forum_id = await self.config.custom("prs", ctx.guild.id).forum_channel()
        issues_forum = ctx.guild.get_channel(issues_forum_id) if issues_forum_id else None
        prs_forum = ctx.guild.get_channel(prs_forum_id) if prs_forum_id else None

        embed = discord.Embed(title="Forum Tags Status", color=await ctx.embed_color())

        for forum, kind in [(issues_forum, "Issues"), (prs_forum, "PRs")]:
            if not isinstance(forum, discord.ForumChannel):
                embed.add_field(name=f"{kind} Forum", value="Not configured", inline=False)
                continue

            current_tags = [t.name for t in forum.available_tags]
            required_tags = self._required_issue_tags if kind == "Issues" else self._required_pr_tags
            existing_status_tags = [t for t in current_tags if t.lower() in {r.lower() for r in required_tags}]
            missing_status_tags = [t for t in required_tags if t.lower() not in {c.lower() for c in current_tags}]
            non_status_tags = [t for t in current_tags if t.lower() not in self._status_tag_names]

            value = f"**Current tags:** {len(current_tags)}/20\n"
            if existing_status_tags:
                value += f"**Status tags present:** {', '.join(existing_status_tags)}\n"
            if missing_status_tags:
                value += f"**Missing status tags:** {', '.join(missing_status_tags)}\n"

            if len(current_tags) + len(missing_status_tags) > 20:
                value += f"⚠️ **Tag limit issue:** Need to remove {len(current_tags) + len(missing_status_tags) - 20} existing tags\n"
                if non_status_tags:
                    removable = non_status_tags[:3]  # Show first 3
                    value += f"**Removable tags:** {', '.join(removable)}"
                    if len(non_status_tags) > 3:
                        value += f" (+{len(non_status_tags) - 3} more)"
            elif missing_status_tags:
                value += "✅ **Can create missing tags**"
            else:
                value += "✅ **All status tags present**"

            embed.add_field(name=f"{kind} Forum ({forum.name})", value=value, inline=False)

        embed.set_footer(text="Required status tags: Issues (open, closed, not resolved) | PRs (open, closed, merged)")
        await ctx.send(embed=embed)

    @ghsyncset.command(name="clear")
    async def ghsyncset_clear(self, ctx: commands.Context, target: str = "snapshot") -> None:
        """
        Clear stored data from the cog.

        Args:
            target: What to clear - "snapshot", "comments", or "full"

        Examples:
            [p]ghsyncset clear snapshot   - Clear snapshot only (recommended)
            [p]ghsyncset clear comments   - Clear comment hashes only (fix stuck comments)
            [p]ghsyncset clear full       - Clear everything (complete reset)

        - snapshot: Reset sync state, forces re-sync on next polling cycle
        - comments: Clear comment tracking, forces all comments to be re-posted
        - full: Clear everything (snapshot + content hashes + message tracking)

        Warning: This will cause content to be re-processed as "new" during the next sync.
        """
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        # Validate target
        valid_targets = ["snapshot", "comments", "full"]
        if target not in valid_targets:
            await ctx.send(f"❌ Invalid target. Use one of: {', '.join(valid_targets)}")
            return

        # Get current data to show what's being cleared
        current_snapshot = await self.config.custom("state", ctx.guild.id).get_raw("snapshot", default=None)
        content_hashes = await self.config.custom("content_hashes", ctx.guild.id).all()

        if target == "snapshot" and not current_snapshot:
            await ctx.send("❌ No snapshot found to clear.")
            return

        if target == "comments" and not content_hashes:
            await ctx.send("❌ No comment hashes found to clear.")
            return

        # Count items
        issues_count = len(current_snapshot.get("issues", {})) if current_snapshot else 0
        prs_count = len(current_snapshot.get("prs", {})) if current_snapshot else 0
        labels_count = len(current_snapshot.get("labels", [])) if current_snapshot else 0

        # Count comments across all content hashes
        total_comment_hashes = 0
        for kind_data in content_hashes.values():
            if isinstance(kind_data, dict):
                for entity_data in kind_data.values():
                    if isinstance(entity_data, dict) and "comments" in entity_data:
                        total_comment_hashes += len(entity_data["comments"])

        # Build description based on target
        if target == "snapshot":
            clear_type = "Snapshot Only"
            description = (
                "This will clear the stored GitHub snapshot containing:\n"
                f"• **{issues_count}** issues\n"
                f"• **{prs_count}** pull requests\n"
                f"• **{labels_count}** labels\n"
                "\n**Effects:**\n"
                "• Next sync will treat all GitHub content as 'new'\n"
                "• May result in duplicate notifications or processing\n"
                "• Sync state will be rebuilt from scratch\n\n"
                "**This action cannot be undone.**"
            )
        elif target == "comments":
            clear_type = "Comment Hashes Only"
            description = (
                "This will clear stored comment tracking data:\n"
                f"• **{total_comment_hashes}** comment hashes across all issues/PRs\n"
                "\n**Effects:**\n"
                "• All GitHub comments will be re-posted to Discord\n"
                "• Useful for fixing stuck comments that weren't posted\n"
                "• May result in duplicate comments if some were already posted\n\n"
                "**This action cannot be undone.**"
            )
        else:  # full
            clear_type = "Full Reset"
            description = (
                "This will clear ALL stored data:\n"
                f"• **{issues_count}** issues\n"
                f"• **{prs_count}** pull requests\n"
                f"• **{labels_count}** labels\n"
                f"• **{total_comment_hashes}** comment hashes\n"
                "• **Content hashes** (change detection data)\n"
                "• **Message tracking** (Discord↔GitHub message links)\n"
                "• **Origin tracking** (content source information)\n"
                "\n**Effects:**\n"
                "• Next sync will treat all GitHub content as 'new'\n"
                "• Discord message links will be lost\n"
                "• Content change detection will be reset\n"
                "• May result in duplicate notifications or processing\n"
                "• Sync state will be rebuilt from scratch\n\n"
                "**This action cannot be undone.**"
            )

        embed = discord.Embed(
            title=f"⚠️ Clear GitHub {clear_type}?",
            description=description,
            color=discord.Color.orange()
        )
        embed.set_footer(text="React with ✅ to confirm or ❌ to cancel")

        msg = await ctx.send(embed=embed)
        await msg.add_reaction("✅")
        await msg.add_reaction("❌")

        def check(reaction, user):
            return (
                user == ctx.author
                and str(reaction.emoji) in ["✅", "❌"]
                and reaction.message.id == msg.id
            )

        try:
            reaction, user = await self.bot.wait_for("reaction_add", timeout=60.0, check=check)
        except asyncio.TimeoutError:
            await msg.edit(embed=discord.Embed(
                title="❌ Timed Out",
                description="Snapshot clear cancelled due to timeout.",
                color=discord.Color.red()
            ))
            try:
                await msg.clear_reactions()
            except:
                pass
            return

        if str(reaction.emoji) == "❌":
            await msg.edit(embed=discord.Embed(
                title="❌ Cancelled",
                description="Snapshot clear cancelled by user.",
                color=discord.Color.red()
            ))
            try:
                await msg.clear_reactions()
            except:
                pass
            return

        # User confirmed, clear the data
        try:
            cleared_items = []

            if target == "snapshot":
                await self.config.custom("state", ctx.guild.id).clear()
                cleared_items = ["snapshot"]
                self.log.info("Cleared GitHub snapshot for guild %s (contained %d issues, %d PRs, %d labels)",
                             ctx.guild.id, issues_count, prs_count, labels_count)

            elif target == "comments":
                # Clear only comment hashes from content_hashes
                all_content_hashes = await self.config.custom("content_hashes", ctx.guild.id).all()
                for kind, kind_data in all_content_hashes.items():
                    if isinstance(kind_data, dict):
                        for entity_num, entity_data in kind_data.items():
                            if isinstance(entity_data, dict) and "comments" in entity_data:
                                entity_data["comments"] = {}
                                await self.config.custom("content_hashes", ctx.guild.id).set_raw(kind, entity_num, value=entity_data)

                cleared_items = ["comment hashes"]
                self.log.info("Cleared %d comment hashes for guild %s", total_comment_hashes, ctx.guild.id)

            else:  # full
                await self.config.custom("state", ctx.guild.id).clear()
                await self.config.custom("content_hashes", ctx.guild.id).clear()
                await self.config.custom("discord_messages", ctx.guild.id).clear()
                await self.config.custom("content_origins", ctx.guild.id).clear()
                cleared_items = ["snapshot", "content hashes", "message tracking", "origin tracking"]
                self.log.info("Cleared all GitHub data for guild %s (contained %d issues, %d PRs, %d labels, %d comment hashes)",
                             ctx.guild.id, issues_count, prs_count, labels_count, total_comment_hashes)

            success_title = f"✅ {clear_type} Complete"

            if target == "snapshot":
                success_description = (
                    f"Successfully cleared GitHub snapshot containing:\n"
                    f"• **{issues_count}** issues\n"
                    f"• **{prs_count}** pull requests\n"
                    f"• **{labels_count}** labels\n"
                    "\n**Next Steps:**\n"
                    "• The next polling cycle will rebuild the snapshot\n"
                    "• Use `[p]ghsyncset syncall` to manually trigger a full sync\n"
                    "• All GitHub content will be re-processed as new"
                )
            elif target == "comments":
                success_description = (
                    f"Successfully cleared comment tracking data:\n"
                    f"• **{total_comment_hashes}** comment hashes\n"
                    "\n**Next Steps:**\n"
                    "• The next polling cycle will re-post all GitHub comments\n"
                    "• Use `[p]ghsyncset syncall` to manually trigger comment sync\n"
                    "• May result in duplicate comments if some were already posted"
                )
            else:  # full
                success_description = (
                    f"Successfully cleared ALL GitHub data:\n"
                    f"• **{issues_count}** issues\n"
                    f"• **{prs_count}** pull requests\n"
                    f"• **{labels_count}** labels\n"
                    f"• **{total_comment_hashes}** comment hashes\n"
                    "• Content hashes, message tracking, origin tracking\n"
                    "\n**Next Steps:**\n"
                    "• The next polling cycle will rebuild everything\n"
                    "• Use `[p]ghsyncset syncall` to manually trigger a full sync\n"
                    "• All GitHub content will be re-processed as new\n"
                    "• Discord ↔ GitHub message links will need to be re-established"
                )

            success_embed = discord.Embed(
                title=success_title,
                description=success_description,
                color=discord.Color.green()
            )

            await msg.edit(embed=success_embed)
            try:
                await msg.clear_reactions()
            except:
                pass

        except Exception as e:
            self.log.exception("Failed to clear snapshot for guild %s", ctx.guild.id)

            error_embed = discord.Embed(
                title="❌ Clear Failed",
                description=f"Failed to clear snapshot: {str(e)}",
                color=discord.Color.red()
            )

            await msg.edit(embed=error_embed)
            try:
                await msg.clear_reactions()
            except:
                pass

    @ghsyncset.command(name="fix_thread_states")
    async def ghsyncset_fix_thread_states(self, ctx: commands.Context) -> None:
        """Retroactively apply correct archived/locked states to all existing threads based on GitHub status."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        issues_forum_id = await self.config.custom("issues", ctx.guild.id).forum_channel()
        prs_forum_id = await self.config.custom("prs", ctx.guild.id).forum_channel()
        issues_forum = ctx.guild.get_channel(issues_forum_id) if issues_forum_id else None
        prs_forum = ctx.guild.get_channel(prs_forum_id) if prs_forum_id else None

        if not isinstance(issues_forum, discord.ForumChannel) and not isinstance(prs_forum, discord.ForumChannel):
            await ctx.send("❌ No forum channels configured.")
            return

        await ctx.send("🔧 Applying correct archived/locked states to all threads based on GitHub status...")

        try:
            # Get current snapshot to determine correct state for each issue/PR
            repo = await self._get_repo(ctx.guild)
            if not repo:
                await ctx.send("❌ Repository not configured.")
                return

            snapshot = await self._build_github_snapshot(ctx.guild)
            fixed_count = 0

            # Fix issues forum threads
            if isinstance(issues_forum, discord.ForumChannel):
                self.log.info("Fixing thread states for issues forum")
                # Check archived threads
                async for thread in issues_forum.archived_threads(limit=None):
                    await self._fix_thread_complete_state(thread, snapshot, "issues")
                    fixed_count += 1
                    await asyncio.sleep(0.1)  # Rate limit protection

                # Check active threads
                for thread in issues_forum.threads:
                    await self._fix_thread_complete_state(thread, snapshot, "issues")
                    fixed_count += 1
                    await asyncio.sleep(0.1)  # Rate limit protection

            # Fix PRs forum threads
            if isinstance(prs_forum, discord.ForumChannel):
                self.log.info("Fixing thread states for PRs forum")
                # Check archived threads
                async for thread in prs_forum.archived_threads(limit=None):
                    await self._fix_thread_complete_state(thread, snapshot, "prs")
                    fixed_count += 1
                    await asyncio.sleep(0.1)  # Rate limit protection

                # Check active threads
                for thread in prs_forum.threads:
                    await self._fix_thread_complete_state(thread, snapshot, "prs")
                    fixed_count += 1
                    await asyncio.sleep(0.1)  # Rate limit protection

            await ctx.send(f"✅ Applied correct states to {fixed_count} threads.")

        except Exception:
            self.log.exception("fix_thread_states failed")
            await ctx.send("❌ Failed to apply thread states. Check logs.")

    # ----------------------
    # Discord -> GitHub: listeners
    # ----------------------
    @commands.Cog.listener()
    async def on_thread_create(self, thread: discord.Thread) -> None:
        guild = thread.guild
        if not guild:
            return
        # Determine if thread belongs to issues or PRs forum
        issues_forum = await self.config.custom("issues", guild.id).forum_channel()
        prs_forum = await self.config.custom("prs", guild.id).forum_channel()
        if thread.parent_id not in {issues_forum, prs_forum}:
            return
        # Do NOT create issues/PRs on GitHub from new Discord threads.
        self.log.debug("on_thread_create: thread=%s parent=%s guild=%s", thread.id, thread.parent_id, guild.id)
        # Try to auto-link if the first message contains a GitHub URL.
        try:
            async for msg in thread.history(limit=1, oldest_first=True):
                self.log.debug("Reading first message for thread %s", thread.id)
                url = self._extract_github_url_from_text(msg.content)
                if not url:
                    continue
                number = self._extract_issue_number(url)
                if not number:
                    continue
                kind = "issues" if thread.parent_id == issues_forum else "prs"
                self.log.info("Auto-link thread %s -> %s #%s", thread.id, kind, number)
                await self._store_link(thread, url, kind=kind)
                await self._store_link_by_number(thread, number, kind=kind)
                await thread.send(f"Linked this thread to {url}")
                break
        except Exception:
            self.log.exception("Auto-link on thread create failed")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        """Handle new Discord messages and sync to GitHub if appropriate."""
        if message.author.bot or not message.guild:
            return
        if not isinstance(message.channel, discord.Thread):
            return

        guild = message.guild
        issues_forum = await self.config.custom("issues", guild.id).forum_channel()
        prs_forum = await self.config.custom("prs", guild.id).forum_channel()

        if message.channel.parent_id not in {issues_forum, prs_forum}:
            return

        # Determine if this is the initial thread creation message
        is_initial_message = False
        try:
            first_message = await message.channel.history(limit=1, oldest_first=True).__anext__()
            is_initial_message = first_message.id == message.id
        except StopAsyncIteration:
            # No messages in thread yet (shouldn't happen, but handle gracefully)
            is_initial_message = True

        kind = "issues" if message.channel.parent_id == issues_forum else "prs"

        # Handle initial thread creation (potential new GitHub issue/PR)
        if is_initial_message:
            await self._handle_new_thread_creation(message, kind)
        else:
            # Handle comment on existing thread
            await self._handle_thread_comment(message, kind)

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message) -> None:
        """Handle Discord message edits and sync to GitHub."""
        if after.author.bot or not after.guild:
            return
        if not isinstance(after.channel, discord.Thread):
            return

        guild = after.guild
        issues_forum = await self.config.custom("issues", guild.id).forum_channel()
        prs_forum = await self.config.custom("prs", guild.id).forum_channel()

        if after.channel.parent_id not in {issues_forum, prs_forum}:
            return

        kind = "issues" if after.channel.parent_id == issues_forum else "prs"
        await self._handle_message_edit(before, after, kind)

    @commands.Cog.listener()
    async def on_message_delete(self, message: discord.Message) -> None:
        """Handle Discord message deletion and sync to GitHub."""
        if message.author.bot or not message.guild:
            return
        if not isinstance(message.channel, discord.Thread):
            return

        guild = message.guild
        issues_forum = await self.config.custom("issues", guild.id).forum_channel()
        prs_forum = await self.config.custom("prs", guild.id).forum_channel()

        if message.channel.parent_id not in {issues_forum, prs_forum}:
            return

        kind = "issues" if message.channel.parent_id == issues_forum else "prs"
        await self._handle_message_delete(message, kind)

    @commands.Cog.listener()
    async def on_thread_update(self, before: discord.Thread, after: discord.Thread) -> None:
        guild = after.guild
        if not guild:
            return

        # Skip if this thread is being updated by the bot to prevent feedback loops
        if after.id in self._bot_updating_threads:
            self.log.debug("Skipping thread_update for thread %s (bot-initiated change)", after.id)
            return

        # Handle thread updates based on origin and content type
        await self._handle_thread_update(before, after)

    async def _handle_thread_update(self, before: discord.Thread, after: discord.Thread) -> None:
        """Handle thread updates with proper conflict resolution based on content origin."""
        guild = after.guild

        # Check if Discord → GitHub sync is enabled
        if not await self._is_discord_to_github_enabled(guild):
            self.log.debug("Discord → GitHub sync disabled, skipping thread update for guild %s", guild.id)
            return

        issues_forum = await self.config.custom("issues", guild.id).forum_channel()
        prs_forum = await self.config.custom("prs", guild.id).forum_channel()

        if after.parent_id not in {issues_forum, prs_forum}:
            return

        repo = await self._get_repo(guild)
        if not repo:
            return

        gh_url = await self._get_link(after)
        if not gh_url:
            return

        issue_number = self._extract_issue_number(gh_url)
        if not issue_number:
            return

        kind = "issues" if after.parent_id == issues_forum else "prs"

        # Get origin data for this content
        origin_data = await self._get_origin(guild, issue_number, kind)

        # Handle tag changes
        if getattr(before, "applied_tags", None) != getattr(after, "applied_tags", None):
            await self._handle_tag_changes(after, repo, issue_number, kind, origin_data)

        # Handle archive/lock state changes
        if before.archived != after.archived or before.locked != after.locked:
            await self._handle_state_changes(before, after, repo, issue_number, kind, origin_data)

    async def _handle_tag_changes(self, thread: discord.Thread, repo, issue_number: int, kind: str, origin_data: Optional[Dict[str, Any]]) -> None:
        """Handle Discord tag changes with conflict resolution."""
        try:
            available_tags: List[discord.ForumTag] = getattr(thread.parent, "available_tags", [])  # type: ignore
            applied_tags: List[discord.ForumTag] = getattr(thread, "applied_tags", []) or []

            # Separate status tags from regular labels
            status_tags = [t.name.lower() for t in applied_tags if t.name.lower() in self._status_tag_names]
            regular_labels = [t.name for t in applied_tags if t.name.lower() not in self._status_tag_names and t.id in {tag.id for tag in available_tags}]

            # Always sync status tags to GitHub regardless of origin (requirement)
            if status_tags:
                github_issue = repo.get_issue(number=issue_number)

                # Handle status changes
                if "open" in status_tags:
                    github_issue.edit(state="open")
                    self.log.info("Opened GitHub %s #%s from Discord status tag", kind, issue_number)
                elif "closed" in status_tags or "not resolved" in status_tags:
                    github_issue.edit(state="closed")
                    self.log.info("Closed GitHub %s #%s from Discord status tag", kind, issue_number)
                elif "merged" in status_tags and kind == "prs":
                    # Cannot merge via API, but log the attempt
                    self.log.warning("Cannot merge PR #%s via API - status tag applied in Discord", issue_number)

            # Handle regular labels based on origin
            if origin_data and origin_data.get("origin") == "discord":
                # Discord-originated content: sync labels to GitHub
                github_issue = repo.get_issue(number=issue_number)
                github_issue.set_labels(*regular_labels)
                self.log.info("Updated GitHub %s #%s labels to match Discord tags (Discord origin)", kind, issue_number)
            elif not origin_data or origin_data.get("origin") == "github":
                # GitHub-originated content: let GitHub → Discord sync handle it during next poll
                self.log.debug("Skipping label sync for GitHub-originated %s #%s", kind, issue_number)

        except Exception:
            self.log.exception("Failed to handle tag changes for %s #%s", kind, issue_number)

    async def _handle_state_changes(self, before: discord.Thread, after: discord.Thread, repo, issue_number: int, kind: str, origin_data: Optional[Dict[str, Any]]) -> None:
        """Handle Discord archive/lock state changes with conflict resolution."""
        try:
            # Check if we should sync this change to GitHub
            should_sync = False

            if origin_data and origin_data.get("origin") == "discord":
                # Discord-originated content: always sync to GitHub
                should_sync = True
            elif not origin_data or origin_data.get("origin") == "github":
                # GitHub-originated content: only sync if user explicitly changed state
                # (This allows manual overrides of GitHub-originated content)
                should_sync = True

            if should_sync:
                self.log.info("User changed thread %s state (archived: %s→%s, locked: %s→%s), syncing to GitHub %s #%s",
                            after.id, before.archived, after.archived, before.locked, after.locked, kind, issue_number)

                github_issue = repo.get_issue(number=issue_number)

                if before.archived != after.archived:
                    new_state = "closed" if after.archived else "open"
                    github_issue.edit(state=new_state)
                    self.log.info("Updated GitHub %s #%s state to %s", kind, issue_number, new_state)

                if before.locked != after.locked:
                    if after.locked:
                        github_issue.lock("off-topic")  # Use a generic lock reason
                        self.log.info("Locked GitHub %s #%s", kind, issue_number)
                    else:
                        github_issue.unlock()
                        self.log.info("Unlocked GitHub %s #%s", kind, issue_number)

        except Exception:
            self.log.exception("Failed to handle state changes for %s #%s", kind, issue_number)

    # ----------------------
    # GitHub -> Discord: inbound webhooks (stub entrypoint)
    # ----------------------
    # Provide a simple command to print webhook URL to configure via reverse proxy or external service
    @commands.command(name="ghsync_webhook_info")
    @commands.admin_or_permissions(manage_guild=True)
    async def ghsync_webhook_info(self, ctx: commands.Context) -> None:
        """Instructions on setting up GitHub webhooks to this cog (use a reverse proxy)."""
        await ctx.send(
            "Set a webhook in your GitHub repo for Issue and Pull Request events pointing to your bot's public endpoint. "
            "This cog provides stubs for processing the payload; connect it via your web layer."
        )

    # ----------------------
    # Persistence of Discord<->GitHub linkage
    # ----------------------
    async def _store_link(self, thread: discord.Thread, gh_url: str, *, kind: str) -> None:
        if self._batch_config_mode:
            # Queue for batch update
            config_path = f"custom.{kind}.links.{thread.id}"
            self._queue_config_update(config_path, gh_url)
        else:
            # Direct update
            if kind == "issues":
                await self.config.custom("issues", thread.guild.id).set_raw(
                    "links", str(thread.id), value=gh_url
                )
            elif kind == "prs":
                await self.config.custom("prs", thread.guild.id).set_raw(
                    "links", str(thread.id), value=gh_url
                )

    async def _get_link(self, thread: discord.Thread) -> Optional[str]:
        # Look in issues mapping first, then PRs mapping
        gh_url = await self.config.custom("issues", thread.guild.id).get_raw(
            "links", str(thread.id), default=None
        )
        if gh_url:
            return gh_url
        return await self.config.custom("prs", thread.guild.id).get_raw(
            "links", str(thread.id), default=None
        )

    @staticmethod
    def _extract_issue_number(url: str) -> Optional[int]:
        m = re.search(r"/issues/(\d+)$", url)
        if m:
            return int(m.group(1))
        m = re.search(r"/pull/(\d+)$", url)
        if m:
            return int(m.group(1))
        return None

    @staticmethod
    def _extract_github_url_from_text(text: str) -> Optional[str]:
        if not text:
            return None
        m = re.search(r"https?://github\.com/[^\s)]+/(issues|pull)/(\d+)", text)
        if m:
            return m.group(0)
        return None

    # ----------------------
    # Background polling task using Discord tasks
    # ----------------------
    @tasks.loop(seconds=60)  # Default interval, will be adjusted dynamically
    async def github_poll_task(self) -> None:
        """Background task to poll GitHub and sync to Discord."""
        try:
            # Check if any guilds have polling enabled
            enabled_guilds = []
            intervals: List[int] = []

            for guild in self.bot.guilds:
                try:
                    if await self.config.guild(guild).poll_enabled():
                        enabled_guilds.append(guild)
                        intervals.append(await self.config.guild(guild).poll_interval())
                except Exception:
                    continue

            if not enabled_guilds:
                # No guilds have polling enabled, use default interval
                return

            self.log.debug("Polling tick: syncing %d enabled guilds", len(enabled_guilds))

            # Sync all enabled guilds
            await asyncio.gather(*(self._sync_guild(g) for g in enabled_guilds))

            # Adjust the loop interval based on the smallest configured interval
            new_interval = min(intervals) if intervals else 60
            new_interval = max(30, new_interval)  # Minimum 30 seconds

            # Only change interval if it's different (avoid unnecessary changes)
            if self.github_poll_task.seconds != new_interval:
                self.log.debug("Adjusting poll interval from %ds to %ds", self.github_poll_task.seconds, new_interval)
                self.github_poll_task.change_interval(seconds=new_interval)

        except Exception:
            self.log.exception("Error in GitHub polling task")

    @github_poll_task.before_loop
    async def before_github_poll_task(self) -> None:
        """Wait for the bot to be ready before starting the polling task."""
        await self.bot.wait_until_red_ready()
        self.log.debug("GitHub polling task started")

    async def cog_load(self) -> None:
        """Start the background polling task when the cog loads."""
        if not self.github_poll_task.is_running():
            self.github_poll_task.start()

    async def cog_unload(self) -> None:
        """Stop the background polling task when the cog unloads."""
        if self.github_poll_task.is_running():
            self.github_poll_task.cancel()
            self.log.debug("GitHub polling task stopped")

    async def _sync_guild(self, guild: discord.Guild, *, force: bool = False) -> None:
        """
        Perform a complete 5-step sync for a guild.

        Args:
            guild: The Discord guild to sync
            force: If True, sync even if polling is disabled (for manual sync)
        """
        # Check if sync should proceed
        if not force and not await self.config.guild(guild).poll_enabled():
            return

        repo = await self._get_repo(guild)
        if not repo:
            self.log.warning("Cannot sync guild %s: repository not configured", guild.id)
            return

        self.log.info("Starting complete 5-step sync for guild %s (force=%s)", guild.id, force)

        try:
            # Start batch config mode to reduce file I/O during sync
            self._start_batch_config_mode()

            # Build fresh snapshot from GitHub using GraphQL
            snapshot = await self._build_github_snapshot(guild)

            # Load previous snapshot for comparison
            prev = await self.config.custom("state", guild.id).get_raw("snapshot", default=None)

            # Perform 5-step reconciliation to Discord
            await self._reconcile_snapshot_to_discord(guild, repo, prev, snapshot)

            # Save new snapshot as the current state (queued for batch save)
            self._queue_config_update(f"custom.state.snapshot", snapshot)

            # Apply all batched config updates at once
            await self._end_batch_config_mode(guild)

            self.log.info("Completed 5-step sync for guild %s", guild.id)

        except Exception:
            self.log.exception("Failed to sync guild %s", guild.id)
            # Ensure batch mode is ended even on error
            if self._batch_config_mode:
                await self._end_batch_config_mode(guild)

    async def _sync_prs(self, guild: discord.Guild, repo, since_dt) -> None:
        # Deprecated pathway; replaced by snapshot reconciliation
        return

    async def _get_thread_by_number(self, guild: discord.Guild, *, number: int, kind: str) -> Optional[discord.Thread]:
        try:
            thread_id = await self._get_thread_id_by_number(guild, number=number, kind=kind)
            if not thread_id:
                return None
            ch = guild.get_channel(int(thread_id))
            if isinstance(ch, discord.Thread):
                return ch
            # Fetch if not cached
            return await guild.fetch_channel(int(thread_id))  # type: ignore
        except Exception:
            return None

    async def _get_thread_id_by_number(self, guild: discord.Guild, *, number: int, kind: str) -> Optional[int]:
        try:
            # First check batch queue if in batch mode
            if self._batch_config_mode:
                batch_key = f"custom.{kind}.links_by_number.{number}"
                if batch_key in self._pending_config_updates:
                    val = self._pending_config_updates[batch_key]
                    return int(val) if val else None

            # Then check stored config
            if kind == "issues":
                val = await self.config.custom("issues", guild.id).get_raw("links_by_number", str(number), default=None)
            else:
                val = await self.config.custom("prs", guild.id).get_raw("links_by_number", str(number), default=None)
            return int(val) if val else None
        except Exception:
            return None

    async def _store_link_by_number(self, thread: discord.Thread, number: int, *, kind: str) -> None:
        if self._batch_config_mode:
            # Queue for batch update
            config_path = f"custom.{kind}.links_by_number.{number}"
            self._queue_config_update(config_path, str(thread.id))
        else:
            # Direct update
            if kind == "issues":
                await self.config.custom("issues", thread.guild.id).set_raw("links_by_number", str(number), value=str(thread.id))
            elif kind == "prs":
                await self.config.custom("prs", thread.guild.id).set_raw("links_by_number", str(number), value=str(thread.id))

    def _labels_to_forum_tags(self, forum: discord.ForumChannel, labels: List[str]) -> List[discord.ForumTag]:
        name_to_tag: Dict[str, discord.ForumTag] = {t.name: t for t in forum.available_tags}
        return [name_to_tag[name] for name in labels if name in name_to_tag]

    def _clean_discord_text(self, text: str) -> str:
        """Clean text for Discord by removing/replacing problematic characters."""
        if not text:
            return ""

        # Remove null bytes and other problematic control characters
        cleaned = text.replace('\x00', '')  # Null bytes
        cleaned = cleaned.replace('\r\n', '\n').replace('\r', '\n')  # Normalize line endings

        # Remove other control characters that might cause issues (except common ones like \n, \t)
        import re
        cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', cleaned)

        # Remove excessive whitespace but preserve single newlines
        lines = cleaned.split('\n')
        cleaned_lines = []
        for line in lines:
            stripped = line.rstrip()
            cleaned_lines.append(stripped)

        # Remove excessive consecutive empty lines (max 2 consecutive)
        result_lines = []
        empty_count = 0
        for line in cleaned_lines:
            if not line.strip():
                empty_count += 1
                if empty_count <= 2:
                    result_lines.append(line)
            else:
                empty_count = 0
                result_lines.append(line)

        cleaned = '\n'.join(result_lines).strip()

        # If text is empty after cleaning, return a safe fallback
        if not cleaned:
            return "(empty)"

        return cleaned

    async def _aiter(self, seq):
        # Helper to iterate possibly blocking iterables in an async context
        for item in seq:
            yield item

    # ----------------------
    # Tag reconciliation and GitHub label helpers
    # ----------------------
    async def _ensure_status_labels(self, repo) -> None:
        # No-op: status tags are Discord-only; do not create as GitHub labels
        return

    async def _build_github_snapshot(self, guild: discord.Guild) -> Dict[str, Any]:
        """Fetch issues, PRs, labels, and comments into a serializable dict using GraphQL."""
        snapshot: Dict[str, Any] = {"labels": [], "issues": {}, "prs": {}, "repo_info": {}}

        self.log.debug("Building GitHub snapshot using GraphQL...")

        # Get repo info
        owner = await self.config.guild(guild).github_owner()
        repo_name = await self.config.guild(guild).github_repo()
        if not owner or not repo_name:
            self.log.error("Owner/repo not configured for guild %s", guild.id)
            return snapshot

        try:
            # Start with the first page
            variables = {
                "owner": owner,
                "repo": repo_name,
                "issuesCursor": None,
                "prsCursor": None
            }

            all_issues: List[Dict[str, Any]] = []
            all_prs: List[Dict[str, Any]] = []
            labels: List[str] = []
            repo_info: Dict[str, Any] = {}

            # Fetch all data with pagination
            while True:
                data = await self._graphql_request(guild, REPOSITORY_QUERY, variables)
                if not data or not data.get("repository"):
                    break

                repo_data = data["repository"]

                # Get repo info (only on first request)
                if not repo_info and repo_data.get("owner"):
                    repo_info = {
                        "owner_login": repo_data["owner"].get("login", ""),
                        "owner_avatar": repo_data["owner"].get("avatarUrl", "")
                    }

                # Get labels (only on first request)
                if not labels and repo_data.get("labels"):
                    labels = [label["name"] for label in repo_data["labels"]["nodes"]]

                # Collect issues
                if repo_data.get("issues"):
                    issues = repo_data["issues"]
                    all_issues.extend(issues["nodes"])

                    if issues["pageInfo"]["hasNextPage"]:
                        variables["issuesCursor"] = issues["pageInfo"]["endCursor"]
                    else:
                        variables["issuesCursor"] = "DONE"

                # Collect PRs
                if repo_data.get("pullRequests"):
                    prs = repo_data["pullRequests"]
                    all_prs.extend(prs["nodes"])

                    if prs["pageInfo"]["hasNextPage"]:
                        variables["prsCursor"] = prs["pageInfo"]["endCursor"]
                    else:
                        variables["prsCursor"] = "DONE"

                # Check if we're done with both
                issues_done = variables["issuesCursor"] == "DONE"
                prs_done = variables["prsCursor"] == "DONE"

                if issues_done and prs_done:
                    break

                # If one is done, set to None to avoid fetching more
                if issues_done:
                    variables["issuesCursor"] = None
                if prs_done:
                    variables["prsCursor"] = None

            # Process repo info and labels
            snapshot["repo_info"] = repo_info
            snapshot["labels"] = labels
            self.log.debug("Fetched %d labels", len(labels))

            # Process issues
            issue_count = 0
            for issue in all_issues:
                issue_count += 1

                # Filter comments that contain Discord message links (avoid loops)
                comments = []
                comment_data = issue.get("comments", {})
                comment_nodes = comment_data.get("nodes", [])

                # Check if comments are truncated
                if comment_data.get("pageInfo", {}).get("hasNextPage"):
                    self.log.warning("Issue #%s has more than 100 comments - some may not be synced", issue["number"])

                for comment in comment_nodes:
                    if comment.get("body") and DISCORD_MESSAGE_LINK_RE.search(comment["body"]):
                        continue
                    comments.append({
                        "id": comment.get("id"),
                        "author": comment.get("author", {}).get("login", "GitHub") if comment.get("author") else "GitHub",
                        "author_avatar": comment.get("author", {}).get("avatarUrl", "") if comment.get("author") else "",
                        "body": comment.get("body", ""),
                        "url": comment.get("url", ""),
                        "updated_at": comment.get("updatedAt", ""),
                    })

                entry = {
                    "number": issue["number"],
                    "title": issue.get("title", ""),
                    "state": issue["state"].lower(),  # GraphQL returns OPEN/CLOSED, we want lowercase
                    "state_reason": issue.get("stateReason", "").lower() if issue.get("stateReason") else None,
                    "locked": bool(issue.get("locked", False)),
                    "labels": [label["name"] for label in issue.get("labels", {}).get("nodes", [])],
                    "url": issue.get("url", ""),
                    "body": issue.get("body", ""),
                    "comments": comments,
                    "user": issue.get("author", {}).get("login", "GitHub") if issue.get("author") else "GitHub",
                    "user_avatar": issue.get("author", {}).get("avatarUrl", "") if issue.get("author") else "",
                    "created_at": issue.get("createdAt", ""),
                    "updated_at": issue.get("updatedAt", "")
                }

                snapshot["issues"][str(issue["number"])] = entry

            self.log.debug("Processed %d issues", issue_count)

            # Process PRs
            pr_count = 0
            for pr in all_prs:
                pr_count += 1

                # Filter comments that contain Discord message links (avoid loops)
                comments = []
                comment_data = pr.get("comments", {})
                comment_nodes = comment_data.get("nodes", [])

                # Check if comments are truncated
                if comment_data.get("pageInfo", {}).get("hasNextPage"):
                    self.log.warning("PR #%s has more than 100 comments - some may not be synced", pr["number"])

                for comment in comment_nodes:
                    if comment.get("body") and DISCORD_MESSAGE_LINK_RE.search(comment["body"]):
                        continue
                    comments.append({
                        "id": comment.get("id"),
                        "author": comment.get("author", {}).get("login", "GitHub") if comment.get("author") else "GitHub",
                        "author_avatar": comment.get("author", {}).get("avatarUrl", "") if comment.get("author") else "",
                        "body": comment.get("body", ""),
                        "url": comment.get("url", ""),
                        "updated_at": comment.get("updatedAt", ""),
                    })

                entry = {
                    "number": pr["number"],
                    "title": pr.get("title", ""),
                    "state": pr["state"].lower(),  # GraphQL returns OPEN/CLOSED/MERGED, we want lowercase
                    "merged": bool(pr.get("merged", False)),
                    "locked": bool(pr.get("locked", False)),
                    "labels": [label["name"] for label in pr.get("labels", {}).get("nodes", [])],
                    "url": pr.get("url", ""),
                    "body": pr.get("body", ""),
                    "comments": comments,
                    "user": pr.get("author", {}).get("login", "GitHub") if pr.get("author") else "GitHub",
                    "user_avatar": pr.get("author", {}).get("avatarUrl", "") if pr.get("author") else "",
                    "created_at": pr.get("createdAt", ""),
                    "updated_at": pr.get("updatedAt", "")
                }

                snapshot["prs"][str(pr["number"])] = entry

            self.log.debug("Processed %d PRs", pr_count)

        except Exception:
            self.log.exception("Failed to build GitHub snapshot using GraphQL")

        self.log.info("GitHub snapshot built: %d labels, %d issues, %d PRs",
                     len(snapshot["labels"]), len(snapshot["issues"]), len(snapshot["prs"]))
        return snapshot

    async def _reconcile_snapshot_to_discord(self, guild: discord.Guild, repo, prev: Optional[Dict[str, Any]], cur: Dict[str, Any]) -> None:
        """
        5-Step structured reconciliation process:
        1. Ensure tag parity (status tags first)
        2. Create missing forum posts
        3. Edit existing forum posts
        4. Sync comments
        5. Update forum post status (archived/locked)
        """
        self.log.info("🔄 Starting 5-step reconciliation for guild %s", guild.id)

        # Get forum channels
        issues_forum_id = await self.config.custom("issues", guild.id).forum_channel()
        prs_forum_id = await self.config.custom("prs", guild.id).forum_channel()
        issues_forum = guild.get_channel(issues_forum_id) if issues_forum_id else None
        prs_forum = guild.get_channel(prs_forum_id) if prs_forum_id else None

        # Process each forum
        for forum, kind in [(issues_forum, "issues"), (prs_forum, "prs")]:
            if not isinstance(forum, discord.ForumChannel):
                if cur.get(kind):
                    self.log.warning("%s forum not configured, skipping %d %s", kind.title(), len(cur[kind]), kind)
                continue

            if not cur.get(kind):
                self.log.debug("No %s to process", kind)
                continue

            self.log.info("Processing %d %s in forum %s", len(cur[kind]), kind, forum.name)

            # Step 1: Ensure tag parity between Discord and GitHub
            await self._step1_ensure_tag_parity(guild, forum, repo, cur, kind)

            # Step 2: Create missing forum posts
            await self._step2_create_missing_posts(guild, forum, prev or {}, cur, kind)

            # Step 3: Edit existing forum posts
            await self._step3_edit_existing_posts(guild, forum, prev or {}, cur, kind)

            # Step 4: Sync comments
            await self._step4_sync_comments(guild, forum, prev or {}, cur, kind)

            # Step 5: Update forum post status
            await self._step5_update_post_status(guild, forum, cur, kind)

        self.log.info("✅ Completed 5-step reconciliation for guild %s", guild.id)

    # ----------------------
    # 5-Step Reconciliation Methods
    # ----------------------
    async def _step1_ensure_tag_parity(self, guild: discord.Guild, forum: discord.ForumChannel, repo, snapshot: Dict[str, Any], kind: str) -> None:
        """Step 1: Ensure parity between Discord and GitHub tags, with status tags first."""
        self.log.info("📋 Step 1: Ensuring tag parity for %s forum", kind)

        # First, ensure status tags exist (required)
        await self._ensure_status_tags_exist(forum, kind)

        # Then reconcile with GitHub labels (optional, space permitting)
        await self._reconcile_forum_and_labels(guild, forum, repo)

        self.log.info("✅ Step 1 completed for %s", kind)

    async def _step2_create_missing_posts(self, guild: discord.Guild, forum: discord.ForumChannel, prev: Dict[str, Any], cur: Dict[str, Any], kind: str) -> None:
        """Step 2: Create forum posts for new GitHub issues/PRs."""
        self.log.info("➕ Step 2: Creating missing forum posts for %s", kind)

        prev_entities = prev.get(kind, {})
        cur_entities = cur.get(kind, {})
        created_count = 0

        # Sort entities by creation date to ensure chronological forum post creation (oldest first)
        sorted_entities = sorted(
            cur_entities.items(),
            key=lambda x: x[1].get("created_at", "")
        )

        if sorted_entities:
            self.log.info("Creating %d %s posts in chronological order: %s #%s (oldest) to %s #%s (newest)",
                         len(sorted_entities),
                         kind,
                         kind[:-1], sorted_entities[0][0],  # Remove 's' from 'issues'/'prs'
                         kind[:-1], sorted_entities[-1][0])

        for number_str, data in sorted_entities:
            # Skip if this entity already has a thread
            if await self._get_thread_id_by_number(guild, number=int(number_str), kind=kind):
                continue

            # Check if this GitHub content originally came from Discord (prevent loops)
            origin_data = await self._get_origin(guild, int(number_str), kind)
            if origin_data and origin_data.get("origin") == "discord":
                self.log.debug("Skipping forum post creation for %s #%s - originated from Discord", kind, number_str)
                continue

            # Create new thread for this issue/PR
            success = await self._create_forum_post(guild, forum, data, kind)
            if success:
                created_count += 1
                # Log progress every 10 posts
                if created_count % 10 == 0:
                    self.log.info("Progress: Created %d %s posts (latest: %s #%s)",
                                 created_count, kind, kind[:-1], number_str)
                # Small delay to help with rate limiting and ensure proper ordering
                await asyncio.sleep(0.2)  # Increased delay to reduce rate limit hits

        self.log.info("✅ Step 2 completed: Created %d new %s posts", created_count, kind)

    async def _step3_edit_existing_posts(self, guild: discord.Guild, forum: discord.ForumChannel, prev: Dict[str, Any], cur: Dict[str, Any], kind: str) -> None:
        """Step 3: Edit existing forum posts that have changed."""
        self.log.info("✏️ Step 3: Editing existing forum posts for %s", kind)

        prev_entities = prev.get(kind, {})
        cur_entities = cur.get(kind, {})
        edited_count = 0

        for number_str, cur_data in cur_entities.items():
            # Only process entities that already have threads
            thread = await self._get_thread_by_number(guild, number=int(number_str), kind=kind)
            if not thread:
                continue

            prev_data = prev_entities.get(number_str, {})

            # Check if content has changed using hashes
            if await self._has_content_changed(guild, int(number_str), kind, cur_data, prev_data):
                success = await self._edit_forum_post(guild, thread, cur_data, kind)
                if success:
                    edited_count += 1

        self.log.info("✅ Step 3 completed: Edited %d existing %s posts", edited_count, kind)

    async def _step4_sync_comments(self, guild: discord.Guild, forum: discord.ForumChannel, prev: Dict[str, Any], cur: Dict[str, Any], kind: str) -> None:
        """Step 4: Sync comments from GitHub to Discord."""
        self.log.info("💬 Step 4: Syncing comments for %s", kind)

        prev_entities = prev.get(kind, {})
        cur_entities = cur.get(kind, {})
        comment_count = 0

        for number_str, cur_data in cur_entities.items():
            thread = await self._get_thread_by_number(guild, number=int(number_str), kind=kind)
            if not thread:
                continue

            prev_data = prev_entities.get(number_str, {})
            new_comments = await self._get_new_comments(guild, int(number_str), kind, cur_data, prev_data)

            self.log.debug("Processing %s #%s: found %d comments in GitHub, %d new comments to sync",
                         kind, number_str, len(cur_data.get("comments", [])), len(new_comments))

            successfully_posted_comments = []
            for comment in new_comments:
                success = await self._post_comment_to_thread(guild, thread, comment, cur_data, kind)
                if success:
                    comment_count += 1
                    successfully_posted_comments.append(comment)

            # Only update hashes for comments that were actually posted to Discord
            if successfully_posted_comments:
                await self._update_specific_comment_hashes(guild, int(number_str), kind, successfully_posted_comments)

        self.log.info("✅ Step 4 completed: Posted %d new comments", comment_count)

    async def _step5_update_post_status(self, guild: discord.Guild, forum: discord.ForumChannel, snapshot: Dict[str, Any], kind: str) -> None:
        """Step 5: Update forum post status (archived/locked) based on GitHub state."""
        self.log.info("📂 Step 5: Updating post status for %s", kind)

        entities = snapshot.get(kind, {})
        updated_count = 0

        for number_str, data in entities.items():
            thread = await self._get_thread_by_number(guild, number=int(number_str), kind=kind)
            if not thread:
                self.log.debug("Step 5: Could not find thread for %s #%s", kind, number_str)
                continue

            self.log.debug("Step 5: Updating status for %s #%s (state=%s, locked=%s)",
                         kind, number_str, data.get("state"), data.get("locked"))

            # Update status tags and archived/locked state
            labels = data.get("labels", [])
            label_tags = self._labels_to_forum_tags(forum, labels)
            status_tags = self._get_status_tags_for_entity(forum, data, kind)
            desired_tags = label_tags + [t for t in status_tags if t.id not in {lt.id for lt in label_tags}]

            await self._apply_thread_state(thread, data, desired_tags, kind)
            updated_count += 1

        self.log.info("✅ Step 5 completed: Updated status for %d %s posts", updated_count, kind)

    # ----------------------
    # Helper methods for the 5-step process
    # ----------------------
    async def _create_forum_post(self, guild: discord.Guild, forum: discord.ForumChannel, data: Dict[str, Any], kind: str) -> bool:
        """Create a new forum post for a GitHub issue/PR."""
        try:
            number = data["number"]
            self.log.info("Creating forum post for %s #%s: %s", kind, number, data.get("title", ""))

            # Prepare content with avatar and organization info
            title = self._clean_discord_text(data.get("title", ""))[:90] or f"{'Issue' if kind == 'issues' else 'PR'} #{number}"

            # Create initial content
            body = self._clean_discord_text(data.get("body", ""))
            github_url = data.get('url', '')
            url_footer = f"\n\nFrom GitHub: {github_url}"
            max_body_length = 1950 - len(url_footer)

            if len(body) > max_body_length:
                body = body[:max_body_length].rsplit(' ', 1)[0] + "..."

            body = (body + url_footer).strip()
            if len(body) > 1990:
                body = f"From GitHub: {github_url}"

            # Get tags (labels + status tags)
            labels = data.get("labels", [])
            label_tags = self._labels_to_forum_tags(forum, labels)
            status_tags = self._get_status_tags_for_entity(forum, data, kind)
            all_tags = label_tags + [t for t in status_tags if t.id not in {lt.id for lt in label_tags}]

            # Create thread with rate limit handling
            try:
                created = await forum.create_thread(name=title, content=body, applied_tags=all_tags)
                thread = created.thread if hasattr(created, "thread") else created
            except discord.HTTPException as e:
                if e.status == 429:  # Rate limited
                    retry_after = e.response.headers.get('Retry-After', '5')
                    self.log.warning("Discord rate limited while creating %s #%s, waiting %s seconds", kind, number, retry_after)
                    await asyncio.sleep(float(retry_after))
                    # Retry once
                    created = await forum.create_thread(name=title, content=body, applied_tags=all_tags)
                    thread = created.thread if hasattr(created, "thread") else created
                else:
                    raise

            if not isinstance(thread, discord.Thread):
                self.log.error("Failed to create thread for %s #%s: invalid type", kind, number)
                return False

            # Store linkages
            await self._store_link(thread, github_url, kind=kind)
            await self._store_link_by_number(thread, number, kind=kind)

            # Store origin as GitHub since this was created from GitHub data
            await self._store_origin(guild, number, kind, "github", None, None)

            # Post enhanced embed with avatars and colors
            embed_msg = await self._post_initial_embed(thread, data, kind)

            # Store message tracking info
            if embed_msg:
                await self._store_embed_message_id(guild, number, kind, thread.id, embed_msg.id)

            # Store content hashes for change detection (but not comments yet - they'll be processed in Step 4)
            await self._store_content_hashes(guild, number, kind, data, include_comments=False)

            # Apply final state (archived/locked)
            await self._apply_thread_state(thread, data, all_tags, kind)

            return True

        except Exception:
            self.log.exception("Failed to create forum post for %s #%s", kind, data.get("number", "unknown"))
            return False

    async def _edit_forum_post(self, guild: discord.Guild, thread: discord.Thread, data: Dict[str, Any], kind: str) -> bool:
        """Edit an existing forum post with updated content."""
        try:
            number = data["number"]
            self.log.debug("Editing forum post for %s #%s", kind, number)

            # Update thread title if changed
            new_title = self._clean_discord_text(data.get("title", ""))[:90] or f"{'Issue' if kind == 'issues' else 'PR'} #{number}"
            if thread.name != new_title:
                await thread.edit(name=new_title)

            # Update embed message
            embed_msg_id = await self._get_embed_message_id(guild, number, kind)
            if embed_msg_id:
                try:
                    embed_msg = await thread.fetch_message(embed_msg_id)
                    new_embed = await self._create_embed(data, kind)
                    await embed_msg.edit(embed=new_embed)
                except discord.NotFound:
                    # Message was deleted, post a new one
                    new_msg = await self._post_initial_embed(thread, data, kind)
                    if new_msg:
                        await self._store_embed_message_id(guild, number, kind, thread.id, new_msg.id)

            # Update stored hashes
            await self._store_content_hashes(guild, number, kind, data)

            return True

        except Exception:
            self.log.exception("Failed to edit forum post for %s #%s", kind, data.get("number", "unknown"))
            return False

    async def _has_content_changed(self, guild: discord.Guild, number: int, kind: str, cur_data: Dict[str, Any], prev_data: Dict[str, Any]) -> bool:
        """Check if content has changed using hash comparison."""
        try:
            stored_hashes = await self.config.custom("content_hashes", guild.id).get_raw(kind, str(number), default={})

            # Calculate current hashes
            title_hash = self._hash_content(cur_data.get("title", ""))
            body_hash = self._hash_content(cur_data.get("body", ""))

            # Compare with stored hashes
            if (stored_hashes.get("title_hash") != title_hash or
                stored_hashes.get("body_hash") != body_hash):
                return True

            return False

        except Exception:
            self.log.exception("Failed to check content changes for %s #%s", kind, number)
            return True  # Assume changed if we can't determine

    async def _get_new_comments(self, guild: discord.Guild, number: int, kind: str, cur_data: Dict[str, Any], prev_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get new comments that haven't been posted to Discord yet."""
        try:
            # Check batch queue first if in batch mode
            stored_hashes = {}
            if self._batch_config_mode:
                batch_key = f"custom.content_hashes.{kind}.{number}"
                if batch_key in self._pending_config_updates:
                    stored_hashes = self._pending_config_updates[batch_key]

            # Fall back to stored config if not in batch or not found in batch
            if not stored_hashes:
                stored_hashes = await self.config.custom("content_hashes", guild.id).get_raw(kind, str(number), default={})

            stored_comment_hashes = stored_hashes.get("comments", {})

            new_comments = []
            total_comments = len(cur_data.get("comments", []))

            for comment in cur_data.get("comments", []):
                comment_id = comment.get("id")
                if not comment_id:
                    continue

                comment_hash = self._hash_content(comment.get("body", ""))
                stored_hash = stored_comment_hashes.get(str(comment_id))

                self.log.debug("Comment %s: hash=%s, stored=%s, is_new=%s",
                             comment_id, comment_hash[:8],
                             stored_hash[:8] if stored_hash else "None",
                             stored_hash != comment_hash)

                # If hash is different or doesn't exist, it's new/updated
                if stored_hash != comment_hash:
                    new_comments.append(comment)

            self.log.debug("Comments for %s #%s: %d total, %d stored_hashes, %d new",
                         kind, number, total_comments, len(stored_comment_hashes), len(new_comments))

            return new_comments

        except Exception:
            self.log.exception("Failed to get new comments for %s #%s", kind, number)
            return []

    async def _post_comment_to_thread(self, guild: discord.Guild, thread: discord.Thread, comment: Dict[str, Any], entity_data: Dict[str, Any], kind: str) -> bool:
        """Post a GitHub comment to a Discord thread."""
        try:
            # Create comment embed with avatar
            embed = await self._create_comment_embed(comment, entity_data, kind)
            msg = await thread.send(embed=embed)

            # Store comment message tracking and origin
            comment_id = comment.get("id")
            number = entity_data["number"]
            if comment_id and msg:
                await self._store_comment_message_id(guild, number, kind, str(comment_id), msg.id)

                # Store origin as GitHub since this comment came from GitHub
                comment_key = f"{kind}_{number}_{comment_id}"
                await self._store_comment_origin(guild, comment_key, "github", msg.id, str(comment_id))

            # Update comment hash (now stored via the batch system after posting)
            # Note: Comment hashes are now batch-updated in Step 4 after all comments are posted

            return True

        except Exception:
            self.log.exception("Failed to post comment to thread %s", thread.id)
            return False

    # ----------------------
    # Enhanced embed and storage helpers
    # ----------------------
    async def _post_initial_embed(self, thread: discord.Thread, data: Dict[str, Any], kind: str) -> Optional[discord.Message]:
        """Post the initial enhanced embed with avatars and colors."""
        try:
            embed = await self._create_embed(data, kind)
            return await thread.send(embed=embed)
        except Exception:
            self.log.exception("Failed to post initial embed")
            return None

    async def _create_embed(self, data: Dict[str, Any], kind: str) -> discord.Embed:
        """Create an enhanced embed with avatars, colors, and proper formatting."""
        # Get status-based color
        color = self._get_status_color(data, kind)

        # Clean and truncate content
        title = self._clean_discord_text(data.get("title", ""))[:256]
        description = self._clean_discord_text(data.get("body", ""))[:4000]

        embed = discord.Embed(
            title=title,
            url=data.get("url", ""),
            description=description,
            color=color
        )

        # Set author with GitHub avatar
        author_name = data.get("user", "GitHub")
        author_avatar = data.get("user_avatar", "")
        if author_avatar:
            embed.set_author(name=author_name[:256], icon_url=author_avatar)
        else:
            embed.set_author(name=author_name[:256])

        # Set thumbnail to organization avatar if available
        # This will be set from repo_info in the snapshot
        # embed.set_thumbnail(url=org_avatar)  # TODO: Add when we have repo_info

        return embed

    async def _create_comment_embed(self, comment: Dict[str, Any], entity_data: Dict[str, Any], kind: str) -> discord.Embed:
        """Create an embed for a GitHub comment with avatar."""
        # Use a neutral color for comments
        color = discord.Color.blurple()

        # Clean and truncate content
        description = self._clean_discord_text(comment.get("body", ""))[:4000]

        embed = discord.Embed(
            description=description,
            url=comment.get("url", ""),
            color=color
        )

        # Set author with GitHub avatar
        author_name = comment.get("author", "GitHub")
        author_avatar = comment.get("author_avatar", "")
        if author_avatar:
            embed.set_author(name=f"💬 {author_name}"[:256], icon_url=author_avatar)
        else:
            embed.set_author(name=f"💬 {author_name}"[:256])

        return embed

    async def _store_content_hashes(self, guild: discord.Guild, number: int, kind: str, data: Dict[str, Any], *, include_comments: bool = True) -> None:
        """Store content hashes for change detection."""
        try:
            title_hash = self._hash_content(data.get("title", ""))
            body_hash = self._hash_content(data.get("body", ""))

            hash_data = {
                "title_hash": title_hash,
                "body_hash": body_hash,
                "comments": {}  # Always start with empty comments dict
            }

            # Only include comment hashes if explicitly requested (after comments are posted)
            if include_comments:
                comment_hashes = {}
                for comment in data.get("comments", []):
                    comment_id = comment.get("id")
                    if comment_id:
                        comment_hashes[str(comment_id)] = self._hash_content(comment.get("body", ""))
                hash_data["comments"] = comment_hashes

            if self._batch_config_mode:
                # Queue for batch update
                config_path = f"custom.content_hashes.{kind}.{number}"
                self._queue_config_update(config_path, hash_data)
            else:
                # Direct update
                await self.config.custom("content_hashes", guild.id).set_raw(kind, str(number), value=hash_data)
        except Exception:
            self.log.exception("Failed to store content hashes for %s #%s", kind, number)

    async def _store_embed_message_id(self, guild: discord.Guild, number: int, kind: str, thread_id: int, message_id: int) -> None:
        """Store the embed message ID for later editing."""
        try:
            message_data = {
                "thread_id": thread_id,
                "embed_message_id": message_id,
                "comments": {}
            }

            if self._batch_config_mode:
                # Queue for batch update
                config_path = f"custom.discord_messages.{kind}.{number}"
                self._queue_config_update(config_path, message_data)
            else:
                # Direct update
                await self.config.custom("discord_messages", guild.id).set_raw(kind, str(number), value=message_data)
        except Exception:
            self.log.exception("Failed to store embed message ID for %s #%s", kind, number)

    async def _get_embed_message_id(self, guild: discord.Guild, number: int, kind: str) -> Optional[int]:
        """Get the stored embed message ID."""
        try:
            message_data = await self.config.custom("discord_messages", guild.id).get_raw(kind, str(number), default={})
            return message_data.get("embed_message_id")
        except Exception:
            return None

    async def _store_comment_message_id(self, guild: discord.Guild, number: int, kind: str, comment_id: str, message_id: int) -> None:
        """Store a comment's Discord message ID for later editing."""
        try:
            message_data = await self.config.custom("discord_messages", guild.id).get_raw(kind, str(number), default={})
            if "comments" not in message_data:
                message_data["comments"] = {}
            message_data["comments"][str(comment_id)] = message_id
            await self.config.custom("discord_messages", guild.id).set_raw(kind, str(number), value=message_data)
        except Exception:
            self.log.exception("Failed to store comment message ID")

    async def _store_comment_hash(self, guild: discord.Guild, number: int, kind: str, comment_id: str, comment_hash: str) -> None:
        """Store a comment's hash for change detection."""
        try:
            hash_data = await self.config.custom("content_hashes", guild.id).get_raw(kind, str(number), default={})
            if "comments" not in hash_data:
                hash_data["comments"] = {}
            hash_data["comments"][str(comment_id)] = comment_hash
            await self.config.custom("content_hashes", guild.id).set_raw(kind, str(number), value=hash_data)
        except Exception:
            self.log.exception("Failed to store comment hash")

    async def _update_comment_hashes_after_posting(self, guild: discord.Guild, number: int, kind: str, data: Dict[str, Any]) -> None:
        """Update comment hashes after successfully posting comments to Discord."""
        try:
            # Get current stored hashes (might be in batch queue)
            stored_hashes = {}
            if self._batch_config_mode:
                batch_key = f"custom.content_hashes.{kind}.{number}"
                if batch_key in self._pending_config_updates:
                    stored_hashes = self._pending_config_updates[batch_key].copy()

            if not stored_hashes:
                stored_hashes = await self.config.custom("content_hashes", guild.id).get_raw(kind, str(number), default={})

            # Ensure comments dict exists
            if "comments" not in stored_hashes:
                stored_hashes["comments"] = {}

            # Update comment hashes for all comments
            for comment in data.get("comments", []):
                comment_id = comment.get("id")
                if comment_id:
                    comment_hash = self._hash_content(comment.get("body", ""))
                    stored_hashes["comments"][str(comment_id)] = comment_hash

            # Store back (using batch mode if active)
            if self._batch_config_mode:
                batch_key = f"custom.content_hashes.{kind}.{number}"
                self._queue_config_update(batch_key, stored_hashes)
            else:
                await self.config.custom("content_hashes", guild.id).set_raw(kind, str(number), value=stored_hashes)

            self.log.debug("Updated comment hashes for %s #%s: %d comments", kind, number, len(stored_hashes["comments"]))
        except Exception:
            self.log.exception("Failed to update comment hashes after posting for %s #%s", kind, number)

    async def _update_specific_comment_hashes(self, guild: discord.Guild, number: int, kind: str, successfully_posted_comments: List[Dict[str, Any]]) -> None:
        """Update comment hashes for only the comments that were successfully posted to Discord."""
        try:
            # Get current stored hashes (might be in batch queue)
            stored_hashes = {}
            if self._batch_config_mode:
                batch_key = f"custom.content_hashes.{kind}.{number}"
                if batch_key in self._pending_config_updates:
                    stored_hashes = self._pending_config_updates[batch_key].copy()

            if not stored_hashes:
                stored_hashes = await self.config.custom("content_hashes", guild.id).get_raw(kind, str(number), default={})

            # Ensure comments dict exists
            if "comments" not in stored_hashes:
                stored_hashes["comments"] = {}

            # Update comment hashes only for successfully posted comments
            for comment in successfully_posted_comments:
                comment_id = comment.get("id")
                if comment_id:
                    comment_hash = self._hash_content(comment.get("body", ""))
                    stored_hashes["comments"][str(comment_id)] = comment_hash

            # Store back (using batch mode if active)
            if self._batch_config_mode:
                batch_key = f"custom.content_hashes.{kind}.{number}"
                self._queue_config_update(batch_key, stored_hashes)
            else:
                await self.config.custom("content_hashes", guild.id).set_raw(kind, str(number), value=stored_hashes)

            self.log.debug("Updated comment hashes for %s #%s: %d successfully posted comments",
                         kind, number, len(successfully_posted_comments))
        except Exception:
            self.log.exception("Failed to update specific comment hashes for %s #%s", kind, number)

    # ----------------------
    # Discord → GitHub sync handlers
    # ----------------------
    async def _is_discord_to_github_enabled(self, guild: discord.Guild) -> bool:
        """Check if Discord → GitHub sync is enabled for this guild."""
        return await self.config.guild(guild).discord_to_github_enabled()

    async def _handle_new_thread_creation(self, message: discord.Message, kind: str) -> None:
        """Handle creation of a new Discord thread that might need a GitHub issue/PR."""
        try:
            guild = message.guild
            if not guild or not isinstance(message.channel, discord.Thread):
                return

            # Check if Discord → GitHub sync is enabled
            if not await self._is_discord_to_github_enabled(guild):
                self.log.debug("Discord → GitHub sync disabled, skipping thread creation for guild %s", guild.id)
                return

            thread = message.channel

            # Check if this thread is already linked to a GitHub issue/PR
            existing_link = await self._get_link(thread)
            if existing_link:
                # Already linked, don't create new GitHub issue
                return

            # Check if user included a GitHub URL to link to existing issue
            github_url = self._extract_github_url_from_text(message.content)
            if github_url:
                number = self._extract_issue_number(github_url)
                if number:
                    # Link to existing GitHub issue/PR
                    await self._store_link(thread, github_url, kind=kind)
                    await self._store_link_by_number(thread, number, kind=kind)
                    await self._store_origin(guild, number, kind, "github", None, None)
                    self.log.info("Linked Discord thread %s to existing GitHub %s #%s", thread.id, kind, number)
                    return

            # Only create new GitHub issues for issues forum, not PRs
            if kind == "prs":
                self.log.debug("Skipping GitHub PR creation for Discord thread - PRs should be created on GitHub")
                return

            # Create new GitHub issue from Discord thread
            repo = await self._get_repo(guild)
            if not repo:
                return

            # Create GitHub issue
            issue_title = thread.name or "New issue from Discord"
            issue_body = f"Created by {message.author} on Discord: {message.jump_url}\n\n{message.content}"

            # Get labels from Discord thread tags (excluding status tags)
            labels = []
            if hasattr(thread, "applied_tags"):
                for tag in thread.applied_tags:
                    if tag.name.lower() not in self._status_tag_names:
                        labels.append(tag.name)

            github_issue = await asyncio.to_thread(
                lambda: repo.create_issue(title=issue_title, body=issue_body, labels=labels)
            )

            # Store the linkage and origin
            await self._store_link(thread, github_issue.html_url, kind=kind)
            await self._store_link_by_number(thread, github_issue.number, kind=kind)
            await self._store_origin(guild, github_issue.number, kind, "discord", message.id, None)

            self.log.info("Created GitHub issue #%s from Discord thread %s", github_issue.number, thread.id)

        except Exception:
            self.log.exception("Failed to handle new thread creation")

    async def _handle_thread_comment(self, message: discord.Message, kind: str) -> None:
        """Handle a new comment in a Discord thread and sync to GitHub."""
        try:
            guild = message.guild
            if not guild or not isinstance(message.channel, discord.Thread):
                return

            # Check if Discord → GitHub sync is enabled
            if not await self._is_discord_to_github_enabled(guild):
                self.log.debug("Discord → GitHub sync disabled, skipping comment for guild %s", guild.id)
                return

            thread = message.channel

            # Get linked GitHub issue/PR
            github_url = await self._get_link(thread)
            if not github_url:
                return

            issue_number = self._extract_issue_number(github_url)
            if not issue_number:
                return

            repo = await self._get_repo(guild)
            if not repo:
                return

            # Format comment for GitHub with Discord user info
            comment_body = f"**{message.author}** on Discord: {message.jump_url}\n\n{message.content}"

            # Post to GitHub
            if kind == "issues":
                github_comment = await asyncio.to_thread(
                    lambda: repo.get_issue(number=issue_number).create_comment(comment_body)
                )
            else:
                github_comment = await asyncio.to_thread(
                    lambda: repo.get_pull(number=issue_number).create_issue_comment(comment_body)
                )

            # Store the comment origin and mapping
            comment_key = f"{kind}_{issue_number}_{github_comment.id}"
            await self._store_comment_origin(guild, comment_key, "discord", message.id, str(github_comment.id))

            # Update local tracking
            await self._store_comment_message_id(guild, issue_number, kind, str(github_comment.id), message.id)

            self.log.info("Posted Discord comment %s as GitHub comment %s on %s #%s",
                         message.id, github_comment.id, kind, issue_number)

        except Exception:
            self.log.exception("Failed to handle thread comment")

    async def _handle_message_edit(self, before: discord.Message, after: discord.Message, kind: str) -> None:
        """Handle Discord message edit and sync to GitHub."""
        try:
            guild = after.guild
            if not guild or not isinstance(after.channel, discord.Thread):
                return

            # Check if Discord → GitHub sync is enabled
            if not await self._is_discord_to_github_enabled(guild):
                self.log.debug("Discord → GitHub sync disabled, skipping message edit for guild %s", guild.id)
                return

            thread = after.channel

            # Get linked GitHub issue/PR
            github_url = await self._get_link(thread)
            if not github_url:
                return

            issue_number = self._extract_issue_number(github_url)
            if not issue_number:
                return

            # Find the corresponding GitHub comment
            comment_key = await self._find_comment_by_discord_message(guild, after.id, kind)
            if not comment_key:
                return

            # Check origin - only edit GitHub if this comment originated from Discord
            origin_data = await self._get_comment_origin(guild, comment_key)
            if not origin_data or origin_data.get("origin") != "discord":
                return

            github_comment_id = origin_data.get("github_comment_id")
            if not github_comment_id:
                return

            repo = await self._get_repo(guild)
            if not repo:
                return

            # Update GitHub comment
            new_comment_body = f"**{after.author}** on Discord: {after.jump_url}\n\n{after.content}"

            if kind == "issues":
                await asyncio.to_thread(
                    lambda: repo.get_issue(number=issue_number).get_comment(int(github_comment_id)).edit(new_comment_body)
                )
            else:
                await asyncio.to_thread(
                    lambda: repo.get_pull(number=issue_number).get_comment(int(github_comment_id)).edit(new_comment_body)
                )

            self.log.info("Updated GitHub comment %s from Discord edit %s on %s #%s",
                         github_comment_id, after.id, kind, issue_number)

        except Exception:
            self.log.exception("Failed to handle message edit")

    async def _handle_message_delete(self, message: discord.Message, kind: str) -> None:
        """Handle Discord message deletion and sync to GitHub."""
        try:
            guild = message.guild
            if not guild or not isinstance(message.channel, discord.Thread):
                return

            # Check if Discord → GitHub sync is enabled
            if not await self._is_discord_to_github_enabled(guild):
                self.log.debug("Discord → GitHub sync disabled, skipping message delete for guild %s", guild.id)
                return

            thread = message.channel

            # Get linked GitHub issue/PR
            github_url = await self._get_link(thread)
            if not github_url:
                return

            issue_number = self._extract_issue_number(github_url)
            if not issue_number:
                return

            # Find the corresponding GitHub comment
            comment_key = await self._find_comment_by_discord_message(guild, message.id, kind)
            if not comment_key:
                return

            # Check origin - only delete GitHub comment if it originated from Discord
            origin_data = await self._get_comment_origin(guild, comment_key)
            if not origin_data or origin_data.get("origin") != "discord":
                return

            github_comment_id = origin_data.get("github_comment_id")
            if not github_comment_id:
                return

            repo = await self._get_repo(guild)
            if not repo:
                return

            # Delete GitHub comment
            if kind == "issues":
                await asyncio.to_thread(
                    lambda: repo.get_issue(number=issue_number).get_comment(int(github_comment_id)).delete()
                )
            else:
                await asyncio.to_thread(
                    lambda: repo.get_pull(number=issue_number).get_comment(int(github_comment_id)).delete()
                )

            # Remove from tracking
            await self._remove_comment_origin(guild, comment_key)

            self.log.info("Deleted GitHub comment %s from Discord deletion %s on %s #%s",
                         github_comment_id, message.id, kind, issue_number)

        except Exception:
            self.log.exception("Failed to handle message delete")

    # ----------------------
    # Origin tracking helper methods
    # ----------------------
    async def _store_origin(self, guild: discord.Guild, number: int, kind: str, origin: str, discord_message_id: Optional[int], github_comment_id: Optional[str]) -> None:
        """Store the origin of content for conflict resolution."""
        try:
            origin_data = {
                "origin": origin,
                "discord_message_id": discord_message_id,
                "github_comment_id": github_comment_id
            }

            if self._batch_config_mode:
                # Queue for batch update
                config_path = f"custom.content_origins.{kind}.{number}"
                self._queue_config_update(config_path, origin_data)
            else:
                # Direct update
                await self.config.custom("content_origins", guild.id).set_raw(kind, str(number), value=origin_data)
        except Exception:
            self.log.exception("Failed to store origin data")

    async def _get_origin(self, guild: discord.Guild, number: int, kind: str) -> Optional[Dict[str, Any]]:
        """Get the origin data for content."""
        try:
            return await self.config.custom("content_origins", guild.id).get_raw(kind, str(number), default=None)
        except Exception:
            return None

    async def _store_comment_origin(self, guild: discord.Guild, comment_key: str, origin: str, discord_message_id: int, github_comment_id: str) -> None:
        """Store the origin of a comment for conflict resolution."""
        try:
            origin_data = {
                "origin": origin,
                "discord_message_id": discord_message_id,
                "github_comment_id": github_comment_id
            }
            await self.config.custom("content_origins", guild.id).set_raw("comments", comment_key, value=origin_data)
        except Exception:
            self.log.exception("Failed to store comment origin")

    async def _get_comment_origin(self, guild: discord.Guild, comment_key: str) -> Optional[Dict[str, Any]]:
        """Get the origin data for a comment."""
        try:
            return await self.config.custom("content_origins", guild.id).get_raw("comments", comment_key, default=None)
        except Exception:
            return None

    async def _remove_comment_origin(self, guild: discord.Guild, comment_key: str) -> None:
        """Remove comment origin tracking."""
        try:
            comments_data = await self.config.custom("content_origins", guild.id).get_raw("comments", default={})
            if comment_key in comments_data:
                del comments_data[comment_key]
                await self.config.custom("content_origins", guild.id).set_raw("comments", value=comments_data)
        except Exception:
            self.log.exception("Failed to remove comment origin")

    async def _find_comment_by_discord_message(self, guild: discord.Guild, discord_message_id: int, kind: str) -> Optional[str]:
        """Find a comment key by Discord message ID."""
        try:
            comments_data = await self.config.custom("content_origins", guild.id).get_raw("comments", default={})
            for comment_key, origin_data in comments_data.items():
                if (origin_data.get("discord_message_id") == discord_message_id and
                    comment_key.startswith(f"{kind}_")):
                    return comment_key
            return None
        except Exception:
            return None

    async def _reconcile_entities(
        self,
        guild: discord.Guild,
        *,
        forum: discord.ForumChannel,
        prev_map: Dict[str, Any],
        cur_map: Dict[str, Any],
        kind: str,
    ) -> None:
        """Reconcile GitHub entities (issues/PRs) to Discord forum threads."""
        name_prefix = "Issue #" if kind == "issues" else "PR #"
        created_count = 0
        updated_count = 0
        failed_count = 0

        for num_str, data in cur_map.items():
            try:
                number = int(num_str)
                thread = await self._get_thread_by_number(guild, number=number, kind=kind)
                labels = data.get("labels", [])

                # Build desired tags: labels + status tags
                label_tags = self._labels_to_forum_tags(forum, labels)
                status_tags = self._get_status_tags_for_entity(forum, data, kind)
                desired_tags = label_tags + [t for t in status_tags if t.id not in {lt.id for lt in label_tags}]

                if not thread:
                    # Create new thread
                    self.log.info("Creating thread for %s #%s: %s", kind, number, data.get("title", ""))
                    try:
                        # Clean and validate title (Discord limit: 100 chars)
                        raw_title = data.get("title") or ""
                        title = self._clean_discord_text(raw_title)[:90] or f"{name_prefix}{number}"

                        # Clean and validate body (Discord limit: 2000 chars for forum posts)
                        raw_body = data.get("body") or ""
                        body = self._clean_discord_text(raw_body)

                        # Ensure we stay well under the 2000 character limit
                        github_url = data.get('url', '')
                        url_footer = f"\n\nFrom GitHub: {github_url}"
                        max_body_length = 1950 - len(url_footer)  # Leave buffer for URL footer

                        if len(body) > max_body_length:
                            body = body[:max_body_length].rsplit(' ', 1)[0] + "..."  # Cut at word boundary

                        body = (body + url_footer).strip()

                        # Final safety check
                        if len(body) > 1990:  # Leave 10 char buffer
                            body = f"From GitHub: {github_url}"

                        created = await forum.create_thread(name=title, content=body)
                        thread = created.thread if hasattr(created, "thread") else created  # type: ignore

                        if not isinstance(thread, discord.Thread):
                            self.log.error("Failed to create thread for %s #%s: invalid type", kind, number)
                            continue

                        # Store linkages
                        await self._store_link(thread, data.get("url", ""), kind=kind)
                        await self._store_link_by_number(thread, number, kind=kind)
                        created_count += 1

                        # Post initial embed with author
                        try:
                            # Clean embed fields (Discord embed description limit: 4096 chars)
                            embed_title = self._clean_discord_text(data.get("title", ""))[:256]  # Embed title limit
                            embed_desc = self._clean_discord_text(data.get("body", ""))[:4000]  # Leave buffer

                            embed = discord.Embed(
                                title=embed_title,
                                url=data.get("url", ""),
                                description=embed_desc
                            )
                            if data.get("user"):
                                embed.set_author(name=data.get("user")[:256])  # Author name limit
                            await thread.send(embed=embed)
                        except Exception:
                            self.log.exception("Failed to post initial embed for %s #%s", kind, number)

                        # Apply tags and state
                        await self._apply_thread_state(thread, data, desired_tags, kind)

                    except discord.HTTPException as e:
                        failed_count += 1
                        if e.code == 50035:  # Invalid Form Body
                            self.log.error("Invalid form body creating thread for %s #%s (error 50035): %s", kind, number, str(e))
                            # Log detailed information for debugging
                            self.log.debug("Problem data - Title: %r (len %d), Body: %r (len %d)",
                                         data.get("title", "")[:100], len(data.get("title", "")),
                                         (body if 'body' in locals() else data.get("body", ""))[:100],
                                         len(body if 'body' in locals() else data.get("body", "")))
                        elif e.code == 50013:  # Missing Permissions
                            self.log.error("Missing permissions to create thread for %s #%s (error 50013)", kind, number)
                        elif e.code == 160002:  # You are being rate limited
                            self.log.warning("Rate limited creating thread for %s #%s (error 160002), will retry later", kind, number)
                            # Don't store failed state for rate limits - allow retry next sync
                        elif e.code == 30007:  # Maximum number of forum threads reached
                            self.log.error("Maximum forum threads reached for %s #%s (error 30007)", kind, number)
                        else:
                            self.log.exception("Discord HTTP error creating thread for %s #%s (code %s): %s",
                                             kind, number, e.code, str(e))
                        continue
                    except Exception as e:
                        failed_count += 1
                        self.log.exception("Failed to create thread for %s #%s: %s", kind, number, str(e))
                        continue
                else:
                    # Update existing thread
                    updated_count += 1
                    await self._update_thread_state(thread, data, desired_tags, kind)

                # Sync comments: only post new ones (only if thread exists)
                if thread:
                    await self._sync_comments_to_thread(thread, prev_map.get(num_str, {}), data)

            except Exception:
                self.log.exception("Failed to reconcile %s #%s", kind, num_str)
                continue

        self.log.info("Reconciled %s: %d created, %d updated, %d failed", kind, created_count, updated_count, failed_count)

    def _get_status_tags_for_entity(self, forum: discord.ForumChannel, data: Dict[str, Any], kind: str) -> List[discord.ForumTag]:
        """Get Discord-only status tags for an entity based on its state."""
        status_names = []

        if kind == "issues":
            if data.get("state") == "closed":
                # Check if issue was closed as not planned/duplicate
                state_reason = data.get("state_reason")
                state_reason_lower = state_reason.lower() if state_reason else ""
                if state_reason_lower in ["not_planned", "duplicate"]:
                    status_names.append("not resolved")
                else:
                    status_names.append("closed")
            elif data.get("state") == "open":
                status_names.append("open")
        elif kind == "prs":
            if data.get("merged"):
                status_names.append("merged")
            elif data.get("state") == "closed":
                status_names.append("closed")
            elif data.get("state") == "open":
                status_names.append("open")

        return self._labels_to_forum_tags(forum, status_names)

    async def _apply_thread_state(self, thread: discord.Thread, data: Dict[str, Any], desired_tags: List[discord.ForumTag], kind: str) -> None:
        """Apply tags and archived/locked state to a thread."""
        try:
            # Mark this thread as being updated by the bot to prevent feedback loops
            self._bot_updating_threads.add(thread.id)
            # Determine desired archived/locked state
            desired_archived = False
            desired_locked = data.get("locked", False)

            if kind == "issues":
                # Issues are archived when closed (regardless of reason)
                desired_archived = data.get("state") == "closed"
                # Issues should also be locked when closed (to prevent further Discord comments)
                if desired_archived:
                    desired_locked = True
            elif kind == "prs":
                # PRs are archived when closed or merged
                desired_archived = data.get("state") == "closed" or data.get("merged", False)
                # PRs should also be locked when closed or merged (to prevent further Discord comments)
                if desired_archived:
                    desired_locked = True

            # Log the state being applied
            issue_number = data.get("number", "unknown")
            state_info = f"{data.get('state', 'unknown')}"
            if kind == "prs" and data.get("merged"):
                state_info += " (merged)"

            self.log.debug("Applying state to %s #%s thread %s: GitHub state=%s, desired archived=%s, locked=%s",
                         kind, issue_number, thread.id, state_info, desired_archived, desired_locked)

            # Check what needs updating
            current_tag_ids = {t.id for t in getattr(thread, "applied_tags", [])}
            desired_tag_ids = {t.id for t in desired_tags}
            needs_tag_update = current_tag_ids != desired_tag_ids
            needs_state_update = (thread.archived != desired_archived or thread.locked != desired_locked)

            # Handle archived threads specially
            if thread.archived and (needs_tag_update or needs_state_update):
                if desired_archived:
                    # Thread should stay archived - unarchive temporarily to update tags, then re-archive
                    if needs_tag_update:
                        self.log.debug("Temporarily unarchiving thread %s to update tags, will re-archive", thread.id)
                        await asyncio.sleep(0.05)
                        await thread.edit(archived=False)
                        # Refresh thread state
                        thread = await thread.guild.fetch_channel(thread.id)  # type: ignore

                        await asyncio.sleep(0.05)
                        await thread.edit(applied_tags=desired_tags)
                        self.log.debug("Updated tags for thread %s", thread.id)

                        # Re-archive with final state
                        await asyncio.sleep(0.05)
                        await thread.edit(archived=True, locked=desired_locked)
                        self.log.debug("Re-archived thread %s with locked=%s", thread.id, desired_locked)
                    elif thread.locked != desired_locked:
                        # Only locked state needs updating
                        await asyncio.sleep(0.05)
                        await thread.edit(archived=False)
                        await asyncio.sleep(0.05)
                        await thread.edit(archived=True, locked=desired_locked)
                        self.log.debug("Updated locked state for archived thread %s", thread.id)
                else:
                    # Thread should be unarchived
                    await asyncio.sleep(0.05)
                    await thread.edit(archived=False, locked=desired_locked)
                    # Refresh thread state
                    thread = await thread.guild.fetch_channel(thread.id)  # type: ignore

                    if needs_tag_update:
                        await asyncio.sleep(0.05)
                        await thread.edit(applied_tags=desired_tags)
                        self.log.debug("Updated tags after unarchiving thread %s", thread.id)
            else:
                # Thread is not archived, normal update flow
                if needs_tag_update:
                    await asyncio.sleep(0.05)
                    await thread.edit(applied_tags=desired_tags)
                    self.log.debug("Updated tags for thread %s", thread.id)

                if needs_state_update:
                    await asyncio.sleep(0.05)
                    await thread.edit(archived=desired_archived, locked=desired_locked)
                    self.log.debug("Updated state for thread %s: archived=%s, locked=%s",
                                 thread.id, desired_archived, desired_locked)

        except discord.HTTPException as e:
            if e.code == 50083:  # Thread is archived
                self.log.warning("Cannot edit archived thread %s (error 50083), skipping state update", thread.id)
            elif e.code == 50001:  # Missing Access
                self.log.warning("Missing permissions to edit thread %s (error 50001)", thread.id)
            elif e.code == 50013:  # Missing Permissions
                self.log.warning("Missing permissions to edit thread %s (error 50013)", thread.id)
            elif e.code == 10003:  # Unknown Channel
                self.log.warning("Thread %s no longer exists (error 10003)", thread.id)
            elif e.code == 40058:  # Everything is archived
                self.log.warning("Cannot edit thread %s in archived forum channel (error 40058)", thread.id)
            else:
                self.log.exception("Discord HTTP error applying thread state for thread %s (code %s): %s",
                                 thread.id, e.code, e)
        except discord.NotFound:
            self.log.warning("Thread %s not found when trying to update state", thread.id)
        except discord.Forbidden:
            self.log.warning("No permission to edit thread %s", thread.id)
        except Exception:
            self.log.exception("Failed to apply thread state for thread %s", thread.id)
        finally:
            # Always remove from tracking set when done
            self._bot_updating_threads.discard(thread.id)

    async def _update_thread_state(self, thread: discord.Thread, data: Dict[str, Any], desired_tags: List[discord.ForumTag], kind: str) -> None:
        """Update an existing thread's state if needed."""
        await self._apply_thread_state(thread, data, desired_tags, kind)

    async def _sync_comments_to_thread(self, thread: discord.Thread, prev_data: Dict[str, Any], cur_data: Dict[str, Any]) -> None:
        """Sync new comments from GitHub to Discord thread."""
        prev_comments = {str(c.get("id")): c for c in prev_data.get("comments", [])}

        for comment in cur_data.get("comments", []):
            cid = str(comment.get("id"))
            if cid and cid != "None" and cid not in prev_comments:
                try:
                    # Clean comment text for Discord (embed description limit: 4096 chars)
                    comment_body = self._clean_discord_text(comment.get("body", ""))[:4000]

                    embed = discord.Embed(
                        description=comment_body,
                        url=comment.get("url", "")
                    )
                    if comment.get("author"):
                        embed.set_author(name=comment.get("author")[:256])  # Author name limit

                    await asyncio.sleep(0.05)  # Rate limit prevention
                    await thread.send(embed=embed)
                    self.log.debug("Posted comment %s to thread %s", cid, thread.id)

                except Exception:
                    self.log.exception("Failed to post comment %s to thread %s", cid, thread.id)

    async def _update_local_snapshot_with_comment(self, guild: discord.Guild, number: int, kind: str, comment: Dict[str, Any]) -> None:
        """Update the local snapshot with a new comment to prevent re-processing."""
        try:
            snapshot = await self.config.custom("state", guild.id).get_raw("snapshot", default=None)
            if not snapshot:
                return

            entity_key = str(number)
            entity_map = snapshot.get(kind, {})

            if entity_key in entity_map:
                if "comments" not in entity_map[entity_key]:
                    entity_map[entity_key]["comments"] = []
                entity_map[entity_key]["comments"].append(comment)

                # Save back to config
                await self.config.custom("state", guild.id).set_raw("snapshot", value=snapshot)
                self.log.debug("Updated local snapshot with comment for %s #%s", kind, number)
        except Exception:
            self.log.exception("Failed to update local snapshot with comment")

    async def _fix_thread_status_tags(self, thread: discord.Thread, snapshot: Dict[str, Any], kind: str) -> None:
        """Fix status tags for a single thread based on GitHub snapshot data."""
        try:
            # Mark this thread as being updated by the bot
            self._bot_updating_threads.add(thread.id)
            # Find the corresponding GitHub issue/PR for this thread
            number = None
            for num_str, data in snapshot.get(kind, {}).items():
                # Try to match by thread name or stored linkage
                thread_id = await self._get_thread_id_by_number(thread.guild, number=int(num_str), kind=kind)
                if thread_id == thread.id:
                    number = int(num_str)
                    break

            if number is None:
                # Try to extract number from thread name as fallback
                import re
                match = re.search(r'#(\d+)', thread.name)
                if match:
                    number = int(match.group(1))
                    if str(number) not in snapshot.get(kind, {}):
                        return  # Number found but no data in snapshot
                else:
                    return  # Can't determine which issue/PR this is

            # Get the GitHub data
            data = snapshot.get(kind, {}).get(str(number))
            if not data:
                return

            # Determine correct status tags
            forum = thread.parent
            if not isinstance(forum, discord.ForumChannel):
                return

            desired_status_tags = self._get_status_tags_for_entity(forum, data, kind)

            # Get current tags, keep non-status tags
            current_tags = list(getattr(thread, "applied_tags", []))
            non_status_tags = [t for t in current_tags if t.name.lower() not in self._status_tag_names]

            # Combine non-status tags with desired status tags
            new_tags = non_status_tags + desired_status_tags

            # Only update if tags actually changed
            current_tag_ids = {t.id for t in current_tags}
            new_tag_ids = {t.id for t in new_tags}

            if current_tag_ids != new_tag_ids:
                # Handle archived threads - need to unarchive temporarily
                was_archived = thread.archived
                if was_archived:
                    await thread.edit(archived=False)
                    # Re-fetch to get updated state
                    thread = await thread.guild.fetch_channel(thread.id)  # type: ignore

                await asyncio.sleep(0.1)  # Rate limit protection
                await thread.edit(applied_tags=new_tags)

                # Re-archive if it was archived before
                if was_archived:
                    await asyncio.sleep(0.1)
                    await thread.edit(archived=True)

                self.log.debug("Fixed status tags for %s thread %s (#%s)", kind, thread.id, number)

        except Exception:
            self.log.exception("Failed to fix status tags for thread %s", thread.id)
        finally:
            # Always remove from tracking set when done
            self._bot_updating_threads.discard(thread.id)

    async def _fix_thread_complete_state(self, thread: discord.Thread, snapshot: Dict[str, Any], kind: str) -> None:
        """Fix both status tags and archived/locked state for a single thread based on GitHub snapshot data."""
        try:
            # Mark this thread as being updated by the bot
            self._bot_updating_threads.add(thread.id)

            # Find the corresponding GitHub issue/PR for this thread
            number = None
            for num_str, data in snapshot.get(kind, {}).items():
                # Try to match by thread name or stored linkage
                thread_id = await self._get_thread_id_by_number(thread.guild, number=int(num_str), kind=kind)
                if thread_id == thread.id:
                    number = int(num_str)
                    break

            if number is None:
                # Try to extract number from thread name as fallback
                import re
                match = re.search(r'#(\d+)', thread.name)
                if match:
                    number = int(match.group(1))
                    if str(number) not in snapshot.get(kind, {}):
                        return  # Number found but no data in snapshot
                else:
                    return  # Can't determine which issue/PR this is

            # Get the GitHub data
            data = snapshot.get(kind, {}).get(str(number))
            if not data:
                return

            # Get forum and labels
            forum = thread.parent
            if not isinstance(forum, discord.ForumChannel):
                return

            labels = data.get("labels", [])
            label_tags = self._labels_to_forum_tags(forum, labels)
            status_tags = self._get_status_tags_for_entity(forum, data, kind)
            desired_tags = label_tags + [t for t in status_tags if t.id not in {lt.id for lt in label_tags}]

            # Apply the complete state (tags + archived/locked)
            await self._apply_thread_state(thread, data, desired_tags, kind)

            self.log.debug("Fixed complete state for %s thread %s (#%s)", kind, thread.id, number)

        except Exception:
            self.log.exception("Failed to fix complete state for thread %s", thread.id)
        finally:
            # Always remove from tracking set when done
            self._bot_updating_threads.discard(thread.id)

    async def _ensure_status_tags_exist(self, forum: discord.ForumChannel, kind: str) -> None:
        """Ensure required status tags exist in the forum channel."""
        try:
            existing_tag_names = {t.name.lower() for t in forum.available_tags}
            required_tags = self._required_issue_tags if kind == "issues" else self._required_pr_tags

            # Check if we're at Discord's 20 tag limit
            missing_tags = [tag for tag in required_tags if tag.lower() not in existing_tag_names]
            if not missing_tags:
                return  # All required tags already exist

            total_tags_after = len(forum.available_tags) + len(missing_tags)
            if total_tags_after > 20:
                self.log.warning(
                    "Cannot create %d missing status tags in %s forum: would exceed Discord's 20 tag limit "
                    "(current: %d, would be: %d). Missing tags: %s",
                    len(missing_tags), kind, len(forum.available_tags), total_tags_after, missing_tags
                )
                # Try to suggest which tags could be removed
                non_status_tags = [t.name for t in forum.available_tags if t.name.lower() not in self._status_tag_names]
                if non_status_tags:
                    self.log.info(
                        "Consider removing some existing tags to make room for status tags. "
                        "Non-status tags: %s", non_status_tags[:5]  # Show first 5
                    )
                return

            for tag_name in missing_tags:
                try:
                    # Create the missing status tag with appropriate color
                    color = None
                    if tag_name == "open":
                        color = discord.Color.green()
                    elif tag_name == "closed":
                        color = discord.Color.red()
                    elif tag_name == "merged":
                        color = discord.Color.purple()
                    elif tag_name == "not resolved":
                        color = discord.Color.greyple()

                    await forum.create_tag(name=tag_name, emoji=None, moderated=False)
                    self.log.info("Created status tag '%s' in %s forum", tag_name, kind)
                except discord.HTTPException as e:
                    if e.code == 50035 and "Must be 20 or fewer in length" in str(e):
                        self.log.error(
                            "Cannot create status tag '%s' in %s forum: Discord 20 tag limit reached. "
                            "Current tags: %d. Please remove some existing tags manually.",
                            tag_name, kind, len(forum.available_tags)
                        )
                    else:
                        self.log.exception("Failed to create status tag '%s' in %s forum (HTTP %s): %s",
                                         tag_name, kind, e.code, e)
                except Exception:
                    self.log.exception("Failed to create status tag '%s' in %s forum", tag_name, kind)
        except Exception:
            self.log.exception("Failed to ensure status tags exist for %s forum", kind)

    async def _reconcile_forum_and_labels(self, guild: discord.Guild, forum: discord.ForumChannel, repo) -> None:
        try:
            gh_labels = list(repo.get_labels())
            gh_label_names = {lbl.name for lbl in gh_labels}
            forum_tag_names = {t.name for t in forum.available_tags}
            # Create missing GitHub labels from forum tags
            for name in sorted(forum_tag_names - gh_label_names):
                if name.lower() in self._status_tag_names:
                    continue
                with contextlib.suppress(Exception):
                    repo.create_label(name=name, color="ededed")
            # Create missing forum tags from GitHub labels
            for name in sorted(gh_label_names - forum_tag_names):
                with contextlib.suppress(Exception):
                    await forum.create_tag(name=name)
        except Exception:
            pass

    async def _store_node_id_by_number(self, guild: discord.Guild, number: int, node_id: str, *, kind: str) -> None:
        if kind == "issues":
            await self.config.custom("issues", guild.id).set_raw("node_id_by_number", str(number), value=node_id)
        elif kind == "prs":
            await self.config.custom("prs", guild.id).set_raw("node_id_by_number", str(number), value=node_id)

    async def _get_node_id_by_number(self, guild: discord.Guild, number: int, *, kind: str) -> Optional[str]:
        try:
            if kind == "issues":
                return await self.config.custom("issues", guild.id).get_raw("node_id_by_number", str(number), default=None)
            return await self.config.custom("prs", guild.id).get_raw("node_id_by_number", str(number), default=None)
        except Exception:
            return None

    async def _delete_github_issue_by_node(self, guild: discord.Guild, node_id: str) -> None:
        gh = await self._get_github_client(guild)
        if not gh:
            return
        # PyGithub doesn't expose GraphQL deleteIssue; use REST fallback: close issue via node lookup is non-trivial
        # Best-effort: this will be a no-op unless extended with a GraphQL client.
        return

