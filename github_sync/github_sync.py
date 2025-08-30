from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

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

# Optimized GraphQL queries for fast data fetching with maximum GitHub API limits
# Uses maximum batch sizes: 100 issues + 100 PRs per call for 4x speed improvement

# Main repository query with optimal batch sizes
REPOSITORY_QUERY = """
query($owner: String!, $repo: String!, $issuesCursor: String, $prsCursor: String) {
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
  repository(owner: $owner, name: $repo) {
    id
    name
    nameWithOwner
    description
    defaultBranchRef {
      name
    }
    owner {
      login
      avatarUrl
      url
      ... on Organization {
        name
        description
      }
      ... on User {
        name
        bio
      }
    }
    labels(first: 100) {
      nodes {
        id
        name
        color
        description
        createdAt
        updatedAt
      }
    }
    issues(first: 100, after: $issuesCursor, states: [OPEN], orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        number
        title
        state
        stateReason
        locked
        url
        body
        bodyText
        createdAt
        updatedAt
        closedAt
        author {
          login
          avatarUrl
          url
          ... on User {
            name
            bio
          }
          ... on Organization {
            name
            description
          }
        }
        assignees(first: 10) {
          nodes {
            login
            avatarUrl
          }
        }
        labels(first: 20) {
          nodes {
            id
            name
            color
            description
          }
        }
        comments(first: 100) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            databaseId
            author {
              login
              avatarUrl
              url
            }
            body
            bodyText
            url
            createdAt
            updatedAt
            lastEditedAt
          }
        }
        milestone {
          id
          title
          description
          state
        }
      }
    }
    pullRequests(first: 100, after: $prsCursor, states: [OPEN], orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        number
        title
        state
        merged
        mergedAt
        mergeable
        locked
        url
        body
        bodyText
        createdAt
        updatedAt
        closedAt
        headRefName
        baseRefName
        author {
          login
          avatarUrl
          url
          ... on User {
            name
            bio
          }
          ... on Organization {
            name
            description
          }
        }
        assignees(first: 10) {
          nodes {
            login
            avatarUrl
          }
        }
        reviewers: reviewRequests(first: 10) {
          nodes {
            requestedReviewer {
              ... on User {
                login
                avatarUrl
              }
              ... on Team {
                name
                slug
              }
            }
          }
        }
        labels(first: 20) {
          nodes {
            id
            name
            color
            description
          }
        }
        comments(first: 100) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            databaseId
            author {
              login
              avatarUrl
              url
            }
            body
            bodyText
            url
            createdAt
            updatedAt
            lastEditedAt
          }
        }
        milestone {
          id
          title
          description
          state
        }
        reviews(first: 50) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            author {
              login
              avatarUrl
              url
            }
            state
            submittedAt
            body
            bodyText
            url
            createdAt
            updatedAt
            comments(first: 50) {
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                id
                databaseId
                author {
                  login
                  avatarUrl
                  url
                }
                body
                bodyText
                url
                path
                position
                line
                originalPosition
                diffHunk
                createdAt
                updatedAt
                lastEditedAt
              }
            }
          }
        }
        commits(last: 1) {
          nodes {
            commit {
              oid
              messageHeadline
              author {
                name
                email
                date
              }
            }
          }
        }
      }
    }
  }
}
"""

# Query to fetch recently closed/merged issues and PRs for cleanup
CLEANUP_QUERY = """
query($owner: String!, $repo: String!, $since: DateTime!) {
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
  repository(owner: $owner, name: $repo) {
    issues(first: 100, states: [CLOSED], filterBy: {since: $since}, orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        number
        state
        stateReason
        updatedAt
        closedAt
      }
    }
    pullRequests(first: 100, states: [CLOSED, MERGED], orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        number
        state
        merged
        updatedAt
        closedAt
        mergedAt
      }
    }
  }
}
"""

# Separate queries for parallel fetching when one resource type is done
ISSUES_ONLY_QUERY = """
query($owner: String!, $repo: String!, $issuesCursor: String) {
  rateLimit {
    cost
    remaining
  }
  repository(owner: $owner, name: $repo) {
    issues(first: 100, after: $issuesCursor, states: [OPEN], orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        number
        title
        state
        stateReason
        locked
        url
        body
        bodyText
        createdAt
        updatedAt
        closedAt
        author {
          login
          avatarUrl
          url
          ... on User {
            name
            bio
          }
          ... on Organization {
            name
            description
          }
        }
        assignees(first: 10) {
          nodes {
            login
            avatarUrl
          }
        }
        labels(first: 20) {
          nodes {
            id
            name
            color
            description
          }
        }
        comments(first: 100) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            databaseId
            author {
              login
              avatarUrl
              url
            }
            body
            bodyText
            url
            createdAt
            updatedAt
            lastEditedAt
          }
        }
        milestone {
          id
          title
          description
          state
        }
      }
    }
  }
}
"""

PRS_ONLY_QUERY = """
query($owner: String!, $repo: String!, $prsCursor: String) {
  rateLimit {
    cost
    remaining
  }
  repository(owner: $owner, name: $repo) {
    pullRequests(first: 100, after: $prsCursor, states: [OPEN], orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        number
        title
        state
        merged
        mergedAt
        mergeable
        locked
        url
        body
        bodyText
        createdAt
        updatedAt
        closedAt
        headRefName
        baseRefName
        author {
          login
          avatarUrl
          url
          ... on User {
            name
            bio
          }
          ... on Organization {
            name
            description
          }
        }
        assignees(first: 10) {
          nodes {
            login
            avatarUrl
          }
        }
        reviewers: reviewRequests(first: 10) {
          nodes {
            requestedReviewer {
              ... on User {
                login
                avatarUrl
              }
              ... on Team {
                name
                slug
              }
            }
          }
        }
        labels(first: 20) {
          nodes {
            id
            name
            color
            description
          }
        }
        comments(first: 100) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            databaseId
            author {
              login
              avatarUrl
              url
            }
            body
            bodyText
            url
            createdAt
            updatedAt
            lastEditedAt
          }
        }
        milestone {
          id
          title
          description
          state
        }
        reviews(first: 50) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            author {
              login
              avatarUrl
              url
            }
            state
            submittedAt
            body
            bodyText
            url
            createdAt
            updatedAt
            comments(first: 50) {
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                id
                databaseId
                author {
                  login
                  avatarUrl
                  url
                }
                body
                bodyText
                url
                path
                position
                line
                originalPosition
                diffHunk
                createdAt
                updatedAt
                lastEditedAt
              }
            }
          }
        }
        commits(last: 1) {
          nodes {
            commit {
              oid
              messageHeadline
              author {
                name
                email
                date
              }
            }
          }
        }
      }
    }
  }
}
"""


class GitHubSync(commands.Cog):
    """
    Sync ACTIVE GitHub issues and pull requests with Discord forum posts.

    OPTIMIZED FOR ACTIVE CONTENT ONLY:
    - Only fetches and syncs OPEN issues and PRs from GitHub
    - Creates forum posts only for open GitHub issues/PRs
    - Automatically deletes Discord threads when GitHub issues/PRs are closed
    - Syncs comments between GitHub and Discord (bidirectional)
    - Syncs Discord forum tags with GitHub labels
    - Auto-applies "open" status tags
    - Prevents feedback loops: origin-aware sync direction
    - Clean state management: no storage of closed/merged entities
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
        # Since we only work with open entities now, we only need "open" status tags
        self._status_tag_names: set[str] = {"open"}

        # Define required status tags for each type (simplified for open-only)
        self._required_issue_tags = {"open"}
        self._required_pr_tags = {"open"}

        # Track threads being updated/created by the bot to prevent sync loops
        self._bot_updating_threads: set[int] = set()
        self._bot_creating_threads: set[int] = set()  # Track threads being created during sync

        # Concurrency control for parallel operations
        self._discord_semaphore = asyncio.Semaphore(5)  # Max 5 concurrent Discord operations
        self._github_semaphore = asyncio.Semaphore(3)   # Max 3 concurrent GitHub operations

        # Snapshot storage
        self.config.init_custom("state", 1)

        # Content tracking for hash-based change detection
        self.config.init_custom("content_hashes", 1)

        # State tracking for hash-based state change detection
        self.config.init_custom("state_hashes", 1)

        self.config.register_custom("content_hashes",
            issues={},  # number -> {"title_hash": str, "body_hash": str, "comments": {comment_id: hash}}
            prs={}      # number -> {"title_hash": str, "body_hash": str, "comments": {comment_id: hash}}
        )

        self.config.register_custom("state_hashes",
            issues={},  # number -> state_hash
            prs={}      # number -> state_hash
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

        self.log.debug("Ending batch config mode, applying %d pending updates", len(self._pending_config_updates))

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

            self.log.debug("Successfully applied %d batched config updates", len(self._pending_config_updates))

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
                        # Log GraphQL errors but continue if we have partial data
                        errors = data["errors"]
                        self.log.warning("GraphQL query had %d errors (continuing with partial data): %s",
                                       len(errors), [e.get("message", str(e))[:100] for e in errors[:3]])

                        # Only return None if we have no data at all
                        if not data.get("data"):
                            self.log.error("GraphQL request failed completely - no data returned")
                            return None

                        # Count deprecated field errors separately
                        deprecated_errors = [e for e in errors if "deprecated" in e.get("message", "").lower()]
                        if deprecated_errors:
                            self.log.debug("Ignoring %d deprecated field errors", len(deprecated_errors))

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
    # Optimized parallel fetching methods for GraphQL
    # ----------------------
    async def _fetch_issues_page(self, guild: discord.Guild, owner: str, repo: str, cursor: Optional[str]) -> Optional[Dict[str, Any]]:
        """Fetch a single page of issues using the optimized issues-only query."""
        try:
            variables = {
                "owner": owner,
                "repo": repo,
                "issuesCursor": cursor
            }

            data = await self._graphql_request(guild, ISSUES_ONLY_QUERY, variables)
            if not data or not data.get("repository"):
                return None

            repo_data = data["repository"]
            issues = repo_data.get("issues", {})
            issue_nodes = issues.get("nodes", [])

            return {
                "issues": issue_nodes,
                "hasNext": issues.get("pageInfo", {}).get("hasNextPage", False),
                "cursor": issues.get("pageInfo", {}).get("endCursor"),
                "rate_limit": data.get("rateLimit", {})
            }
        except Exception:
            self.log.exception("Failed to fetch issues page")
            return None

    async def _fetch_prs_page(self, guild: discord.Guild, owner: str, repo: str, cursor: Optional[str]) -> Optional[Dict[str, Any]]:
        """Fetch a single page of PRs using the optimized PRs-only query."""
        try:
            variables = {
                "owner": owner,
                "repo": repo,
                "prsCursor": cursor
            }

            data = await self._graphql_request(guild, PRS_ONLY_QUERY, variables)
            if not data or not data.get("repository"):
                return None

            repo_data = data["repository"]
            prs = repo_data.get("pullRequests", {})
            pr_nodes = prs.get("nodes", [])

            return {
                "prs": pr_nodes,
                "hasNext": prs.get("pageInfo", {}).get("hasNextPage", False),
                "cursor": prs.get("pageInfo", {}).get("endCursor"),
                "rate_limit": data.get("rateLimit", {})
            }
        except Exception:
            self.log.exception("Failed to fetch PRs page")
            return None

    async def _fetch_closed_entities_for_cleanup(self, guild: discord.Guild, since_hours: int = 24) -> Optional[Dict[str, Any]]:
        """Fetch recently closed/merged issues and PRs for cleanup purposes."""
        try:
            owner = await self.config.guild(guild).github_owner()
            repo_name = await self.config.guild(guild).github_repo()
            if not owner or not repo_name:
                return None

            # Calculate since timestamp (look back specified hours)
            import datetime
            since_dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=since_hours)
            since_iso = since_dt.isoformat()

            variables = {
                "owner": owner,
                "repo": repo_name,
                "since": since_iso
            }

            data = await self._graphql_request(guild, CLEANUP_QUERY, variables)
            if not data or not data.get("repository"):
                return None

            repo_data = data["repository"]
            closed_issues = repo_data.get("issues", {}).get("nodes", [])
            all_closed_prs = repo_data.get("pullRequests", {}).get("nodes", [])
            
            # Manually filter PRs by the since timestamp since filterBy is not supported
            closed_prs = []
            for pr in all_closed_prs:
                # Check if the PR was updated since our cutoff time
                updated_at = pr.get("updatedAt")
                if updated_at and updated_at >= since_iso:
                    closed_prs.append(pr)

            return {
                "closed_issues": closed_issues,
                "closed_prs": closed_prs,
                "rate_limit": data.get("rateLimit", {})
            }

        except Exception:
            self.log.exception("Failed to fetch closed entities for cleanup")
            return None

    async def _cleanup_closed_discord_threads(self, guild: discord.Guild, closed_data: Dict[str, Any]) -> int:
        """Delete Discord threads for closed/merged GitHub issues and PRs."""
        deleted_count = 0
        
        try:
            # Process closed issues
            for issue in closed_data.get("closed_issues", []):
                number = issue.get("number")
                if not number:
                    continue
                    
                thread = await self._get_thread_by_number(guild, number=number, kind="issues")
                if thread:
                    try:
                        await thread.delete()
                        self.log.debug("Deleted Discord thread for closed issue #%s", number)
                        deleted_count += 1
                        
                        # Clean up stored data
                        await self._cleanup_entity_data(guild, number, "issues")
                        
                    except discord.NotFound:
                        # Thread already deleted
                        await self._cleanup_entity_data(guild, number, "issues")
                    except Exception:
                        self.log.exception("Failed to delete thread for closed issue #%s", number)

            # Process closed/merged PRs
            for pr in closed_data.get("closed_prs", []):
                number = pr.get("number")
                if not number:
                    continue
                    
                thread = await self._get_thread_by_number(guild, number=number, kind="prs")
                if thread:
                    try:
                        await thread.delete()
                        self.log.debug("Deleted Discord thread for closed/merged PR #%s", number)
                        deleted_count += 1
                        
                        # Clean up stored data
                        await self._cleanup_entity_data(guild, number, "prs")
                        
                    except discord.NotFound:
                        # Thread already deleted
                        await self._cleanup_entity_data(guild, number, "prs")
                    except Exception:
                        self.log.exception("Failed to delete thread for closed/merged PR #%s", number)

        except Exception:
            self.log.exception("Failed to cleanup closed Discord threads")
            
        return deleted_count

    async def _cleanup_entity_data(self, guild: discord.Guild, number: int, kind: str) -> None:
        """Clean up all stored data for a closed/deleted entity."""
        try:
            # Remove from links_by_number
            try:
                await self.config.custom(kind, guild.id).clear_raw("links_by_number", str(number))
            except Exception:
                pass
                
            # Remove from content_hashes
            try:
                content_hashes = await self.config.custom("content_hashes", guild.id).get_raw(kind, default={})
                if str(number) in content_hashes:
                    del content_hashes[str(number)]
                    await self.config.custom("content_hashes", guild.id).set_raw(kind, value=content_hashes)
            except Exception:
                pass
                
            # Remove from state_hashes
            try:
                state_hashes = await self.config.custom("state_hashes", guild.id).get_raw(kind, default={})
                if str(number) in state_hashes:
                    del state_hashes[str(number)]
                    await self.config.custom("state_hashes", guild.id).set_raw(kind, value=state_hashes)
            except Exception:
                pass
                
            # Remove from discord_messages
            try:
                discord_messages = await self.config.custom("discord_messages", guild.id).get_raw(kind, default={})
                if str(number) in discord_messages:
                    del discord_messages[str(number)]
                    await self.config.custom("discord_messages", guild.id).set_raw(kind, value=discord_messages)
            except Exception:
                pass
                
            # Remove from content_origins
            try:
                content_origins = await self.config.custom("content_origins", guild.id).get_raw(kind, default={})
                if str(number) in content_origins:
                    del content_origins[str(number)]
                    await self.config.custom("content_origins", guild.id).set_raw(kind, value=content_origins)
            except Exception:
                pass
                
            self.log.debug("Cleaned up stored data for %s #%s", kind, number)
            
        except Exception:
            self.log.exception("Failed to cleanup entity data for %s #%s", kind, number)

    async def _cleanup_orphaned_discord_threads(self, guild: discord.Guild, current_snapshot: Dict[str, Any]) -> int:
        """Clean up Discord threads for issues/PRs that are no longer open in GitHub."""
        deleted_count = 0
        
        try:
            # Get current open issues/PRs from snapshot
            open_issue_numbers = set(current_snapshot.get("issues", {}).keys())
            open_pr_numbers = set(current_snapshot.get("prs", {}).keys())
            
            # Check issues forum for orphaned threads
            issues_forum_id = await self.config.custom("issues", guild.id).forum_channel()
            if issues_forum_id:
                issues_forum = guild.get_channel(issues_forum_id)
                if isinstance(issues_forum, discord.ForumChannel):
                    deleted_count += await self._cleanup_forum_orphaned_threads(
                        guild, issues_forum, open_issue_numbers, "issues"
                    )
            
            # Check PRs forum for orphaned threads
            prs_forum_id = await self.config.custom("prs", guild.id).forum_channel()
            if prs_forum_id:
                prs_forum = guild.get_channel(prs_forum_id)
                if isinstance(prs_forum, discord.ForumChannel):
                    deleted_count += await self._cleanup_forum_orphaned_threads(
                        guild, prs_forum, open_pr_numbers, "prs"
                    )
                    
        except Exception:
            self.log.exception("Failed to cleanup orphaned Discord threads")
            
        return deleted_count

    async def _cleanup_forum_orphaned_threads(self, guild: discord.Guild, forum: discord.ForumChannel, open_numbers: set, kind: str) -> int:
        """Clean up orphaned threads in a specific forum."""
        deleted_count = 0
        
        try:
            # Check stored thread links to find threads that should exist
            links_by_number = await self.config.custom(kind, guild.id).get_raw("links_by_number", default={})
            
            for number_str, thread_id_str in links_by_number.items():
                number = int(number_str)
                
                # If this issue/PR is no longer open, delete the Discord thread
                if str(number) not in open_numbers:
                    try:
                        # Get the thread, either from cache or by fetching
                        thread = None
                        cached_channel = guild.get_channel(int(thread_id_str))
                        if isinstance(cached_channel, discord.Thread):
                            thread = cached_channel
                        elif not cached_channel:
                            # Try to fetch if not in cache
                            try:
                                fetched = await guild.fetch_channel(int(thread_id_str))
                                if isinstance(fetched, discord.Thread):
                                    thread = fetched
                                else:
                                    # Not a thread, clean up data
                                    await self._cleanup_entity_data(guild, number, kind)
                                    continue
                            except discord.NotFound:
                                # Thread already deleted, just clean up data
                                await self._cleanup_entity_data(guild, number, kind)
                                continue
                        else:
                            # Channel exists but is not a thread, clean up data
                            await self._cleanup_entity_data(guild, number, kind)
                            continue
                                
                        if thread:
                            await thread.delete()
                            self.log.debug("Deleted orphaned Discord thread for closed %s #%s", kind[:-1], number)
                            deleted_count += 1
                            
                            # Clean up stored data
                            await self._cleanup_entity_data(guild, number, kind)
                            
                    except discord.NotFound:
                        # Thread already deleted, just clean up data
                        await self._cleanup_entity_data(guild, number, kind)
                    except Exception:
                        self.log.exception("Failed to delete orphaned thread for %s #%s", kind, number)
                        
        except Exception:
            self.log.exception("Failed to cleanup orphaned threads in %s forum", kind)
            
        return deleted_count

    async def _cleanup_thread_tracking(self, thread_id: int) -> None:
        """Clean up thread tracking after a delay to prevent memory leaks."""
        try:
            # Wait 30 seconds for on_thread_create to naturally remove it
            await asyncio.sleep(30)
            # Remove from tracking if still present (shouldn't be, but safety measure)
            self._bot_creating_threads.discard(thread_id)
            self.log.debug("Cleaned up thread tracking for %s (safety cleanup)", thread_id)
        except Exception:
            self.log.exception("Failed to clean up thread tracking for %s", thread_id)

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
        # Since we only work with open entities, always return green for active content
        state = data.get("state", "").lower()
        if state == "open":
            return discord.Color.green()
        else:
            # Fallback for any edge cases
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
        self.log.debug("Repo configured to %s/%s (guild=%s)", owner, repo, getattr(ctx.guild, 'id', None))
        await ctx.send(f"✅ Repository set to `{owner}/{repo}`.")

    @ghsyncset.command(name="issues_forum")
    async def ghsyncset_issues_forum(self, ctx: commands.Context, channel: discord.ForumChannel) -> None:
        """Set the forum channel to mirror GitHub Issues."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return
        await self.config.custom("issues", ctx.guild.id).forum_channel.set(channel.id)
        self.log.debug("Issues forum set: %s (%s) guild=%s", channel.name, channel.id, ctx.guild.id)
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
        self.log.debug("PRs forum set: %s (%s) guild=%s", channel.name, channel.id, ctx.guild.id)
        await ctx.send(f"✅ PRs forum set to {channel.mention}.")
        # Ensure status tags exist and reconcile tag/label sets immediately
        await self._ensure_status_tags_exist(channel, "prs")
        repo = await self._get_repo(ctx.guild)
        if repo:
            await self._reconcile_forum_and_labels(ctx.guild, channel, repo)

    @ghsyncset.command(name="show")
    async def ghsyncset_show(self, ctx: commands.Context, detailed: bool = False) -> None:
        """
        Show current GitHub Sync configuration.

        Args:
            detailed: If True, show detailed snapshot information and performance metrics
        """
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
            
            # Add enhanced task information
            next_iteration = self.github_poll_task.next_iteration
            if next_iteration:
                embed.add_field(name="Next Run", value=f"<t:{int(next_iteration.timestamp())}:R>", inline=True)
            
            # Count enabled guilds
            enabled_count = 0
            for guild in self.bot.guilds:
                try:
                    if await self.config.guild(guild).poll_enabled():
                        enabled_count += 1
                except Exception:
                    continue
            embed.add_field(name="Enabled Guilds", value=str(enabled_count), inline=True)

        # Add forum tags status information
        issues_forum_obj = ctx.guild.get_channel(issues_forum) if issues_forum else None
        prs_forum_obj = ctx.guild.get_channel(prs_forum) if prs_forum else None
        
        for forum, kind, forum_name in [(issues_forum_obj, "Issues", "Issues Forum"), (prs_forum_obj, "PRs", "PRs Forum")]:
            if not isinstance(forum, discord.ForumChannel):
                embed.add_field(name=f"{forum_name} Tags", value="Not configured", inline=False)
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

            embed.add_field(name=f"{forum_name} Tags ({forum.name})", value=value, inline=False)

        embed.set_footer(text="Required status tags: Issues (open) | PRs (open)")

        # Add enhanced snapshot information if detailed=True
        if detailed:
            try:
                snapshot = await self.config.custom("state", ctx.guild.id).get_raw("snapshot", default=None)
                if snapshot:
                    metadata = snapshot.get("fetch_metadata", {})

                    # Calculate time since last fetch
                    import time
                    last_fetch = metadata.get("timestamp", 0)
                    if last_fetch:
                        age_seconds = int(time.time() - last_fetch)
                        age_str = f"<t:{int(last_fetch)}:R>"
                    else:
                        age_str = "Never"

                    snapshot_info = (
                        f"**Last Fetch:** {age_str}\n"
                        f"**Issues:** {len(snapshot.get('issues', {}))}\n"
                        f"**PRs:** {len(snapshot.get('prs', {}))}\n"
                        f"**Labels:** {len(snapshot.get('labels', {}))}\n"
                        f"**Comments:** {metadata.get('comments_fetched', 0)}\n"
                        f"**API Calls:** {metadata.get('total_api_calls', 0)}\n"
                        f"**Fetch Time:** {metadata.get('fetch_duration', 0):.2f}s"
                    )

                    if metadata.get("truncated_entities"):
                        snapshot_info += f"\n**⚠️ Truncated:** {len(metadata['truncated_entities'])} entities"

                    embed.add_field(name="📊 Snapshot Status", value=snapshot_info, inline=True)

                    # Performance info
                    repo_info = snapshot.get("repo_info", {})
                    if repo_info:
                        perf_info = (
                            f"**Repo:** {repo_info.get('name_with_owner', 'Unknown')}\n"
                            f"**Owner:** {repo_info.get('owner_type', 'Unknown')} ({repo_info.get('owner_login', 'Unknown')})\n"
                            f"**Assignees:** {metadata.get('assignees_count', 0)}\n"
                            f"**Milestones:** {metadata.get('milestones_count', 0)}"
                        )
                        embed.add_field(name="📈 Repository Data", value=perf_info, inline=True)
                else:
                    embed.add_field(name="📊 Snapshot Status", value="No snapshot available", inline=True)
            except Exception:
                embed.add_field(name="📊 Snapshot Status", value="Error reading snapshot", inline=True)

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
        self.log.debug("Manual syncall triggered (guild=%s)", ctx.guild.id)
        try:
            # Force sync even if polling is disabled
            await self._sync_guild(ctx.guild, force=True)
            await ctx.tick()
        except Exception:
            self.log.exception("syncall failed (guild=%s)", ctx.guild.id)
            await ctx.send("Sync failed. Check logs.")









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
        - full: Clear everything (snapshot + content hashes + state hashes + message tracking)
        - state: Clear state hashes only (forces state updates on next sync)

        Warning: This will cause content to be re-processed as "new" during the next sync.
        """
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        # Validate target
        valid_targets = ["snapshot", "comments", "state", "full"]
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

        if target == "state":
            state_hashes = await self.config.custom("state_hashes", ctx.guild.id).all()
            if not state_hashes:
                await ctx.send("❌ No state hashes found to clear.")
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
        elif target == "state":
            # Count state hashes
            state_hashes = await self.config.custom("state_hashes", ctx.guild.id).all()
            total_state_hashes = sum(len(kind_data) for kind_data in state_hashes.values() if isinstance(kind_data, dict))

            clear_type = "State Hashes Only"
            description = (
                "This will clear stored state tracking data:\n"
                f"• **{total_state_hashes}** state hashes across all issues/PRs\n"
                "\n**Effects:**\n"
                "• All GitHub entities will have their status re-checked on next sync\n"
                "• Useful for fixing Step 5 false positive updates\n"
                "• State hashes will be rebuilt automatically during next sync\n\n"
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
                "• **State hashes** (status change detection data)\n"
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
                self.log.debug("Cleared GitHub snapshot for guild %s (contained %d issues, %d PRs, %d labels)",
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
                self.log.debug("Cleared %d comment hashes for guild %s", total_comment_hashes, ctx.guild.id)

            elif target == "state":
                # Get count before clearing
                state_hashes = await self.config.custom("state_hashes", ctx.guild.id).all()
                total_state_hashes = sum(len(kind_data) for kind_data in state_hashes.values() if isinstance(kind_data, dict))

                # Clear state hashes
                await self.config.custom("state_hashes", ctx.guild.id).clear()
                cleared_items = ["state hashes"]
                self.log.debug("Cleared %d state hashes for guild %s", total_state_hashes, ctx.guild.id)

            else:  # full
                await self.config.custom("state", ctx.guild.id).clear()
                await self.config.custom("content_hashes", ctx.guild.id).clear()
                await self.config.custom("state_hashes", ctx.guild.id).clear()
                await self.config.custom("discord_messages", ctx.guild.id).clear()
                await self.config.custom("content_origins", ctx.guild.id).clear()
                cleared_items = ["snapshot", "content hashes", "state hashes", "message tracking", "origin tracking"]
                self.log.debug("Cleared all GitHub data for guild %s (contained %d issues, %d PRs, %d labels, %d comment hashes)",
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
            elif target == "state":
                # Get the count that was cleared
                success_description = (
                    f"Successfully cleared state tracking data:\n"
                    f"• **{total_state_hashes}** state hashes\n"
                    "\n**Next Steps:**\n"
                    "• The next polling cycle will re-check all entity states\n"
                    "• Use `[p]ghsyncset syncall` to manually trigger state sync\n"
                    "• State hashes will be rebuilt automatically during sync\n"
                    "• Use `[p]ghsyncset init_state_hashes` to pre-initialize hashes without updates"
                )
            else:  # full
                success_description = (
                    f"Successfully cleared ALL GitHub data:\n"
                    f"• **{issues_count}** issues\n"
                    f"• **{prs_count}** pull requests\n"
                    f"• **{labels_count}** labels\n"
                    f"• **{total_comment_hashes}** comment hashes\n"
                    "• Content hashes, state hashes, message tracking, origin tracking\n"
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

    @ghsyncset.command(name="test_graphql", hidden=True)
    async def ghsyncset_test_graphql(self, ctx: commands.Context) -> None:
        """Test GraphQL query to debug snapshot issues."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        repo = await self._get_repo(ctx.guild)
        if not repo:
            await ctx.send("❌ Repository not configured.")
            return

        await ctx.send("🧪 Testing GraphQL query...")

        try:
            # Test variables for first page
            owner = await self.config.guild(ctx.guild).github_owner()
            repo_name = await self.config.guild(ctx.guild).github_repo()

            variables = {
                "owner": owner,
                "repo": repo_name,
                "issuesCursor": None,
                "prsCursor": None
            }

            # Make GraphQL request
            data = await self._graphql_request(ctx.guild, REPOSITORY_QUERY, variables)

            if not data:
                await ctx.send("❌ GraphQL request returned no data.")
                return

            repo_data = data.get("repository", {})
            if not repo_data:
                await ctx.send("❌ No repository data in GraphQL response.")
                return

            # Analyze the response
            issues_data = repo_data.get("issues", {})
            prs_data = repo_data.get("pullRequests", {})
            labels_data = repo_data.get("labels", {})

            issues_count = len(issues_data.get("nodes", []))
            prs_count = len(prs_data.get("nodes", []))
            labels_count = len(labels_data.get("nodes", []))

            embed = discord.Embed(
                title="🧪 GraphQL Test Results",
                color=discord.Color.green()
            )

            embed.add_field(
                name="📊 Data Retrieved",
                value=f"**Issues:** {issues_count}\n**PRs:** {prs_count}\n**Labels:** {labels_count}",
                inline=True
            )

            if repo_data.get("owner"):
                owner_info = repo_data["owner"]
                embed.add_field(
                    name="🏢 Repository Info",
                    value=f"**Owner:** {owner_info.get('login', 'Unknown')}\n**Type:** {'Org' if owner_info.get('name') else 'User'}",
                    inline=True
                )

            # Show pagination info
            if issues_data.get("pageInfo", {}).get("hasNextPage"):
                embed.add_field(name="📄 Issues Pagination", value="Has more pages", inline=True)
            if prs_data.get("pageInfo", {}).get("hasNextPage"):
                embed.add_field(name="📄 PRs Pagination", value="Has more pages", inline=True)

            # Sample issue/PR titles
            if issues_count > 0:
                sample_issues = [issue.get("title", "No title")[:50] for issue in issues_data["nodes"][:3]]
                embed.add_field(
                    name="📝 Sample Issues",
                    value="\n".join(f"• {title}" for title in sample_issues),
                    inline=False
                )

            await ctx.send(embed=embed)

        except Exception:
            self.log.exception("GraphQL test failed")
            await ctx.send("❌ GraphQL test failed. Check logs for details.")

    @ghsyncset.command(name="cleanup_closed")
    async def ghsyncset_cleanup_closed(self, ctx: commands.Context, days: int = 7) -> None:
        """
        Clean up Discord threads for closed/merged GitHub issues and PRs.
        
        Args:
            days: How many days back to look for closed issues/PRs (default: 7)
        """
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        repo = await self._get_repo(ctx.guild)
        if not repo:
            await ctx.send("❌ Repository not configured.")
            return

        await ctx.send(f"🔄 Looking for closed issues/PRs in the last {days} days...")

        try:
            # Fetch closed entities
            cleanup_data = await self._fetch_closed_entities_for_cleanup(ctx.guild, since_hours=days * 24)
            if not cleanup_data:
                await ctx.send("❌ Failed to fetch closed entities from GitHub.")
                return

            closed_issues_count = len(cleanup_data.get("closed_issues", []))
            closed_prs_count = len(cleanup_data.get("closed_prs", []))

            if closed_issues_count == 0 and closed_prs_count == 0:
                await ctx.send(f"✅ No closed issues or PRs found in the last {days} days.")
                return

            # Show what will be cleaned up
            embed = discord.Embed(
                title="🧹 Cleanup Preview",
                description=f"Found in the last {days} days:",
                color=discord.Color.orange()
            )
            embed.add_field(name="Closed Issues", value=str(closed_issues_count), inline=True)
            embed.add_field(name="Closed/Merged PRs", value=str(closed_prs_count), inline=True)
            embed.add_field(name="Action", value="Delete corresponding Discord threads", inline=False)
            embed.set_footer(text="React with ✅ to proceed or ❌ to cancel")

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
                    description="Cleanup cancelled due to timeout.",
                    color=discord.Color.red()
                ))
                return

            if str(reaction.emoji) == "❌":
                await msg.edit(embed=discord.Embed(
                    title="❌ Cancelled",
                    description="Cleanup cancelled by user.",
                    color=discord.Color.red()
                ))
                return

            # Proceed with cleanup
            deleted_count = await self._cleanup_closed_discord_threads(ctx.guild, cleanup_data)

            success_embed = discord.Embed(
                title="✅ Cleanup Complete",
                description=f"Successfully deleted {deleted_count} Discord threads for closed GitHub entities.",
                color=discord.Color.green()
            )
            success_embed.add_field(
                name="Summary",
                value=f"• Found {closed_issues_count} closed issues\n• Found {closed_prs_count} closed/merged PRs\n• Deleted {deleted_count} Discord threads",
                inline=False
            )

            await msg.edit(embed=success_embed)

        except Exception:
            self.log.exception("cleanup_closed command failed")
            await ctx.send("❌ Failed to cleanup closed threads. Check logs.")

    @ghsyncset.command(name="test_embed", hidden=True)
    async def ghsyncset_test_embed(self, ctx: commands.Context) -> None:
        """Test embed creation and posting to debug freezing issues."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        await ctx.send("🧪 Testing embed creation and posting...")

        try:
            # Create a test data structure
            test_data = {
                "number": 999,
                "title": "Test Issue for Embed Testing",
                "body": "This is a test issue to debug embed creation and posting performance.",
                "state": "open",
                "url": "https://github.com/test/test/issues/999",
                "user": "TestUser",
                "user_avatar": "https://avatars.githubusercontent.com/u/1?v=4"
            }

            # Test embed creation timing
            import time
            start_time = time.time()
            
            try:
                embed = await asyncio.wait_for(
                    self._create_embed(test_data, "issues"),
                    timeout=5.0
                )
                creation_time = time.time() - start_time
                
                # Test posting the embed to this channel
                post_start = time.time()
                msg = await asyncio.wait_for(
                    ctx.send(embed=embed),
                    timeout=10.0
                )
                post_time = time.time() - post_start
                
                # Success report
                success_embed = discord.Embed(
                    title="✅ Embed Test Successful",
                    color=discord.Color.green()
                )
                success_embed.add_field(
                    name="Performance",
                    value=f"• Embed creation: {creation_time:.2f}s\n• Embed posting: {post_time:.2f}s\n• Total time: {creation_time + post_time:.2f}s",
                    inline=False
                )
                success_embed.add_field(
                    name="Status", 
                    value="All embed operations completed successfully", 
                    inline=False
                )
                
                await ctx.send(embed=success_embed)
                
            except asyncio.TimeoutError:
                await ctx.send("❌ **TIMEOUT DETECTED**: Embed creation or posting took too long (>15s total)")
            except Exception as e:
                await ctx.send(f"❌ **ERROR DETECTED**: {str(e)}")

        except Exception:
            self.log.exception("test_embed command failed")
            await ctx.send("❌ Failed to test embeds. Check logs.")

    @ghsyncset.command(name="force_cleanup", hidden=True)
    async def ghsyncset_force_cleanup(self, ctx: commands.Context) -> None:
        """Force cleanup of all orphaned Discord threads (debug command)."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        repo = await self._get_repo(ctx.guild)
        if not repo:
            await ctx.send("❌ Repository not configured.")
            return

        await ctx.send("🧹 Force cleaning up orphaned Discord threads...")

        try:
            # Build current snapshot to see what should exist
            snapshot = await self._build_github_snapshot(ctx.guild)
            
            # Run cleanup phases
            cleanup_data = await self._fetch_closed_entities_for_cleanup(ctx.guild, since_hours=168)
            recent_deleted = 0
            if cleanup_data:
                recent_deleted = await self._cleanup_closed_discord_threads(ctx.guild, cleanup_data)
            
            orphaned_deleted = await self._cleanup_orphaned_discord_threads(ctx.guild, snapshot)
            
            total_deleted = recent_deleted + orphaned_deleted
            
            embed = discord.Embed(
                title="🧹 Cleanup Complete",
                color=discord.Color.green() if total_deleted > 0 else discord.Color.blue()
            )
            embed.add_field(
                name="Results",
                value=f"• Recently closed: {recent_deleted} threads deleted\n• Orphaned threads: {orphaned_deleted} threads deleted\n• **Total deleted:** {total_deleted} threads",
                inline=False
            )
            embed.add_field(
                name="Current State",
                value=f"• Open issues: {len(snapshot.get('issues', {}))}\n• Open PRs: {len(snapshot.get('prs', {}))}",
                inline=False
            )
            
            if total_deleted == 0:
                embed.add_field(name="Status", value="✅ No orphaned threads found", inline=False)
            else:
                embed.add_field(name="Status", value=f"✅ Cleaned up {total_deleted} orphaned threads", inline=False)
            
            await ctx.send(embed=embed)
            
        except Exception:
            self.log.exception("force_cleanup command failed")
            await ctx.send("❌ Failed to force cleanup. Check logs.")

    @ghsyncset.command(name="test_cleanup_query", hidden=True)
    async def ghsyncset_test_cleanup_query(self, ctx: commands.Context, hours: int = 24) -> None:
        """Test the cleanup GraphQL query to debug issues."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return
            
        owner = await self.config.guild(ctx.guild).github_owner()
        repo_name = await self.config.guild(ctx.guild).github_repo()
        if not owner or not repo_name:
            await ctx.send("❌ Repository not configured.")
            return

        await ctx.send(f"🧪 Testing cleanup query for last {hours} hours...")
        
        try:
            # Test the cleanup query
            cleanup_data = await self._fetch_closed_entities_for_cleanup(ctx.guild, since_hours=hours)
            
            if cleanup_data:
                closed_issues = cleanup_data.get("closed_issues", [])
                closed_prs = cleanup_data.get("closed_prs", [])
                rate_limit = cleanup_data.get("rate_limit", {})
                
                result = [
                    f"**Cleanup Query Results (last {hours}h):**",
                    f"• Closed issues found: {len(closed_issues)}",
                    f"• Closed/merged PRs found: {len(closed_prs)}",
                    f"• Rate limit remaining: {rate_limit.get('remaining', 'unknown')}"
                ]
                
                if closed_issues:
                    issue_numbers = ["#" + str(issue["number"]) for issue in closed_issues[:5]]
                    result.append(f"**Recent closed issues:** {', '.join(issue_numbers)}")
                if closed_prs:
                    pr_numbers = ["#" + str(pr["number"]) for pr in closed_prs[:5]]
                    result.append(f"**Recent closed/merged PRs:** {', '.join(pr_numbers)}")
                    
                await ctx.send("\n".join(result))
            else:
                await ctx.send("❌ No cleanup data returned - check configuration and logs.")
                
        except Exception:
            self.log.exception("test_cleanup_query command failed")
            await ctx.send("❌ Failed to test cleanup query. Check logs.")

    @ghsyncset.command(name="fix_orphaned_threads", hidden=True)
    async def ghsyncset_fix_orphaned_threads(self, ctx: commands.Context) -> None:
        """Find and restore mappings for orphaned Discord threads."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        await ctx.send("🔍 Searching for orphaned Discord threads...")
        
        try:
            issues_forum_id = await self.config.custom("issues", ctx.guild.id).forum_channel()
            prs_forum_id = await self.config.custom("prs", ctx.guild.id).forum_channel()
            
            restored_count = 0
            
            # Check issues forum
            if issues_forum_id:
                issues_forum = ctx.guild.get_channel(issues_forum_id)
                if isinstance(issues_forum, discord.ForumChannel):
                    for thread in issues_forum.threads:
                        # Look for threads with format "#123: Title"
                        if thread.name.startswith("#") and ":" in thread.name:
                            try:
                                number_part = thread.name.split(":")[0][1:]  # Remove # and get number
                                number = int(number_part)
                                
                                # Check if mapping exists
                                existing_id = await self._get_thread_id_by_number(ctx.guild, number=number, kind="issues")
                                if not existing_id:
                                    # Restore mapping and tracking
                                    await self._store_link_by_number(thread, number, kind="issues")
                                    await self._restore_orphaned_thread_tracking(ctx.guild, thread, number, "issues")
                                    restored_count += 1
                                    self.log.debug("Restored mapping for issues thread %s (#%s)", thread.id, number)
                            except (ValueError, IndexError):
                                continue  # Skip threads that don't match expected format
            
            # Check PRs forum
            if prs_forum_id:
                prs_forum = ctx.guild.get_channel(prs_forum_id)
                if isinstance(prs_forum, discord.ForumChannel):
                    for thread in prs_forum.threads:
                        # Look for threads with format "#123: Title"
                        if thread.name.startswith("#") and ":" in thread.name:
                            try:
                                number_part = thread.name.split(":")[0][1:]  # Remove # and get number
                                number = int(number_part)
                                
                                # Check if mapping exists
                                existing_id = await self._get_thread_id_by_number(ctx.guild, number=number, kind="prs")
                                if not existing_id:
                                    # Restore mapping and tracking
                                    await self._store_link_by_number(thread, number, kind="prs")
                                    await self._restore_orphaned_thread_tracking(ctx.guild, thread, number, "prs")
                                    restored_count += 1
                                    self.log.debug("Restored mapping for prs thread %s (#%s)", thread.id, number)
                            except (ValueError, IndexError):
                                continue  # Skip threads that don't match expected format
            
            if restored_count > 0:
                await ctx.send(f"✅ Restored {restored_count} orphaned thread mappings.")
            else:
                await ctx.send("ℹ️ No orphaned threads found.")
                
        except Exception:
            self.log.exception("fix_orphaned_threads command failed")
            await ctx.send("❌ Failed to fix orphaned threads. Check logs.")

    @ghsyncset.command(name="test_post_creation", hidden=True)
    async def ghsyncset_test_post_creation(self, ctx: commands.Context, issue_number: int) -> None:
        """Test creating a single forum post for debugging hanging issues."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        repo = await self._get_repo(ctx.guild)
        if not repo:
            await ctx.send("❌ Repository not configured.")
            return

        await ctx.send(f"🧪 Testing creation of forum post for issue #{issue_number}...")
        
        try:
            # Get the GitHub issue data
            github_issue = await asyncio.to_thread(lambda: repo.get_issue(issue_number))
            
            # Build minimal data structure
            test_data = {
                "number": github_issue.number,
                "title": github_issue.title,
                "body": github_issue.body or "",
                "state": github_issue.state,
                "url": github_issue.html_url,
                "user": github_issue.user.login if github_issue.user else "unknown",
                "user_avatar": github_issue.user.avatar_url if github_issue.user else "",
                "created_at": github_issue.created_at.isoformat() if github_issue.created_at else "",
                "updated_at": github_issue.updated_at.isoformat() if github_issue.updated_at else "",
                "labels": [{"name": label.name} for label in github_issue.labels],
                "comments": []
            }
            
            # Get the issues forum
            issues_forum_id = await self.config.custom("issues", ctx.guild.id).forum_channel()
            if not issues_forum_id:
                await ctx.send("❌ Issues forum not configured.")
                return
                
            issues_forum = ctx.guild.get_channel(issues_forum_id)
            if not isinstance(issues_forum, discord.ForumChannel):
                await ctx.send("❌ Issues forum not found or not a forum channel.")
                return
            
            # Test forum post creation
            success = await self._create_forum_post(ctx.guild, issues_forum, test_data, "issues")
            
            if success:
                await ctx.send(f"✅ Successfully created forum post for issue #{issue_number}")
            else:
                await ctx.send(f"❌ Failed to create forum post for issue #{issue_number}")
                
        except Exception:
            self.log.exception("test_post_creation command failed")
            await ctx.send("❌ Failed to test post creation. Check logs.")

    @ghsyncset.command(name="concurrency", hidden=True)
    async def ghsyncset_concurrency(self, ctx: commands.Context, discord_limit: Optional[int] = None, github_limit: Optional[int] = None) -> None:
        """View or configure concurrency limits for parallel operations."""
        if discord_limit is None and github_limit is None:
            await ctx.send(
                f"**Current Concurrency Limits:**\n"
                f"• Discord operations: {self._discord_semaphore._value} concurrent\n"
                f"• GitHub operations: {self._github_semaphore._value} concurrent\n\n"
                f"Usage: `{ctx.prefix}ghsyncset concurrency <discord_limit> [github_limit]`"
            )
            return

        try:
            if discord_limit is not None:
                if not (1 <= discord_limit <= 10):
                    await ctx.send("❌ Discord limit must be between 1 and 10")
                    return
                self._discord_semaphore = asyncio.Semaphore(discord_limit)
                
            if github_limit is not None:
                if not (1 <= github_limit <= 5):
                    await ctx.send("❌ GitHub limit must be between 1 and 5")
                    return
                self._github_semaphore = asyncio.Semaphore(github_limit)
            
            await ctx.send(
                f"✅ **Updated Concurrency Limits:**\n"
                f"• Discord operations: {self._discord_semaphore._value} concurrent\n"
                f"• GitHub operations: {self._github_semaphore._value} concurrent"
            )
        except Exception:
            self.log.exception("concurrency command failed")
            await ctx.send("❌ Failed to update concurrency limits. Check logs.")

    @ghsyncset.command(name="test_review_comments", hidden=True)
    async def ghsyncset_test_review_comments(self, ctx: commands.Context, pr_number: int) -> None:
        """Test fetching review comments for a specific PR."""
        if not ctx.guild:
            await ctx.send("Run this in a guild.")
            return

        repo = await self._get_repo(ctx.guild)
        if not repo:
            await ctx.send("❌ Repository not configured.")
            return

        await ctx.send(f"🧪 Testing review comment detection for PR #{pr_number}...")
        
        try:
            # Get GitHub token
            token = await self.config.guild(ctx.guild).github_token()
            owner = await self.config.guild(ctx.guild).github_owner()
            repo_name = await self.config.guild(ctx.guild).github_repo()
            
            if not all([token, owner, repo_name]):
                await ctx.send("❌ GitHub configuration incomplete.")
                return

            # Test GraphQL query for this specific PR
            query = """
            query($owner: String!, $repo: String!, $prNumber: Int!) {
              repository(owner: $owner, name: $repo) {
                pullRequest(number: $prNumber) {
                  number
                  title
                  comments(first: 10) {
                    totalCount
                    nodes {
                      id
                      body
                      author { login }
                      createdAt
                    }
                  }
                  reviews(first: 10) {
                    totalCount
                    nodes {
                      id
                      state
                      body
                      author { login }
                      createdAt
                      comments(first: 10) {
                        totalCount
                        nodes {
                          id
                          body
                          path
                          line
                          position
                          author { login }
                          createdAt
                        }
                      }
                    }
                  }
                }
              }
            }
            """
            
            variables = {
                "owner": owner,
                "repo": repo_name,
                "prNumber": pr_number
            }
            
            data = await self._graphql_request(ctx.guild, query, variables)
            
            if not data or not data.get("repository", {}).get("pullRequest"):
                await ctx.send(f"❌ PR #{pr_number} not found.")
                return
                
            pr_data = data["repository"]["pullRequest"]
            regular_comments = pr_data.get("comments", {})
            reviews = pr_data.get("reviews", {})
            
            # Count review comments from all reviews
            total_review_comments = 0
            total_review_summaries = 0
            for review in reviews.get("nodes", []):
                if review.get("body", "").strip():  # Review has summary text
                    total_review_summaries += 1
                total_review_comments += review.get("comments", {}).get("totalCount", 0)
            
            result = [
                f"**PR #{pr_number}: {pr_data.get('title', 'Unknown Title')}**",
                f"• Regular comments: {regular_comments.get('totalCount', 0)}",
                f"• Reviews with summaries: {total_review_summaries}",
                f"• Review comments (line-by-line): {total_review_comments}",
                f"• **Total comments: {regular_comments.get('totalCount', 0) + total_review_summaries + total_review_comments}**"
            ]
            
            # Show sample of each type
            if regular_comments.get("nodes"):
                result.append("\n**Sample Regular Comments:**")
                for comment in regular_comments["nodes"][:3]:
                    author = comment.get("author", {}).get("login", "Unknown")
                    body = comment.get("body", "")[:100] + "..." if len(comment.get("body", "")) > 100 else comment.get("body", "")
                    result.append(f"• {author}: {body}")
                    
            if reviews.get("nodes"):
                result.append("\n**Sample Reviews:**")
                for review in reviews["nodes"][:2]:  # Show fewer due to nested structure
                    author = review.get("author", {}).get("login", "Unknown")
                    state = review.get("state", "").lower()
                    
                    # Show review summary if present
                    if review.get("body", "").strip():
                        body = review.get("body", "")[:80] + "..." if len(review.get("body", "")) > 80 else review.get("body", "")
                        result.append(f"• {author} ({state}): {body}")
                    
                    # Show sample review comments
                    review_comments = review.get("comments", {}).get("nodes", [])
                    for comment in review_comments[:2]:  # Max 2 per review
                        comment_author = comment.get("author", {}).get("login", "Unknown")
                        path = comment.get("path", "")
                        line = comment.get("line", comment.get("position", ""))
                        comment_body = comment.get("body", "")[:80] + "..." if len(comment.get("body", "")) > 80 else comment.get("body", "")
                        result.append(f"  ↳ {comment_author} on `{path}:{line}`: {comment_body}")
            
            await ctx.send("\n".join(result))
                
        except Exception:
            self.log.exception("test_review_comments command failed")
            await ctx.send("❌ Failed to test review comments. Check logs.")



    # ----------------------
    # Discord -> GitHub: listeners
    # ----------------------
    @commands.Cog.listener()
    async def on_thread_create(self, thread: discord.Thread) -> None:
        guild = thread.guild
        if not guild:
            return

        # Skip if this thread is being created by the bot during sync to prevent feedback loops
        if thread.id in self._bot_creating_threads:
            self.log.debug("Skipping thread_create for thread %s (bot-created during sync)", thread.id)
            # Remove from tracking set since creation is complete
            self._bot_creating_threads.discard(thread.id)
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
                self.log.debug("Auto-link thread %s -> %s #%s", thread.id, kind, number)
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
    async def on_thread_delete(self, thread: discord.Thread) -> None:
        """Handle Discord thread deletion and close corresponding GitHub issue/PR."""
        try:
            guild = thread.guild
            if not guild:
                return

            # Check if Discord → GitHub sync is enabled
            if not await self._is_discord_to_github_enabled(guild):
                self.log.debug("Discord → GitHub sync disabled, skipping thread deletion for guild %s", guild.id)
                return

            # Check if this is an issues or PRs forum thread
            issues_forum = await self.config.custom("issues", guild.id).forum_channel()
            prs_forum = await self.config.custom("prs", guild.id).forum_channel()

            if thread.parent_id not in {issues_forum, prs_forum}:
                return

            # Get linked GitHub issue/PR
            github_url = await self._get_link(thread)
            if not github_url:
                return

            issue_number = self._extract_issue_number(github_url)
            if not issue_number:
                return

            kind = "issues" if thread.parent_id == issues_forum else "prs"

            # Check origin - only close GitHub if this issue/PR originated from Discord
            origin_data = await self._get_origin(guild, issue_number, kind)
            if not origin_data or origin_data.get("origin") != "discord":
                self.log.debug("Skipping GitHub close for %s #%s - originated from GitHub", kind, issue_number)
                return

            repo = await self._get_repo(guild)
            if not repo:
                return

            # Close the GitHub issue/PR
            if kind == "issues":
                github_issue = await asyncio.to_thread(lambda: repo.get_issue(number=issue_number))
                await asyncio.to_thread(lambda: github_issue.edit(state="closed"))
                self.log.debug("Closed GitHub issue #%s due to Discord thread deletion", issue_number)
            else:
                # Can't close PRs via API, just log it
                self.log.debug("Discord thread for PR #%s was deleted, but PRs cannot be closed via API", issue_number)

            # Clean up stored data
            await self._cleanup_entity_data(guild, issue_number, kind)

        except Exception:
            self.log.exception("Failed to handle thread deletion")

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
                    self.log.debug("Opened GitHub %s #%s from Discord status tag", kind, issue_number)
                elif "closed" in status_tags or "not resolved" in status_tags:
                    github_issue.edit(state="closed")
                    self.log.debug("Closed GitHub %s #%s from Discord status tag", kind, issue_number)
                elif "merged" in status_tags and kind == "prs":
                    # Cannot merge via API, but log the attempt
                    self.log.warning("Cannot merge PR #%s via API - status tag applied in Discord", issue_number)

            # Handle regular labels based on origin
            if origin_data and origin_data.get("origin") == "discord":
                # Discord-originated content: sync labels to GitHub
                github_issue = repo.get_issue(number=issue_number)
                github_issue.set_labels(*regular_labels)
                self.log.debug("Updated GitHub %s #%s labels to match Discord tags (Discord origin)", kind, issue_number)
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
                self.log.debug("User changed thread %s state (archived: %s→%s, locked: %s→%s), syncing to GitHub %s #%s",
                            after.id, before.archived, after.archived, before.locked, after.locked, kind, issue_number)

                github_issue = repo.get_issue(number=issue_number)

                if before.archived != after.archived:
                    new_state = "closed" if after.archived else "open"
                    github_issue.edit(state=new_state)
                    self.log.debug("Updated GitHub %s #%s state to %s", kind, issue_number, new_state)

                if before.locked != after.locked:
                    if after.locked:
                        github_issue.lock("off-topic")  # Use a generic lock reason
                        self.log.debug("Locked GitHub %s #%s", kind, issue_number)
                    else:
                        github_issue.unlock()
                        self.log.debug("Unlocked GitHub %s #%s", kind, issue_number)

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

        self.log.debug("Starting complete 5-step sync for guild %s (force=%s)", guild.id, force)

        try:
            # Start batch config mode to reduce file I/O during sync
            self._start_batch_config_mode()

            # Build fresh snapshot from GitHub using GraphQL (only open issues/PRs)
            snapshot = await self._build_github_snapshot(guild)

            # Load previous snapshot for comparison
            prev = await self.config.custom("state", guild.id).get_raw("snapshot", default=None)

            # CLEANUP PHASE: Check for recently closed/merged issues and clean up Discord threads
            # Look back longer (7 days) to catch issues that might have been missed
            cleanup_data = await self._fetch_closed_entities_for_cleanup(guild, since_hours=168)  # 7 days
            if cleanup_data:
                deleted_count = await self._cleanup_closed_discord_threads(guild, cleanup_data)
                if deleted_count > 0:
                    self.log.debug("Cleanup phase: Deleted %d Discord threads for closed GitHub entities", deleted_count)

            # ADDITIONAL CLEANUP: Check existing Discord threads for closed issues not in recent cleanup
            additional_deleted = await self._cleanup_orphaned_discord_threads(guild, snapshot)
            if additional_deleted > 0:
                self.log.debug("Additional cleanup: Deleted %d orphaned Discord threads", additional_deleted)

            # Perform 5-step reconciliation to Discord (only for open entities)
            await self._reconcile_snapshot_to_discord(guild, repo, prev, snapshot)

            # Save new snapshot as the current state (queued for batch save)
            self._queue_config_update(f"custom.state.snapshot", snapshot)

            # Apply all batched config updates at once
            await self._end_batch_config_mode(guild)

            self.log.debug("Completed 5-step sync for guild %s", guild.id)

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

    async def _find_existing_thread_for_issue(self, guild: discord.Guild, number: int, kind: str) -> Optional[discord.Thread]:
        """Find an existing Discord thread for a GitHub issue/PR, even if mapping was lost."""
        try:
            # First try the stored mapping
            thread = await self._get_thread_by_number(guild, number=number, kind=kind)
            if thread:
                return thread

            # If no mapping found, search the forum physically
            forum_id = await self.config.custom(kind, guild.id).forum_channel()
            if not forum_id:
                return None

            forum = guild.get_channel(forum_id)
            if not isinstance(forum, discord.ForumChannel):
                return None

            # Search forum threads for one that matches this issue number
            # Look for threads with titles starting with "#<number>:"
            prefix = f"#{number}:"
            for thread in forum.threads:
                if thread.name.startswith(prefix):
                    self.log.debug("Found orphaned thread %s for %s #%s - restoring all tracking data", 
                                thread.id, kind, number)
                    # Restore the mapping
                    await self._store_link_by_number(thread, number, kind=kind)
                    
                    # Also restore tracking data to prevent false positives in Steps 3 and 5
                    # This prevents the system from thinking everything changed
                    await self._restore_orphaned_thread_tracking(guild, thread, number, kind)
                    
                    return thread

            return None
        except Exception:
            self.log.exception("Failed to find existing thread for %s #%s", kind, number)
            return None

    async def _restore_orphaned_thread_tracking(self, guild: discord.Guild, thread: discord.Thread, number: int, kind: str) -> None:
        """Restore minimal tracking data for an orphaned thread to prevent false change detection."""
        try:
            # Store basic thread message tracking
            # We don't know the exact message IDs, but we can prevent false positives
            # by storing the thread ID so the system knows a thread exists
            discord_messages = await self.config.custom("discord_messages", guild.id).get_raw(kind, default={})
            if str(number) not in discord_messages:
                discord_messages[str(number)] = {
                    "thread_id": thread.id,
                    "embed_message_id": None,  # Will be set if/when we post new content
                    "comments": {}
                }
                if self._batch_config_mode:
                    self._queue_config_update(f"custom.discord_messages.{kind}", discord_messages)
                else:
                    await self.config.custom("discord_messages", guild.id).set_raw(kind, value=discord_messages)
            
            # Note: We intentionally DON'T restore content_hashes or state_hashes
            # This allows the next sync to properly detect and store current state
            # while preventing duplicate thread creation
            
            self.log.debug("Restored tracking for orphaned thread %s (%s #%s)", thread.id, kind, number)
            
        except Exception:
            self.log.exception("Failed to restore tracking for orphaned thread %s (%s #%s)", thread.id, kind, number)

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

        # Fast basic cleaning - remove null bytes and normalize line endings
        cleaned = text.replace('\x00', '').replace('\r\n', '\n').replace('\r', '\n')
        
        # Quick length check - if too long, truncate early to avoid expensive processing
        if len(cleaned) > 5000:
            cleaned = cleaned[:5000] + "..."
        
        # Basic control character removal (only the most problematic ones)
        cleaned = cleaned.replace('\x08', '').replace('\x0B', '').replace('\x0C', '')
        
        # Simple consecutive newline reduction (max 3 consecutive)
        while '\n\n\n\n' in cleaned:
            cleaned = cleaned.replace('\n\n\n\n', '\n\n\n')
        
        # Strip and ensure not empty
        cleaned = cleaned.strip()
        return cleaned

    def _format_discord_user_for_github(self, author: "discord.User | discord.Member", jump_url: str) -> str:
        """Format Discord user info for GitHub with properly scaled avatar."""
        avatar_url = author.display_avatar.url
        discord_link = f"[Discord]({jump_url})"
        # Use HTML img tag with fixed dimensions for proper scaling in GitHub
        avatar_html = f'<img src="{avatar_url}" alt="{author.display_name}" width="20" height="20" style="border-radius: 50%;">'
        return f"{avatar_html} **{author.display_name}** on {discord_link}"

    async def _create_forum_post_parallel(self, guild: discord.Guild, forum: discord.ForumChannel, data: Dict[str, Any], kind: str) -> bool:
        """Thread-safe wrapper for creating forum posts with semaphore control."""
        async with self._discord_semaphore:
            return await self._create_forum_post(guild, forum, data, kind)

    async def _edit_forum_post_parallel(self, guild: discord.Guild, thread: discord.Thread, data: Dict[str, Any], kind: str) -> bool:
        """Thread-safe wrapper for editing forum posts with semaphore control."""
        async with self._discord_semaphore:
            return await self._edit_forum_post(guild, thread, data, kind)

    async def _post_comment_parallel(self, guild: discord.Guild, thread: discord.Thread, comment: Dict[str, Any], entity_data: Dict[str, Any], kind: str) -> bool:
        """Thread-safe wrapper for posting comments with semaphore control."""
        async with self._discord_semaphore:
            return await self._post_comment_to_thread(guild, thread, comment, entity_data, kind)

    async def _apply_thread_state_parallel(self, thread: discord.Thread, data: Dict[str, Any], desired_tags: List[discord.ForumTag], kind: str) -> None:
        """Thread-safe wrapper for applying thread state with semaphore control."""
        async with self._discord_semaphore:
            return await self._apply_thread_state(thread, data, desired_tags, kind)

    def _format_forum_title(self, data: Dict[str, Any], kind: str) -> str:
        """Format a forum thread title with issue/PR number at the beginning."""
        number = data["number"]
        issue_title = self._clean_discord_text(data.get("title", ""))
        prefix = "Issue" if kind == "issues" else "PR"

        if issue_title:
            # Format: "#123: Title" (truncate title to fit within Discord's 100 char limit)
            number_prefix = f"#{number}: "
            max_title_length = 100 - len(number_prefix)  # Discord's hard limit is 100 chars
            
            if max_title_length <= 0:  # Safety check
                return f"{prefix} #{number}"
                
            truncated_title = issue_title[:max_title_length] if len(issue_title) > max_title_length else issue_title
            final_title = f"{number_prefix}{truncated_title}"
            
            # Final safety check to ensure we don't exceed 100 chars
            if len(final_title) > 100:
                self.log.warning("Title still too long after truncation: %d chars", len(final_title))
                return f"{prefix} #{number}"
                
            return final_title
        else:
            # Fallback if no title
            return f"{prefix} #{number}"

    def _quick_content_diff(self, cur_data: Dict[str, Any], prev_data: Dict[str, Any]) -> bool:
        """Fast pre-screening to check if content might have changed without config lookups."""
        # Compare basic fields that indicate potential changes
        if not prev_data:
            return True  # New entity, definitely changed

        # More precise comparison - check exact fields that matter for content
        content_fields = ["title", "body", "updated_at"]
        for field in content_fields:
            if cur_data.get(field) != prev_data.get(field):
                return True

        return False

    def _quick_comments_diff(self, cur_data: Dict[str, Any], prev_data: Dict[str, Any]) -> bool:
        """Fast pre-screening to check if comments might have changed without config lookups."""
        cur_comments = cur_data.get("comments", [])
        prev_comments = prev_data.get("comments", [])

        # Quick count comparison
        if len(cur_comments) != len(prev_comments):
            return True

        # More precise: check if comment IDs and updated timestamps differ
        if cur_comments and prev_comments:
            # Create mappings of comment ID to updated timestamp
            cur_comment_map = {c.get("id"): c.get("updated_at") for c in cur_comments if c.get("id")}
            prev_comment_map = {c.get("id"): c.get("updated_at") for c in prev_comments if c.get("id")}
            
            # Check if any comments were added, removed, or updated
            if cur_comment_map != prev_comment_map:
                return True

        return False

    def _can_edit_thread(self, thread: discord.Thread, data: Dict[str, Any], kind: str, operation: str = "content") -> bool:
        """Check if a thread can be safely edited based on its current state."""
        # Since we only work with open entities, we can always edit threads
        # If a thread is archived/locked, it means it was closed and should have been deleted
        if thread.archived or thread.locked:
            self.log.warning("Found archived/locked thread for open %s #%s - this thread should have been deleted", 
                           kind, data.get("number"))
        
        return True

    def _needs_state_update(self, thread: discord.Thread, data: Dict[str, Any], desired_tags: List[discord.ForumTag], kind: str) -> bool:
        """Fast check if a thread needs state updates without actually applying them."""
        # Determine desired archived/locked state from GitHub data
        desired_archived = False
        desired_locked = data.get("locked", False)

        if kind == "issues":
            desired_archived = data.get("state") == "closed"
            if desired_archived:
                desired_locked = True
        elif kind == "prs":
            desired_archived = data.get("state") == "closed" or data.get("merged", False)
            if desired_archived:
                desired_locked = True

        # Check if current state differs from desired
        current_tag_ids = {t.id for t in getattr(thread, "applied_tags", [])}
        desired_tag_ids = {t.id for t in desired_tags}

        tags_differ = current_tag_ids != desired_tag_ids
        state_differs = (thread.archived != desired_archived or thread.locked != desired_locked)

        return tags_differ or state_differs

    def _calculate_state_hash(self, data: Dict[str, Any], desired_tags: List[discord.ForumTag], kind: str) -> str:
        """Calculate a hash representing the desired state of an entity."""
        # Determine desired archived/locked state from GitHub data
        desired_archived = False
        desired_locked = data.get("locked", False)

        if kind == "issues":
            desired_archived = data.get("state") == "closed"
            if desired_archived:
                desired_locked = True
        elif kind == "prs":
            desired_archived = data.get("state") == "closed" or data.get("merged", False)
            if desired_archived:
                desired_locked = True

        # Create state representation
        state_data = {
            "state": data.get("state"),
            "locked": data.get("locked", False),
            "merged": data.get("merged", False) if kind == "prs" else False,
            "labels": sorted(data.get("labels", [])),  # Sort for consistent hashing
            "desired_archived": desired_archived,
            "desired_locked": desired_locked,
            "tag_names": sorted([t.name for t in desired_tags])  # Sort for consistent hashing
        }

        # Create hash from state data
        state_str = json.dumps(state_data, sort_keys=True)
        return hashlib.sha256(state_str.encode('utf-8')).hexdigest()[:16]

    async def _get_entities_with_state_changes(self, guild: discord.Guild, forum: discord.ForumChannel, entities: Dict[str, Any], kind: str) -> List[Tuple[str, Dict[str, Any], List[discord.ForumTag]]]:
        """Lightning-fast hash-based identification of entities needing state updates."""
        entities_needing_updates = []

        # Get stored state hashes
        try:
            stored_hashes = await self.config.custom("state_hashes", guild.id).get_raw(kind, default={})
        except Exception:
            stored_hashes = {}

        for number_str, data in entities.items():
            # Pre-compute desired tags (fast, no Discord API calls)
            labels = data.get("labels", [])
            label_tags = self._labels_to_forum_tags(forum, labels)
            status_tags = self._get_status_tags_for_entity(forum, data, kind)
            
            # Combine tags, ensuring status tags have priority and we don't exceed Discord's 5-tag limit
            desired_tags = status_tags  # Status tags first (most important)
            for tag in label_tags:
                if tag.id not in {st.id for st in desired_tags} and len(desired_tags) < 5:
                    desired_tags.append(tag)

            # Calculate current state hash
            current_hash = self._calculate_state_hash(data, desired_tags, kind)
            stored_hash = stored_hashes.get(number_str)

            # If hash differs or doesn't exist, this entity needs updates
            if current_hash != stored_hash:
                entities_needing_updates.append((number_str, data, desired_tags))

        return entities_needing_updates

    async def _store_state_hash(self, guild: discord.Guild, number: int, kind: str, data: Dict[str, Any], desired_tags: List[discord.ForumTag]) -> None:
        """Store the state hash for an entity after successful update."""
        try:
            state_hash = self._calculate_state_hash(data, desired_tags, kind)

            if self._batch_config_mode:
                # Queue for batch update
                config_path = f"custom.state_hashes.{kind}.{number}"
                self._queue_config_update(config_path, state_hash)
            else:
                # Direct update
                await self.config.custom("state_hashes", guild.id).set_raw(kind, str(number), value=state_hash)
        except Exception:
            self.log.exception("Failed to store state hash for %s #%s", kind, number)

    async def _invalidate_state_hash(self, guild: discord.Guild, number: int, kind: str) -> None:
        """Invalidate the stored state hash to force state update on next sync."""
        try:
            if self._batch_config_mode:
                # Queue for batch removal
                config_path = f"custom.state_hashes.{kind}.{number}"
                # Remove from pending updates if it exists
                if config_path in self._pending_config_updates:
                    del self._pending_config_updates[config_path]
            
            # Always remove from stored config to ensure invalidation
            try:
                state_hashes = await self.config.custom("state_hashes", guild.id).get_raw(kind, default={})
                if str(number) in state_hashes:
                    del state_hashes[str(number)]
                    await self.config.custom("state_hashes", guild.id).set_raw(kind, value=state_hashes)
                    self.log.debug("Invalidated state hash for %s #%s (GitHub state changed)", kind, number)
            except Exception:
                pass  # Hash might not exist yet, which is fine
                
        except Exception:
            self.log.exception("Failed to invalidate state hash for %s #%s", kind, number)

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
        """
        Fetch issues, PRs, labels, and comments into a complete, self-contained snapshot using GraphQL.

        This snapshot contains ALL data needed for sync operations, eliminating the need for additional
        GitHub API calls during the 5-step reconciliation process, significantly improving performance.
        """
        snapshot: Dict[str, Any] = {
            "labels": {},           # name -> {id, color, description, ...}
            "labels_by_id": {},     # id -> {name, color, description, ...}
            "issues": {},           # number -> {id, title, state, labels, comments, ...}
            "prs": {},              # number -> {id, title, state, labels, comments, ...}
            "repo_info": {},        # {id, name, owner, description, default_branch, ...}
            "node_ids": {},         # For efficient GraphQL mutations: {issues: {number: id}, prs: {number: id}}
            "assignees": {},        # Aggregated assignee data for rich Discord display
            "milestones": {},       # Milestone data
            "fetch_metadata": {     # Track fetch completeness and performance
                "timestamp": None,
                "total_api_calls": 0,
                "issues_fetched": 0,
                "prs_fetched": 0,
                "comments_fetched": 0,
                "truncated_entities": []  # Track which issues/PRs have >100 comments
            }
        }

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
            labels_data: Dict[str, Dict[str, Any]] = {}
            labels_by_id: Dict[str, Dict[str, Any]] = {}
            repo_info: Dict[str, Any] = {}
            api_calls = 0

            # Import time for metadata
            import time
            start_time = time.time()

            # OPTIMIZED: Parallel fetching strategy for maximum speed
            self.log.debug("🚀 Starting optimized GraphQL fetch (100 issues + 100 PRs per call)")

            # Phase 1: Combined fetching while both have data
            issues_cursor = None
            prs_cursor = None
            issues_done = False
            prs_done = False

            # Keep track of rate limit info
            rate_limit_info = {}

            while not (issues_done and prs_done):
                # Use combined query while both have data
                if not issues_done and not prs_done:
                    variables = {
                        "owner": owner,
                        "repo": repo_name,
                        "issuesCursor": issues_cursor,
                        "prsCursor": prs_cursor
                    }

                    data = await self._graphql_request(guild, REPOSITORY_QUERY, variables)
                    api_calls += 1

                    if not data or not data.get("repository"):
                        self.log.warning("GraphQL request returned no repository data on call %d", api_calls)
                        break

                    repo_data = data["repository"]

                    # Track rate limit info
                    if data.get("rateLimit"):
                        rate_limit_info = data["rateLimit"]
                        self.log.debug("Rate limit: %d/%d remaining (cost: %d)",
                                     rate_limit_info.get("remaining", 0),
                                     rate_limit_info.get("limit", 5000),
                                     rate_limit_info.get("cost", 0))

                    # Get enhanced repo info (only on first request)
                    if not repo_info and repo_data.get("owner"):
                        repo_info = {
                            "id": repo_data.get("id", ""),
                            "name": repo_data.get("name", ""),
                            "name_with_owner": repo_data.get("nameWithOwner", ""),
                            "description": repo_data.get("description", ""),
                            "default_branch": repo_data.get("defaultBranchRef", {}).get("name", "main"),
                            "owner_login": repo_data["owner"].get("login", ""),
                            "owner_avatar": repo_data["owner"].get("avatarUrl", ""),
                            "owner_url": repo_data["owner"].get("url", ""),
                            "owner_name": repo_data["owner"].get("name", ""),
                            "owner_type": "Organization" if "description" in repo_data["owner"] else "User"
                        }

                    # Get enhanced labels data (only on first request)
                    if not labels_data and repo_data.get("labels"):
                        for label in repo_data["labels"]["nodes"]:
                            label_name = label.get("name", "")
                            label_id = label.get("id", "")
                            label_info = {
                                "id": label_id,
                                "name": label_name,
                                "color": label.get("color", ""),
                                "description": label.get("description", ""),
                                "created_at": label.get("createdAt", ""),
                                "updated_at": label.get("updatedAt", "")
                            }
                            labels_data[label_name] = label_info
                            if label_id:
                                labels_by_id[label_id] = label_info

                    # Process issues
                    if repo_data.get("issues"):
                        issues = repo_data["issues"]
                        issue_nodes = issues.get("nodes", [])
                        all_issues.extend(issue_nodes)

                        if issues.get("pageInfo", {}).get("hasNextPage"):
                            issues_cursor = issues["pageInfo"]["endCursor"]
                        else:
                            issues_done = True
                            issues_cursor = None
                    else:
                        issues_done = True

                    # Process PRs
                    if repo_data.get("pullRequests"):
                        prs = repo_data["pullRequests"]
                        pr_nodes = prs.get("nodes", [])
                        all_prs.extend(pr_nodes)

                        if prs.get("pageInfo", {}).get("hasNextPage"):
                            prs_cursor = prs["pageInfo"]["endCursor"]
                        else:
                            prs_done = True
                            prs_cursor = None
                    else:
                        prs_done = True

                    self.log.debug("Combined fetch %d: %d issues (%d total), %d PRs (%d total) | Rate limit: %d remaining",
                                 api_calls, len(issue_nodes), len(all_issues), len(pr_nodes), len(all_prs),
                                 rate_limit_info.get("remaining", 0))

                # Phase 2: Separate parallel fetching for remaining data
                else:
                    # Prepare parallel requests for remaining data
                    parallel_requests = []

                    if not issues_done:
                        parallel_requests.append(
                            self._fetch_issues_page(guild, owner, repo_name, issues_cursor)
                        )

                    if not prs_done:
                        parallel_requests.append(
                            self._fetch_prs_page(guild, owner, repo_name, prs_cursor)
                        )

                    if parallel_requests:
                        # Execute parallel requests
                        results = await asyncio.gather(*parallel_requests, return_exceptions=True)
                        api_calls += len(parallel_requests)

                        for i, result in enumerate(results):
                            if isinstance(result, Exception):
                                self.log.error("Parallel fetch failed: %s", result)
                                continue

                            if result is None:
                                continue

                            # Type guard to ensure result is a dict
                            if not isinstance(result, dict):
                                self.log.error("Unexpected result type in parallel fetch: %s", type(result))
                                continue

                            if "issues" in result:
                                issue_nodes = result.get("issues", [])
                                all_issues.extend(issue_nodes)
                                if result.get("hasNext"):
                                    issues_cursor = result.get("cursor")
                                else:
                                    issues_done = True

                                self.log.debug("Parallel issues fetch: %d issues (%d total)",
                                             len(issue_nodes), len(all_issues))

                            elif "prs" in result:
                                pr_nodes = result.get("prs", [])
                                all_prs.extend(pr_nodes)
                                if result.get("hasNext"):
                                    prs_cursor = result.get("cursor")
                                else:
                                    prs_done = True

                                self.log.debug("Parallel PRs fetch: %d PRs (%d total)",
                                             len(pr_nodes), len(all_prs))
                    else:
                        break

            # Log performance summary
            fetch_duration = time.time() - start_time
            self.log.debug("✅ Optimized fetch complete: %d issues, %d PRs in %.2fs (%d API calls, %.1f items/call)",
                         len(all_issues), len(all_prs), fetch_duration, api_calls,
                         (len(all_issues) + len(all_prs)) / max(api_calls, 1))

            # Store repo info and labels in enhanced format
            snapshot["repo_info"] = repo_info
            snapshot["labels"] = labels_data
            snapshot["labels_by_id"] = labels_by_id

            # Store node IDs for efficient mutations
            snapshot["node_ids"] = {"issues": {}, "prs": {}}

            # Aggregated data stores
            assignees_store = {}
            milestones_store = {}
            total_comments = 0
            truncated_entities = []

            # Process issues with enhanced data
            issue_count = 0
            for issue in all_issues:
                issue_count += 1
                issue_number = issue["number"]

                # Store GraphQL node ID for efficient mutations
                snapshot["node_ids"]["issues"][issue_number] = issue.get("id", "")

                # Process enhanced author data
                author = issue.get("author") or {}
                author_data = {
                    "login": author.get("login", "GitHub"),
                    "avatar": author.get("avatarUrl", ""),
                    "url": author.get("url", ""),
                    "name": author.get("name", ""),
                    "bio": author.get("bio", "") if author.get("bio") else author.get("description", "")
                }

                # Process assignees
                assignees = []
                for assignee in issue.get("assignees", {}).get("nodes", []):
                    assignee_login = assignee.get("login")
                    if assignee_login:
                        assignee_data = {
                            "login": assignee_login,
                            "avatar": assignee.get("avatarUrl", "")
                        }
                        assignees.append(assignee_data)
                        assignees_store[assignee_login] = assignee_data

                # Process milestone
                milestone = None
                if issue.get("milestone"):
                    milestone_data = {
                        "id": issue["milestone"].get("id", ""),
                        "title": issue["milestone"].get("title", ""),
                        "description": issue["milestone"].get("description", ""),
                        "state": issue["milestone"].get("state", "")
                    }
                    milestone = milestone_data
                    milestones_store[milestone_data["id"]] = milestone_data

                # Process enhanced labels with metadata
                labels = []
                label_details = []
                for label in issue.get("labels", {}).get("nodes", []):
                    label_name = label.get("name", "")
                    labels.append(label_name)
                    label_details.append({
                        "id": label.get("id", ""),
                        "name": label_name,
                        "color": label.get("color", ""),
                        "description": label.get("description", "")
                    })

                # Process comments with enhanced data
                comments = []
                comment_data = issue.get("comments", {})
                comment_nodes = comment_data.get("nodes", [])

                # Check if comments are truncated
                if comment_data.get("pageInfo", {}).get("hasNextPage"):
                    self.log.warning("Issue #%s has more than 100 comments - some may not be synced", issue_number)
                    truncated_entities.append(f"issues #{issue_number}")

                for comment in comment_nodes:
                    # Skip Discord message links to avoid loops
                    if comment.get("body") and DISCORD_MESSAGE_LINK_RE.search(comment["body"]):
                        continue

                    comment_author = comment.get("author") or {}
                    comments.append({
                        "id": comment.get("id"),
                        "database_id": comment.get("databaseId"),  # For REST API compatibility
                        "author": comment_author.get("login", "GitHub"),
                        "author_avatar": comment_author.get("avatarUrl", ""),
                        "author_url": comment_author.get("url", ""),
                        "body": comment.get("body", ""),
                        "body_text": comment.get("bodyText", ""),  # Plain text version
                        "url": comment.get("url", ""),
                        "created_at": comment.get("createdAt", ""),
                        "updated_at": comment.get("updatedAt", ""),
                        "last_edited_at": comment.get("lastEditedAt", "")
                    })
                    total_comments += 1

                # Build enhanced entry
                entry = {
                    "id": issue.get("id", ""),  # GraphQL node ID
                    "number": issue_number,
                    "title": issue.get("title", ""),
                    "state": issue["state"].lower(),  # GraphQL returns OPEN/CLOSED, we want lowercase
                    "state_reason": issue.get("stateReason", "").lower() if issue.get("stateReason") else None,
                    "locked": bool(issue.get("locked", False)),
                    "url": issue.get("url", ""),
                    "body": issue.get("body", ""),
                    "body_text": issue.get("bodyText", ""),  # Plain text version
                    "created_at": issue.get("createdAt", ""),
                    "updated_at": issue.get("updatedAt", ""),
                    "closed_at": issue.get("closedAt", ""),

                    # Enhanced data
                    "author": author_data,
                    "assignees": assignees,
                    "milestone": milestone,

                    # Labels with metadata
                    "labels": labels,  # For backward compatibility
                    "label_details": label_details,  # Full label data

                    # Comments
                    "comments": comments,

                    # Backward compatibility fields (kept for existing logic)
                    "user": author_data["login"],
                    "user_avatar": author_data["avatar"],
                }

                snapshot["issues"][str(issue_number)] = entry

            self.log.debug("Processed %d issues", issue_count)

            # Process PRs with enhanced data
            pr_count = 0
            for pr in all_prs:
                pr_count += 1
                pr_number = pr["number"]

                # Store GraphQL node ID for efficient mutations
                snapshot["node_ids"]["prs"][pr_number] = pr.get("id", "")

                # Process enhanced author data
                author = pr.get("author") or {}
                author_data = {
                    "login": author.get("login", "GitHub"),
                    "avatar": author.get("avatarUrl", ""),
                    "url": author.get("url", ""),
                    "name": author.get("name", ""),
                    "bio": author.get("bio", "") if author.get("bio") else author.get("description", "")
                }

                # Process assignees
                assignees = []
                for assignee in pr.get("assignees", {}).get("nodes", []):
                    assignee_login = assignee.get("login")
                    if assignee_login:
                        assignee_data = {
                            "login": assignee_login,
                            "avatar": assignee.get("avatarUrl", "")
                        }
                        assignees.append(assignee_data)
                        assignees_store[assignee_login] = assignee_data

                # Process reviewers
                reviewers = []
                for review_request in pr.get("reviewers", {}).get("nodes", []):
                    reviewer = review_request.get("requestedReviewer", {})
                    if reviewer.get("login"):  # User reviewer
                        reviewers.append({
                            "type": "user",
                            "login": reviewer.get("login"),
                            "avatar": reviewer.get("avatarUrl", "")
                        })
                    elif reviewer.get("name"):  # Team reviewer
                        reviewers.append({
                            "type": "team",
                            "name": reviewer.get("name"),
                            "slug": reviewer.get("slug", "")
                        })

                # Process milestone
                milestone = None
                if pr.get("milestone"):
                    milestone_data = {
                        "id": pr["milestone"].get("id", ""),
                        "title": pr["milestone"].get("title", ""),
                        "description": pr["milestone"].get("description", ""),
                        "state": pr["milestone"].get("state", "")
                    }
                    milestone = milestone_data
                    milestones_store[milestone_data["id"]] = milestone_data

                # Process enhanced labels with metadata
                labels = []
                label_details = []
                for label in pr.get("labels", {}).get("nodes", []):
                    label_name = label.get("name", "")
                    labels.append(label_name)
                    label_details.append({
                        "id": label.get("id", ""),
                        "name": label_name,
                        "color": label.get("color", ""),
                        "description": label.get("description", "")
                    })

                # Process latest commit info
                latest_commit = None
                if pr.get("commits", {}).get("nodes"):
                    commit_data = pr["commits"]["nodes"][0]["commit"]
                    latest_commit = {
                        "oid": commit_data.get("oid", ""),
                        "message": commit_data.get("messageHeadline", ""),
                        "author_name": commit_data.get("author", {}).get("name", ""),
                        "author_email": commit_data.get("author", {}).get("email", ""),
                        "date": commit_data.get("author", {}).get("date", "")
                    }

                # Process comments with enhanced data (both regular and review comments)
                comments = []
                
                # Process regular comments
                comment_data = pr.get("comments", {})
                comment_nodes = comment_data.get("nodes", [])

                # Check if regular comments are truncated
                if comment_data.get("pageInfo", {}).get("hasNextPage"):
                    self.log.warning("PR #%s has more than 100 regular comments - some may not be synced", pr_number)
                    truncated_entities.append(f"prs #{pr_number}")

                for comment in comment_nodes:
                    # Skip Discord message links to avoid loops
                    if comment.get("body") and DISCORD_MESSAGE_LINK_RE.search(comment["body"]):
                        continue

                    comment_author = comment.get("author") or {}
                    comments.append({
                        "id": comment.get("id"),
                        "database_id": comment.get("databaseId"),  # For REST API compatibility
                        "author": comment_author.get("login", "GitHub"),
                        "author_avatar": comment_author.get("avatarUrl", ""),
                        "author_url": comment_author.get("url", ""),
                        "body": comment.get("body", ""),
                        "body_text": comment.get("bodyText", ""),  # Plain text version
                        "url": comment.get("url", ""),
                        "created_at": comment.get("createdAt", ""),
                        "updated_at": comment.get("updatedAt", ""),
                        "last_edited_at": comment.get("lastEditedAt", ""),
                        "type": "comment"  # Mark as regular comment
                    })
                    total_comments += 1

                # Process review comments (through reviews structure)
                reviews_data = pr.get("reviews", {})
                reviews_nodes = reviews_data.get("nodes", [])

                # Check if reviews are truncated
                if reviews_data.get("pageInfo", {}).get("hasNextPage"):
                    self.log.warning("PR #%s has more than 50 reviews - some review comments may not be synced", pr_number)
                    truncated_entities.append(f"prs #{pr_number}")

                for review in reviews_nodes:
                    review_author = review.get("author") or {}
                    review_state = review.get("state", "")
                    review_body = review.get("body", "")
                    
                    # Add the review itself as a comment if it has content
                    if review_body and not DISCORD_MESSAGE_LINK_RE.search(review_body):
                        # Format review summary with state
                        state_text = f" ({review_state.lower()})" if review_state else ""
                        contextual_body = f"**Review{state_text}:**\n\n{review_body}"
                        
                        comments.append({
                            "id": review.get("id"),
                            "database_id": None,  # Reviews don't have databaseId
                            "author": review_author.get("login", "GitHub"),
                            "author_avatar": review_author.get("avatarUrl", ""),
                            "author_url": review_author.get("url", ""),
                            "body": contextual_body,
                            "body_text": review.get("bodyText", ""),
                            "url": review.get("url", ""),
                            "created_at": review.get("createdAt", ""),
                            "updated_at": review.get("updatedAt", ""),
                            "last_edited_at": review.get("updatedAt", ""),  # Reviews don't have lastEditedAt
                            "type": "review",  # Mark as review summary
                            "review_state": review_state
                        })
                        total_comments += 1

                    # Process line-by-line review comments within this review
                    review_comments_data = review.get("comments", {})
                    review_comment_nodes = review_comments_data.get("nodes", [])
                    
                    # Check if review comments are truncated
                    if review_comments_data.get("pageInfo", {}).get("hasNextPage"):
                        self.log.warning("PR #%s review has more than 50 comments - some may not be synced", pr_number)
                        truncated_entities.append(f"prs #{pr_number}")

                    for review_comment in review_comment_nodes:
                        # Skip Discord message links to avoid loops
                        if review_comment.get("body") and DISCORD_MESSAGE_LINK_RE.search(review_comment["body"]):
                            continue

                        comment_author = review_comment.get("author") or {}
                        
                        # Format review comment with file/line context
                        file_path = review_comment.get("path", "")
                        line_number = review_comment.get("line") or review_comment.get("position", "")
                        
                        # Create enhanced body with file context for review comments
                        original_body = review_comment.get("body", "")
                        if file_path and line_number:
                            contextual_body = f"**Review on `{file_path}` line {line_number}:**\n\n{original_body}"
                        elif file_path:
                            contextual_body = f"**Review on `{file_path}`:**\n\n{original_body}"
                        else:
                            contextual_body = f"**Code Review:**\n\n{original_body}"
                        
                        comments.append({
                            "id": review_comment.get("id"),
                            "database_id": review_comment.get("databaseId"),  # For REST API compatibility
                            "author": comment_author.get("login", "GitHub"),
                            "author_avatar": comment_author.get("avatarUrl", ""),
                            "author_url": comment_author.get("url", ""),
                            "body": contextual_body,  # Enhanced with file context
                            "body_text": review_comment.get("bodyText", ""),  # Plain text version
                            "url": review_comment.get("url", ""),
                            "created_at": review_comment.get("createdAt", ""),
                            "updated_at": review_comment.get("updatedAt", ""),
                            "last_edited_at": review_comment.get("lastEditedAt", ""),
                            "type": "review_comment",  # Mark as review comment
                            "file_path": file_path,
                            "line": line_number,
                            "diff_hunk": review_comment.get("diffHunk", ""),
                            "review_state": review_state  # Include the parent review state
                        })
                        total_comments += 1

                # Sort all comments by creation date to maintain chronological order
                comments.sort(key=lambda c: c.get("created_at", ""))

                # Build enhanced entry
                entry = {
                    "id": pr.get("id", ""),  # GraphQL node ID
                    "number": pr_number,
                    "title": pr.get("title", ""),
                    "state": pr["state"].lower(),  # GraphQL returns OPEN/CLOSED/MERGED, we want lowercase
                    "merged": bool(pr.get("merged", False)),
                    "merged_at": pr.get("mergedAt", ""),
                    "mergeable": pr.get("mergeable", ""),
                    "locked": bool(pr.get("locked", False)),
                    "url": pr.get("url", ""),
                    "body": pr.get("body", ""),
                    "body_text": pr.get("bodyText", ""),  # Plain text version
                    "created_at": pr.get("createdAt", ""),
                    "updated_at": pr.get("updatedAt", ""),
                    "closed_at": pr.get("closedAt", ""),
                    "head_ref": pr.get("headRefName", ""),
                    "base_ref": pr.get("baseRefName", ""),

                    # Enhanced data
                    "author": author_data,
                    "assignees": assignees,
                    "reviewers": reviewers,
                    "milestone": milestone,
                    "latest_commit": latest_commit,

                    # Labels with metadata
                    "labels": labels,  # For backward compatibility
                    "label_details": label_details,  # Full label data

                    # Comments
                    "comments": comments,

                    # Backward compatibility fields (kept for existing logic)
                    "user": author_data["login"],
                    "user_avatar": author_data["avatar"],
                }

                snapshot["prs"][str(pr_number)] = entry

            self.log.debug("Processed %d PRs", pr_count)

            # Store aggregated data
            snapshot["assignees"] = assignees_store
            snapshot["milestones"] = milestones_store

            # Store comprehensive fetch metadata
            snapshot["fetch_metadata"] = {
                "timestamp": time.time(),
                "total_api_calls": api_calls,
                "issues_fetched": issue_count,
                "prs_fetched": pr_count,
                "comments_fetched": total_comments,
                "truncated_entities": truncated_entities,
                "fetch_duration": time.time() - start_time,
                "labels_count": len(labels_data),
                "assignees_count": len(assignees_store),
                "milestones_count": len(milestones_store)
            }

        except Exception:
            self.log.exception("Failed to build GitHub snapshot using GraphQL")

        # Enhanced logging with performance metrics
        metadata = snapshot.get("fetch_metadata", {})
        total_items = len(snapshot["issues"]) + len(snapshot["prs"])
        items_per_call = total_items / max(metadata.get("total_api_calls", 1), 1)

        self.log.debug(
            "✅ GitHub snapshot built: %d labels, %d issues, %d PRs, %d comments | "
            "%d API calls in %.2fs (%.1f items/call) | %d assignees, %d milestones",
            len(snapshot["labels"]), len(snapshot["issues"]), len(snapshot["prs"]),
            metadata.get("comments_fetched", 0), metadata.get("total_api_calls", 0),
            metadata.get("fetch_duration", 0), items_per_call,
            metadata.get("assignees_count", 0), metadata.get("milestones_count", 0)
        )

        if metadata.get("truncated_entities"):
            self.log.warning("⚠️ Some entities have >100 comments and may be incomplete: %s",
                           ", ".join(metadata["truncated_entities"][:5]))

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
        self.log.debug("🔄 Starting 5-step reconciliation for guild %s", guild.id)

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

            self.log.debug("Processing %d %s in forum %s", len(cur[kind]), kind, forum.name)

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

        self.log.debug("✅ Completed 5-step reconciliation for guild %s", guild.id)

    # ----------------------
    # 5-Step Reconciliation Methods
    # ----------------------
    async def _step1_ensure_tag_parity(self, guild: discord.Guild, forum: discord.ForumChannel, repo, snapshot: Dict[str, Any], kind: str) -> None:
        """Step 1: Ensure parity between Discord and GitHub tags, with status tags first."""
        self.log.debug("📋 Step 1: Ensuring tag parity for %s forum", kind)

        # First, ensure status tags exist (required)
        await self._ensure_status_tags_exist(forum, kind)

        # Then reconcile with GitHub labels using snapshot data (no blocking calls)
        await self._reconcile_forum_and_labels_from_snapshot(guild, forum, snapshot)

        self.log.debug("✅ Step 1 completed for %s", kind)

    async def _step2_create_missing_posts(self, guild: discord.Guild, forum: discord.ForumChannel, prev: Dict[str, Any], cur: Dict[str, Any], kind: str) -> None:
        """Step 2: Create forum posts for new GitHub issues/PRs."""
        self.log.debug("➕ Step 2: Creating missing forum posts for %s", kind)

        prev_entities = prev.get(kind, {}) if prev else {}
        cur_entities = cur.get(kind, {})
        created_count = 0

        # Find truly new entities: in current but not in previous snapshot
        new_entities = {}
        for number_str, data in cur_entities.items():
            if number_str not in prev_entities:
                new_entities[number_str] = data

        # Special case: if there's no previous snapshot (first run), we need to be more careful
        # Only consider entities "new" if they don't already have Discord threads
        if not prev_entities:
            self.log.debug("No previous snapshot found - checking all entities against existing Discord threads")
            truly_new_entities = {}
            for number_str, data in new_entities.items():
                existing_thread = await self._find_existing_thread_for_issue(guild, int(number_str), kind)
                if not existing_thread:
                    truly_new_entities[number_str] = data
                else:
                    self.log.debug("Entity %s #%s has existing thread %s - not creating new", kind, number_str, existing_thread.id)
            new_entities = truly_new_entities

        if not new_entities:
            self.log.debug("No new %s to create", kind)
            return

        # Sort entities by creation date to ensure chronological forum post creation (oldest first)
        sorted_entities = sorted(
            new_entities.items(),
            key=lambda x: x[1].get("created_at", "")
        )

        self.log.debug("Found %d new %s to create: %s #%s (oldest) to %s #%s (newest)",
                     len(sorted_entities),
                     kind,
                     kind[:-1], sorted_entities[0][0],  # Remove 's' from 'issues'/'prs'
                     kind[:-1], sorted_entities[-1][0])

        # Use parallel processing for better performance
        async def create_single_post(number_str: str, data: Dict[str, Any]) -> bool:
            """Create a single forum post with all necessary checks."""
            try:
                # Double-check: Skip if this entity already has a thread (prevent duplicates from orphaned threads)
                existing_thread = await self._find_existing_thread_for_issue(guild, int(number_str), kind)
                if existing_thread:
                    self.log.debug("Skipping new %s #%s - thread was restored from orphaned state (ID: %s)", kind, number_str, existing_thread.id)
                    return False

                # Check if this GitHub content originally came from Discord (prevent loops)
                origin_data = await self._get_origin(guild, int(number_str), kind)
                if origin_data and origin_data.get("origin") == "discord":
                    self.log.debug("Skipping forum post creation for %s #%s - originated from Discord", kind, number_str)
                    return False

                # Create new thread for this issue/PR
                return await self._create_forum_post_parallel(guild, forum, data, kind)
            except Exception:
                self.log.exception("Failed to create forum post for %s #%s", kind, number_str)
                return False

        # Process posts in parallel batches to prevent overwhelming Discord API
        batch_size = 5  # Process 5 posts concurrently at most
        total_batches = (len(sorted_entities) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(sorted_entities))
            batch = sorted_entities[start_idx:end_idx]
            
            self.log.debug("Processing batch %d/%d: %s #%s to #%s (%d posts)",
                         batch_num + 1, total_batches,
                         kind[:-1], batch[0][0], batch[-1][0], len(batch))
            
            # Create all posts in the batch concurrently
            tasks = [create_single_post(number_str, data) for number_str, data in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successful creations
            batch_created = sum(1 for result in results if result is True)
            created_count += batch_created
            
            if batch_created > 0:
                self.log.debug("Batch %d/%d completed: Created %d/%d posts (total: %d)",
                             batch_num + 1, total_batches, batch_created, len(batch), created_count)
            
            # Small delay between batches to be gentle on Discord API
            if batch_num < total_batches - 1:  # Don't delay after the last batch
                await asyncio.sleep(0.5)

        self.log.debug("✅ Step 2 completed: Created %d new %s posts", created_count, kind)

    async def _step3_edit_existing_posts(self, guild: discord.Guild, forum: discord.ForumChannel, prev: Dict[str, Any], cur: Dict[str, Any], kind: str) -> None:
        """Step 3: Edit existing forum posts that have changed."""
        self.log.debug("✏️ Step 3: Editing existing forum posts for %s", kind)

        prev_entities = prev.get(kind, {})
        cur_entities = cur.get(kind, {})

        # Fast pre-screening: identify entities that might have changed
        entities_to_check = []
        for number_str, cur_data in cur_entities.items():
            prev_data = prev_entities.get(number_str, {})

            # Quick hash comparison without config lookup
            if self._quick_content_diff(cur_data, prev_data):
                entities_to_check.append((number_str, cur_data, prev_data))

        if not entities_to_check:
            self.log.debug("✅ Step 3 completed: No content changes detected, skipped all edits")
            return

        self.log.debug("Step 3: Pre-screening found %d/%d entities with potential changes",
                      len(entities_to_check), len(cur_entities))

        # Use parallel processing for editing posts
        async def edit_single_post(number_str: str, cur_data: Dict[str, Any], prev_data: Dict[str, Any]) -> bool:
            """Edit a single forum post with all necessary checks."""
            try:
                # Only process entities that already have threads
                thread = await self._get_thread_by_number(guild, number=int(number_str), kind=kind)
                if not thread:
                    return False

                # Check if we can safely edit this thread
                if not self._can_edit_thread(thread, cur_data, kind, "content"):
                    self.log.debug("Skipping edit for %s #%s - thread is archived/locked and should remain so", kind, number_str)
                    return False

                # Detailed hash comparison with config lookup (only for entities that passed pre-screening)
                if await self._has_content_changed(guild, int(number_str), kind, cur_data, prev_data):
                    return await self._edit_forum_post_parallel(guild, thread, cur_data, kind)
                return False
            except Exception:
                self.log.exception("Failed to edit forum post for %s #%s", kind, number_str)
                return False

        # Process edits in parallel batches
        batch_size = 5  # Process 5 edits concurrently at most
        total_batches = (len(entities_to_check) + batch_size - 1) // batch_size
        edited_count = 0
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(entities_to_check))
            batch = entities_to_check[start_idx:end_idx]
            
            # Edit all posts in the batch concurrently
            tasks = [edit_single_post(number_str, cur_data, prev_data) for number_str, cur_data, prev_data in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successful edits
            batch_edited = sum(1 for result in results if result is True)
            edited_count += batch_edited
            
            if batch_edited > 0:
                self.log.debug("Batch %d/%d: Edited %d/%d posts", batch_num + 1, total_batches, batch_edited, len(batch))
            
            # Small delay between batches
            if batch_num < total_batches - 1:
                await asyncio.sleep(0.3)

        self.log.debug("✅ Step 3 completed: Edited %d existing %s posts", edited_count, kind)

    async def _step4_sync_comments(self, guild: discord.Guild, forum: discord.ForumChannel, prev: Dict[str, Any], cur: Dict[str, Any], kind: str) -> None:
        """Step 4: Sync comments from GitHub to Discord."""
        self.log.debug("💬 Step 4: Syncing comments for %s", kind)

        prev_entities = prev.get(kind, {})
        cur_entities = cur.get(kind, {})

        # Fast pre-screening: identify entities that might have new comments
        entities_with_new_comments = []
        for number_str, cur_data in cur_entities.items():
            prev_data = prev_entities.get(number_str, {})

            # Quick comparison: different comment counts or new comment IDs
            if self._quick_comments_diff(cur_data, prev_data):
                entities_with_new_comments.append((number_str, cur_data, prev_data))

        if not entities_with_new_comments:
            self.log.debug("✅ Step 4 completed: No new comments detected, skipped comment sync")
            return

        self.log.debug("Step 4: Pre-screening found %d/%d entities with potential new comments",
                      len(entities_with_new_comments), len(cur_entities))

        # Use parallel processing for comment syncing
        async def sync_comments_for_entity(number_str: str, cur_data: Dict[str, Any], prev_data: Dict[str, Any]) -> int:
            """Sync comments for a single entity and return count of posted comments."""
            try:
                thread = await self._get_thread_by_number(guild, number=int(number_str), kind=kind)
                if not thread:
                    return 0

                # Check if we can safely post comments to this thread
                if not self._can_edit_thread(thread, cur_data, kind, "content"):
                    self.log.debug("Skipping comments for %s #%s - thread is archived/locked and should remain so", kind, number_str)
                    return 0

                new_comments = await self._get_new_comments(guild, int(number_str), kind, cur_data, prev_data)

                if not new_comments:
                    return 0

                self.log.debug("Processing %s #%s: found %d comments in GitHub, %d new comments to sync",
                             kind, number_str, len(cur_data.get("comments", [])), len(new_comments))

                # Post comments in parallel for this entity (but limit concurrency per entity)
                async def post_single_comment(comment: Dict[str, Any]) -> bool:
                    return await self._post_comment_parallel(guild, thread, comment, cur_data, kind)

                # Process comments for this entity in smaller batches
                comment_batch_size = 3  # Max 3 comments per entity concurrently
                successfully_posted_comments = []
                
                for i in range(0, len(new_comments), comment_batch_size):
                    comment_batch = new_comments[i:i + comment_batch_size]
                    tasks = [post_single_comment(comment) for comment in comment_batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Collect successfully posted comments
                    for comment, success in zip(comment_batch, results):
                        if success is True:
                            successfully_posted_comments.append(comment)

                # Only update hashes for comments that were actually posted to Discord
                if successfully_posted_comments:
                    await self._update_specific_comment_hashes(guild, int(number_str), kind, successfully_posted_comments)

                return len(successfully_posted_comments)
            except Exception:
                self.log.exception("Failed to sync comments for %s #%s", kind, number_str)
                return 0

        # Process entities in parallel batches
        batch_size = 3  # Process 3 entities concurrently (each may have multiple comments)
        total_batches = (len(entities_with_new_comments) + batch_size - 1) // batch_size
        comment_count = 0
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(entities_with_new_comments))
            batch = entities_with_new_comments[start_idx:end_idx]
            
            # Sync comments for all entities in the batch concurrently
            tasks = [sync_comments_for_entity(number_str, cur_data, prev_data) for number_str, cur_data, prev_data in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count total comments posted
            batch_comments = sum(result for result in results if isinstance(result, int))
            comment_count += batch_comments
            
            if batch_comments > 0:
                self.log.debug("Batch %d/%d: Posted %d comments", batch_num + 1, total_batches, batch_comments)
            
            # Small delay between batches
            if batch_num < total_batches - 1:
                await asyncio.sleep(0.3)

        self.log.debug("✅ Step 4 completed: Posted %d new comments", comment_count)

    async def _step5_update_post_status(self, guild: discord.Guild, forum: discord.ForumChannel, snapshot: Dict[str, Any], kind: str) -> None:
        """Step 5: Update forum post status (archived/locked) based on GitHub state."""
        self.log.debug("📂 Step 5: Updating post status for %s", kind)

        entities = snapshot.get(kind, {})

        # Lightning-fast hash-based pre-screening
        entities_needing_updates = await self._get_entities_with_state_changes(guild, forum, entities, kind)

        if not entities_needing_updates:
            self.log.debug("✅ Step 5 completed: No status changes detected via hash comparison, skipped all updates")
            return

        self.log.debug("Step 5: Hash-based screening found %d/%d entities needing status updates",
                      len(entities_needing_updates), len(entities))

        # Use parallel processing for status updates
        async def update_single_thread_status(number_str: str, data: Dict[str, Any], desired_tags: List[discord.ForumTag]) -> bool:
            """Update status for a single thread."""
            try:
                # Only now do we actually fetch the thread (for entities we know need updates)
                thread = await self._get_thread_by_number(guild, number=int(number_str), kind=kind)
                if not thread:
                    return False

                self.log.debug("Step 5: Updating status for %s #%s (state=%s, locked=%s)",
                             kind, number_str, data.get("state"), data.get("locked"))

                await self._apply_thread_state_parallel(thread, data, desired_tags, kind)

                # Store the new state hash after successful update
                await self._store_state_hash(guild, int(number_str), kind, data, desired_tags)
                return True
            except Exception:
                self.log.exception("Failed to update status for %s #%s", kind, number_str)
                return False

        # Process status updates in parallel batches
        batch_size = 5  # Process 5 status updates concurrently at most
        total_batches = (len(entities_needing_updates) + batch_size - 1) // batch_size
        updated_count = 0
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(entities_needing_updates))
            batch = entities_needing_updates[start_idx:end_idx]
            
            # Update all status in the batch concurrently
            tasks = [update_single_thread_status(number_str, data, desired_tags) for number_str, data, desired_tags in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successful updates
            batch_updated = sum(1 for result in results if result is True)
            updated_count += batch_updated
            
            if batch_updated > 0:
                self.log.debug("Batch %d/%d: Updated %d/%d thread status", batch_num + 1, total_batches, batch_updated, len(batch))
            
            # Small delay between batches
            if batch_num < total_batches - 1:
                await asyncio.sleep(0.3)

        self.log.debug("✅ Step 5 completed: Updated status for %d %s posts", updated_count, kind)

    # ----------------------
    # Helper methods for the 5-step process
    # ----------------------
    async def _create_forum_post(self, guild: discord.Guild, forum: discord.ForumChannel, data: Dict[str, Any], kind: str) -> bool:
        """Create a new forum post for a GitHub issue/PR."""
        try:
            number = data["number"]
            self.log.debug("Creating forum post for %s #%s: %s", kind, number, data.get("title", ""))

            # Prepare content with avatar and organization info - include number at start
            title = self._format_forum_title(data, kind)

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
            
            # Combine tags, ensuring status tags have priority and we don't exceed Discord's 5-tag limit
            all_tags = status_tags  # Status tags first (most important)
            for tag in label_tags:
                if tag.id not in {st.id for st in all_tags} and len(all_tags) < 5:
                    all_tags.append(tag)
            
            # Log if we had to drop some tags due to Discord's limit
            total_desired_tags = len(status_tags) + len([t for t in label_tags if t.id not in {st.id for st in status_tags}])
            if total_desired_tags > 5:
                dropped_count = total_desired_tags - 5
                self.log.warning("Discord 5-tag limit: dropped %d label tags for %s #%s (kept %d status + %d label tags)", 
                               dropped_count, kind, number, len(status_tags), len(all_tags) - len(status_tags))

            # Create thread with rate limit handling and bot tracking
            thread = None
            try:
                created = await forum.create_thread(name=title, content=body, applied_tags=all_tags)
                thread = created.thread if hasattr(created, "thread") else created

                # Track this thread as bot-created to prevent feedback loops
                if isinstance(thread, discord.Thread):
                    self._bot_creating_threads.add(thread.id)
                    self.log.debug("Tracking bot-created thread %s for issue #%s", thread.id, number)

            except discord.HTTPException as e:
                if e.status == 429:  # Rate limited
                    retry_after = e.response.headers.get('Retry-After', '5')
                    self.log.warning("Discord rate limited while creating %s #%s, waiting %s seconds", kind, number, retry_after)
                    await asyncio.sleep(float(retry_after))
                    # Retry once
                    created = await forum.create_thread(name=title, content=body, applied_tags=all_tags)
                    thread = created.thread if hasattr(created, "thread") else created

                    # Track this thread as bot-created to prevent feedback loops (retry case)
                    if isinstance(thread, discord.Thread):
                        self._bot_creating_threads.add(thread.id)
                        self.log.debug("Tracking bot-created thread %s for issue #%s (retry)", thread.id, number)
                elif e.status == 400 and e.code == 50035:  # Invalid Form Body
                    # Log detailed information about what might be causing the issue
                    self.log.error("Invalid form body creating %s #%s - debugging info:", kind, number)
                    self.log.error("  Title length: %d chars - %r", len(title), title[:100])
                    self.log.error("  Content length: %d chars - %r", len(body), body[:200])
                    self.log.error("  Applied tags: %d tags - %s", len(all_tags), [f"{t.name}({t.id})" for t in all_tags])
                    self.log.error("  Forum channel: %s (%s)", forum.name, forum.id)
                    
                    # Check for specific issues
                    if len(title) == 0:
                        self.log.error("  ❌ Title is empty!")
                    elif len(title) > 100:
                        self.log.error("  ❌ Title exceeds 100 character limit!")
                    
                    if len(body) == 0:
                        self.log.error("  ❌ Content is empty!")
                    elif len(body) > 2000:
                        self.log.error("  ❌ Content exceeds 2000 character limit!")
                    
                    if len(all_tags) > 5:
                        self.log.error("  ❌ Too many tags applied (limit: 5)!")
                    
                    # Check for invalid tag IDs
                    forum_tag_ids = {t.id for t in forum.available_tags}
                    invalid_tags = [t for t in all_tags if t.id not in forum_tag_ids]
                    if invalid_tags:
                        self.log.error("  ❌ Invalid tag IDs: %s", [f"{t.name}({t.id})" for t in invalid_tags])
                    
                    raise  # Re-raise the original exception
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

            # Add small delay after thread creation to prevent rate limiting
            await asyncio.sleep(0.1)

            # Post enhanced embed with avatars and colors
            embed_msg = await self._post_initial_embed(thread, data, kind)

            # Store message tracking info
            if embed_msg:
                await self._store_embed_message_id(guild, number, kind, thread.id, embed_msg.id)

            # Store content hashes for change detection (but not comments yet - they'll be processed in Step 4)
            try:
                await asyncio.wait_for(
                    self._store_content_hashes(guild, number, kind, data, include_comments=False),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                self.log.warning("Timeout storing content hashes for %s #%s - continuing", kind, number)

            # Add delay before applying thread state to prevent issues
            await asyncio.sleep(0.1)

            # Apply final state (archived/locked) with timeout protection
            try:
                await asyncio.wait_for(
                    self._apply_thread_state(thread, data, all_tags, kind),
                    timeout=45.0  # Longer timeout since this involves multiple Discord API calls
                )
            except asyncio.TimeoutError:
                self.log.error("Timeout applying thread state for %s #%s - continuing", kind, number)
                # Continue execution rather than failing the entire post creation

            # Note: Thread will be removed from _bot_creating_threads when on_thread_create is called
            # or after a timeout to prevent memory leaks

            return True

        except Exception:
            self.log.exception("Failed to create forum post for %s #%s", kind, data.get("number", "unknown"))
            # Clean up tracking in case of failure
            if thread and isinstance(thread, discord.Thread):
                self._bot_creating_threads.discard(thread.id)
            return False
        finally:
            # Cleanup: Remove from tracking after a reasonable delay in case on_thread_create doesn't fire
            if thread and isinstance(thread, discord.Thread):
                asyncio.create_task(self._cleanup_thread_tracking(thread.id))

    async def _edit_forum_post(self, guild: discord.Guild, thread: discord.Thread, data: Dict[str, Any], kind: str) -> bool:
        """Edit an existing forum post with updated content."""
        try:
            number = data["number"]
            self.log.debug("Editing forum post for %s #%s", kind, number)

            # Update thread title if changed - include number at start
            new_title = self._format_forum_title(data, kind)
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
            # Check batch queue first if in batch mode
            stored_hashes = {}
            if self._batch_config_mode:
                batch_key = f"custom.content_hashes.{kind}.{number}"
                if batch_key in self._pending_config_updates:
                    stored_hashes = self._pending_config_updates[batch_key]

            # Fall back to stored config if not in batch or not found in batch
            if not stored_hashes:
                stored_hashes = await self.config.custom("content_hashes", guild.id).get_raw(kind, str(number), default={})

            # Calculate current hashes
            title_hash = self._hash_content(cur_data.get("title", ""))
            body_hash = self._hash_content(cur_data.get("body", ""))

            # If no stored hashes exist, this might be the first time - check against previous data
            if not stored_hashes.get("title_hash") and not stored_hashes.get("body_hash"):
                # Compare against previous snapshot data instead
                if prev_data:
                    prev_title_hash = self._hash_content(prev_data.get("title", ""))
                    prev_body_hash = self._hash_content(prev_data.get("body", ""))
                    return title_hash != prev_title_hash or body_hash != prev_body_hash
                # No previous data and no stored hashes - treat as new content
                return True

            # Compare with stored hashes
            if (stored_hashes.get("title_hash") != title_hash or
                stored_hashes.get("body_hash") != body_hash):
                self.log.debug("Content changed for %s #%s: title_hash=%s->%s, body_hash=%s->%s",
                             kind, number, 
                             stored_hashes.get("title_hash", "None")[:8], title_hash[:8],
                             stored_hashes.get("body_hash", "None")[:8], body_hash[:8])
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

            # If no stored comment hashes and we have previous data, use previous data for comparison
            if not stored_comment_hashes and prev_data:
                prev_comment_map = {str(c.get("id")): self._hash_content(c.get("body", "")) 
                                  for c in prev_data.get("comments", []) if c.get("id")}
                
                for comment in cur_data.get("comments", []):
                    comment_id = comment.get("id")
                    if not comment_id:
                        continue

                    comment_hash = self._hash_content(comment.get("body", ""))
                    prev_hash = prev_comment_map.get(str(comment_id))

                    # New comment or changed comment
                    if prev_hash != comment_hash:
                        new_comments.append(comment)
            else:
                # Use stored hashes for comparison
                for comment in cur_data.get("comments", []):
                    comment_id = comment.get("id")
                    if not comment_id:
                        continue

                    comment_hash = self._hash_content(comment.get("body", ""))
                    stored_hash = stored_comment_hashes.get(str(comment_id))

                    # If hash is different or doesn't exist, it's new/updated
                    if stored_hash != comment_hash:
                        new_comments.append(comment)

            if new_comments:
                self.log.debug("Comments for %s #%s: %d total, %d new/changed",
                             kind, number, total_comments, len(new_comments))

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
            # Create embed with timeout protection
            embed = await asyncio.wait_for(
                self._create_embed(data, kind),
                timeout=10.0  # 10 second timeout for embed creation
            )
            
            # Add retry logic for Discord rate limits with timeout
            for attempt in range(3):
                try:
                    # Send embed with timeout protection
                    return await asyncio.wait_for(
                        thread.send(embed=embed),
                        timeout=15.0  # 15 second timeout for sending
                    )
                except asyncio.TimeoutError:
                    self.log.warning("Timeout posting embed for thread %s (attempt %d/3)", thread.id, attempt + 1)
                    if attempt == 2:  # Last attempt
                        raise
                    await asyncio.sleep(2)
                    continue
                except discord.HTTPException as e:
                    if e.status == 429:  # Rate limited
                        retry_after = float(e.response.headers.get('Retry-After', '1'))
                        self.log.warning("Rate limited posting embed for thread %s, waiting %.1fs (attempt %d/3)", 
                                       thread.id, retry_after, attempt + 1)
                        await asyncio.sleep(min(retry_after, 10))  # Cap wait time at 10 seconds
                        continue
                    else:
                        raise
                except Exception as e:
                    if attempt == 2:  # Last attempt
                        raise
                    self.log.warning("Failed to post embed (attempt %d/3): %s", attempt + 1, str(e))
                    await asyncio.sleep(1)
                    
            return None
            
        except asyncio.TimeoutError:
            self.log.error("Timeout creating or posting embed for thread %s", thread.id)
            return None
        except Exception:
            self.log.exception("Failed to post initial embed for thread %s", thread.id)
            return None

    async def _create_embed(self, data: Dict[str, Any], kind: str) -> discord.Embed:
        """Create an enhanced embed with avatars, colors, and proper formatting."""
        try:
            # Get status-based color (simplified for open-only entities)
            color = self._get_status_color(data, kind)

            # Clean and truncate content with length limits
            title = self._clean_discord_text(data.get("title", ""))[:240]  # Leave buffer under 256
            description = self._clean_discord_text(data.get("body", ""))[:3800]  # Leave buffer under 4096

            # Create basic embed
            embed = discord.Embed(
                title=title or "No title",
                url=data.get("url", ""),
                description=description or "No description",
                color=color
            )

            # Set author with fallbacks to prevent hanging on avatar loading
            author_name = data.get("user", "GitHub")[:240]  # Ensure name is not too long
            author_avatar = data.get("user_avatar", "")
            
            # Only set avatar if URL looks valid to prevent hanging
            if author_avatar and author_avatar.startswith(("http://", "https://")):
                try:
                    embed.set_author(name=author_name, icon_url=author_avatar)
                except Exception:
                    # Fallback to no avatar if there's any issue
                    embed.set_author(name=author_name)
            else:
                embed.set_author(name=author_name)

            return embed
            
        except Exception:
            # Fallback embed if anything goes wrong
            self.log.exception("Failed to create embed, using fallback")
            return discord.Embed(
                title=f"{kind[:-1].title()} #{data.get('number', 'Unknown')}",
                description="Error creating embed - see logs",
                color=discord.Color.default()
            )

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
            # Use retry logic to handle Windows file locking issues
            for attempt in range(3):
                try:
                    message_data = await self.config.custom("discord_messages", guild.id).get_raw(kind, str(number), default={})
                    if "comments" not in message_data:
                        message_data["comments"] = {}
                    message_data["comments"][str(comment_id)] = message_id
                    await self.config.custom("discord_messages", guild.id).set_raw(kind, str(number), value=message_data)
                    break
                except PermissionError:
                    if attempt < 2:  # Don't sleep on the last attempt
                        await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    else:
                        raise
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
                    self.log.debug("Linked Discord thread %s to existing GitHub %s #%s", thread.id, kind, number)
                    return

            # Only create new GitHub issues for issues forum, not PRs
            if kind == "prs":
                self.log.debug("Skipping GitHub PR creation for Discord thread - PRs should be created on GitHub")
                return

            # Create new GitHub issue from Discord thread
            repo = await self._get_repo(guild)
            if not repo:
                return

            # Create GitHub issue with user avatar and formatted link
            issue_title = thread.name or "New issue from Discord"
            
            # Format the issue body with user avatar and markdown link
            user_info = self._format_discord_user_for_github(message.author, message.jump_url)
            issue_body = f"{user_info}\n\n{message.content}"

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

            # IMPORTANT: Invalidate state hash since new GitHub issue was created
            await self._invalidate_state_hash(guild, github_issue.number, kind)

            self.log.debug("Created GitHub issue #%s from Discord thread %s", github_issue.number, thread.id)

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

            # Format comment for GitHub with user avatar and formatted link
            user_info = self._format_discord_user_for_github(message.author, message.jump_url)
            comment_body = f"{user_info}\n\n{message.content}"

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

            # IMPORTANT: Invalidate state hash since GitHub state changed
            await self._invalidate_state_hash(guild, issue_number, kind)

            self.log.debug("Posted Discord comment %s as GitHub comment %s on %s #%s",
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

            # Update GitHub comment with user avatar and formatted link
            user_info = self._format_discord_user_for_github(after.author, after.jump_url)
            new_comment_body = f"{user_info}\n\n{after.content}"

            if kind == "issues":
                await asyncio.to_thread(
                    lambda: repo.get_issue(number=issue_number).get_comment(int(github_comment_id)).edit(new_comment_body)
                )
            else:
                await asyncio.to_thread(
                    lambda: repo.get_pull(number=issue_number).get_comment(int(github_comment_id)).edit(new_comment_body)
                )

            # IMPORTANT: Invalidate state hash since GitHub state changed
            await self._invalidate_state_hash(guild, issue_number, kind)

            self.log.debug("Updated GitHub comment %s from Discord edit %s on %s #%s",
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

            # IMPORTANT: Invalidate state hash since GitHub state changed
            await self._invalidate_state_hash(guild, issue_number, kind)

            self.log.debug("Deleted GitHub comment %s from Discord deletion %s on %s #%s",
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
            
            # Use retry logic to handle Windows file locking issues
            for attempt in range(3):
                try:
                    await self.config.custom("content_origins", guild.id).set_raw("comments", comment_key, value=origin_data)
                    break
                except PermissionError:
                    if attempt < 2:  # Don't sleep on the last attempt
                        await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    else:
                        raise
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
            # Use retry logic to handle Windows file locking issues
            for attempt in range(3):
                try:
                    comments_data = await self.config.custom("content_origins", guild.id).get_raw("comments", default={})
                    if comment_key in comments_data:
                        del comments_data[comment_key]
                        await self.config.custom("content_origins", guild.id).set_raw("comments", value=comments_data)
                    break
                except PermissionError:
                    if attempt < 2:  # Don't sleep on the last attempt
                        await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    else:
                        raise
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
                
                # Combine tags, ensuring status tags have priority and we don't exceed Discord's 5-tag limit
                desired_tags = status_tags  # Status tags first (most important)
                for tag in label_tags:
                    if tag.id not in {st.id for st in desired_tags} and len(desired_tags) < 5:
                        desired_tags.append(tag)

                if not thread:
                    # Create new thread
                    self.log.debug("Creating thread for %s #%s: %s", kind, number, data.get("title", ""))
                    try:
                        # Clean and validate title (Discord limit: 100 chars) - include number at start
                        title = self._format_forum_title(data, kind)

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

        self.log.debug("Reconciled %s: %d created, %d updated, %d failed", kind, created_count, updated_count, failed_count)

    def _get_status_tags_for_entity(self, forum: discord.ForumChannel, data: Dict[str, Any], kind: str) -> List[discord.ForumTag]:
        """Get Discord-only status tags for an entity based on its state."""
        status_names = []

        # Since we only work with open entities, everything gets "open" status
        if data.get("state") == "open":
            status_names.append("open")

        return self._labels_to_forum_tags(forum, status_names)

    async def _apply_thread_state(self, thread: discord.Thread, data: Dict[str, Any], desired_tags: List[discord.ForumTag], kind: str) -> None:
        """Apply tags and archived/locked state to a thread."""
        try:
            # Mark this thread as being updated by the bot to prevent feedback loops
            self._bot_updating_threads.add(thread.id)
            # Since we only work with open entities, threads should never be archived or locked
            desired_archived = False
            desired_locked = False

            # Add timeout protection for the entire operation
            timeout_seconds = 30  # 30 second timeout to prevent hanging

            # Log the state being applied
            issue_number = data.get("number", "unknown")
            state_info = f"{data.get('state', 'unknown')}"
            if kind == "prs" and data.get("merged"):
                state_info += " (merged)"

            self.log.debug("Applying state to %s #%s thread %s: GitHub state=%s, desired archived=%s, locked=%s",
                         kind, issue_number, thread.id, state_info, desired_archived, desired_locked)

            # Enforce Discord's 5-tag limit as a safety measure
            if len(desired_tags) > 5:
                # Get forum to identify status tags
                forum = thread.parent
                if isinstance(forum, discord.ForumChannel):
                    status_tag_names = self._status_tag_names
                    status_tags = [t for t in desired_tags if t.name.lower() in status_tag_names]
                    label_tags = [t for t in desired_tags if t.name.lower() not in status_tag_names]
                    
                    # Prioritize status tags, then take as many label tags as fit
                    safe_desired_tags = status_tags + label_tags[:5-len(status_tags)]
                    
                    dropped_count = len(desired_tags) - len(safe_desired_tags)
                    self.log.warning("Discord 5-tag limit: dropped %d tags for %s #%s thread %s (kept %d status + %d label tags)", 
                                   dropped_count, kind, data.get("number", "unknown"), thread.id, 
                                   len(status_tags), len(safe_desired_tags) - len(status_tags))
                    desired_tags = safe_desired_tags
                else:
                    # Fallback: just take first 5 tags
                    desired_tags = desired_tags[:5]
                    self.log.warning("Discord 5-tag limit: truncated to first 5 tags for thread %s", thread.id)

            # Check what needs updating
            current_tag_ids = {t.id for t in getattr(thread, "applied_tags", [])}
            desired_tag_ids = {t.id for t in desired_tags}
            needs_tag_update = current_tag_ids != desired_tag_ids
            needs_state_update = (thread.archived != desired_archived or thread.locked != desired_locked)

            # Wrap all Discord operations in a timeout to prevent hanging
            async def apply_changes():
                # Since we only work with open entities, any archived thread should be unarchived
                if thread.archived:
                    self.log.debug("Unarchiving thread %s (should be open)", thread.id)
                    await thread.edit(archived=False, locked=False)
                    await asyncio.sleep(0.1)  # Small delay for Discord to process

                # Apply tags if needed
                if needs_tag_update:
                    self.log.debug("Updating tags for thread %s", thread.id)
                    await thread.edit(applied_tags=desired_tags)
                    await asyncio.sleep(0.1)  # Small delay for Discord to process

                # Ensure thread is in correct state (should always be unarchived/unlocked for open entities)
                if needs_state_update and (desired_archived or desired_locked):
                    self.log.debug("Updating state for thread %s: archived=%s, locked=%s",
                                 thread.id, desired_archived, desired_locked)
                    await thread.edit(archived=desired_archived, locked=desired_locked)

            # Apply changes with timeout protection
            try:
                await asyncio.wait_for(apply_changes(), timeout=timeout_seconds)
                self.log.debug("Successfully applied state to thread %s", thread.id)
            except asyncio.TimeoutError:
                self.log.error("Timeout applying state to thread %s after %ds - skipping", thread.id, timeout_seconds)
                return

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
            elif e.code == 50035 and "Must be 5 or fewer in length" in str(e):
                self.log.error("Discord 5-tag limit exceeded for thread %s - this should have been prevented! Tags: %d", 
                             thread.id, len(desired_tags))
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
            
            # Combine tags, ensuring status tags have priority and we don't exceed Discord's 5-tag limit
            desired_tags = status_tags  # Status tags first (most important)
            for tag in label_tags:
                if tag.id not in {st.id for st in desired_tags} and len(desired_tags) < 5:
                    desired_tags.append(tag)

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
                    self.log.debug(
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

                    await forum.create_tag(name=tag_name, emoji=None, moderated=False)
                    self.log.debug("Created status tag '%s' in %s forum", tag_name, kind)
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

    async def _reconcile_forum_and_labels_from_snapshot(self, guild: discord.Guild, forum: discord.ForumChannel, snapshot: Dict[str, Any]) -> None:
        """
        Reconcile forum tags with GitHub labels using snapshot data (no blocking GitHub API calls).
        This eliminates the expensive repo.get_labels() call that was slowing down every sync.
        """
        try:
            # Get GitHub labels from snapshot (fast - no API call)
            gh_labels_data = snapshot.get("labels", {})
            gh_label_names = set(gh_labels_data.keys())
            forum_tag_names = {t.name for t in forum.available_tags}

            # Track changes for logging
            created_github_labels = 0
            created_discord_tags = 0

            # Create missing GitHub labels from Discord forum tags (if needed)
            # Note: This still requires PyGithub calls, but only for new labels (rare)
            repo = await self._get_repo(guild)
            if repo:
                for name in sorted(forum_tag_names - gh_label_names):
                    if name.lower() in self._status_tag_names:
                        continue  # Skip Discord-only status tags
                    try:
                        await asyncio.to_thread(lambda: repo.create_label(name=name, color="ededed"))
                        created_github_labels += 1
                        self.log.debug("Created GitHub label '%s' from Discord tag", name)
                    except Exception:
                        self.log.debug("Failed to create GitHub label '%s' (may already exist)", name)

            # Create missing Discord forum tags from GitHub labels (if space permits)
            missing_discord_tags = sorted(gh_label_names - forum_tag_names)
            if missing_discord_tags:
                # Check Discord's 20-tag limit
                current_tag_count = len(forum.available_tags)
                available_slots = 20 - current_tag_count

                if available_slots > 0:
                    tags_to_create = missing_discord_tags[:available_slots]
                    for name in tags_to_create:
                        try:
                            await forum.create_tag(name=name)
                            created_discord_tags += 1
                            self.log.debug("Created Discord tag '%s' from GitHub label", name)
                        except Exception:
                            self.log.debug("Failed to create Discord tag '%s'", name)

                    if len(missing_discord_tags) > available_slots:
                        self.log.warning(
                            "Could only create %d of %d Discord tags due to 20-tag limit. "
                            "Remaining GitHub labels: %s",
                            available_slots, len(missing_discord_tags),
                            ", ".join(missing_discord_tags[available_slots:])
                        )
                else:
                    self.log.warning(
                        "Cannot create %d Discord tags from GitHub labels: 20-tag limit reached. "
                        "Missing labels: %s",
                        len(missing_discord_tags), ", ".join(missing_discord_tags[:5])
                    )

            if created_github_labels or created_discord_tags:
                self.log.debug("Tag sync: created %d GitHub labels, %d Discord tags",
                             created_github_labels, created_discord_tags)
            else:
                self.log.debug("Tag sync: no changes needed")

        except Exception:
            self.log.exception("Failed to reconcile forum tags and GitHub labels")

    async def _reconcile_forum_and_labels(self, guild: discord.Guild, forum: discord.ForumChannel, repo) -> None:
        """
        Legacy method for backward compatibility (still makes blocking calls).
        New code should use _reconcile_forum_and_labels_from_snapshot().
        """
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

