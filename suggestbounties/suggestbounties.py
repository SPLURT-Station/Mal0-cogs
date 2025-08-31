from redbot.core import commands, Config
import discord
from typing import Optional, List, Dict, Any
import re
from github import Github, GithubException
import yaml  # type: ignore
import logging
import io
import json
from datetime import datetime

class TemplateConfigView(discord.ui.View):
    def __init__(self, schema, on_confirm, on_cancel, message=None, author_id: Optional[int] = None):
        # No timeout; view will persist until explicitly cleared
        super().__init__(timeout=None)
        self.schema = schema
        self.on_confirm = on_confirm
        self.on_cancel = on_cancel
        self.message = message
        self.author_id = author_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Allow interactions only from the command invoker."""
        try:
            if self.author_id is not None and interaction.user.id != self.author_id:
                await interaction.response.send_message("You cannot use this configuration. Only the command invoker can interact with it.", ephemeral=True)
                return False
        except Exception:
            # If we fail to respond, still block the interaction
            return False
        return True

    @discord.ui.button(label="Configure Template", style=discord.ButtonStyle.green)
    async def submit(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Use SchemaModal instead of TemplateInputModal for proper dynamic field generation
        async def modal_callback(modal, modal_interaction):
            await self.on_confirm(modal.responses, modal_interaction)

        modal = SchemaModal(
            schema=self.schema,
            message_link="",  # Empty for template configuration
            suggestion_name="",
            suggestion_number="",
            issue_body="",
            on_submit_callback=modal_callback
        )
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Template configuration cancelled.", ephemeral=True)
        await self.on_cancel()



# Helper to parse YAML schema and extract modal fields
class SchemaModal(discord.ui.Modal):
    def __init__(self, schema: dict, message_link: str, suggestion_name: str, suggestion_number: str, issue_body: str, on_submit_callback):
        title = schema.get("title", "New Issue")
        super().__init__(title=title[:45])
        self.schema = schema
        self.message_link = message_link
        self.suggestion_name = suggestion_name
        self.suggestion_number = suggestion_number
        self.issue_body = issue_body
        self.on_submit_callback = on_submit_callback
        self.responses: Dict[str, str] = {}
        self.field_map = []  # (id, type, label, required, placeholder, description)
        self.info_texts = []

        # Add title field if schema has a title
        if "title" in schema:
            self.add_item(discord.ui.TextInput(
                label="Issue Title",
                placeholder=schema.get("title", ""),
                default=schema.get("title", ""),
                required=True,
                max_length=100,
                style=discord.TextStyle.short,
                custom_id="title"
            ))

        # Parse schema body
        field_count = 1 if "title" in schema else 0
        for item in schema.get("body", []):
            if field_count >= 5:  # Discord modal limit
                break
            if item.get("type") == "markdown":
                self.info_texts.append(item["attributes"]["value"])
            elif item.get("type") in ("textarea", "input"):
                field_id = item.get("id")
                label = item["attributes"].get("label", field_id)
                placeholder = item["attributes"].get("placeholder", "")
                required = item.get("validations", {}).get("required", False)
                description = item["attributes"].get("description", "")
                style = discord.TextStyle.paragraph if item["type"] == "textarea" else discord.TextStyle.short
                # Pre-fill suggestion-link if present
                default = ""
                if field_id == "suggestion-link":
                    default = message_link
                self.add_item(discord.ui.TextInput(
                    label=label[:45],
                    placeholder=placeholder[:100],
                    required=required,
                    style=style,
                    custom_id=field_id,
                    default=default
                ))
                self.field_map.append((field_id, item["type"], label, required, placeholder, description))
                field_count += 1
        # If less than 1 field, add a dummy so modal is valid
        if field_count == 0:
            self.add_item(discord.ui.TextInput(label="(No fields)", required=False, style=discord.TextStyle.short, custom_id="dummy"))

    async def on_submit(self, interaction: discord.Interaction):
        for child in self.children:
            if hasattr(child, 'custom_id') and getattr(child, 'custom_id', None):
                self.responses[getattr(child, 'custom_id')] = getattr(child, 'value')  # type: ignore
        await self.on_submit_callback(self, interaction)

class SuggestBounties(commands.Cog):
    """
    Links suggestions from ideaboard to GitHub issues as bounties, tracks their status, and updates messages when issues are closed.
    """

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=908039527271104513, force_registration=True)
        self.config.register_guild(
            github_repo=None,  # e.g. "owner/repo"
            github_token=None, # GitHub personal access token
            suggestion_channel=None, # Channel ID for suggestions
            github_schema=None, # YAML schema for issue template
            github_template=None, # Template format string
            auto_suggestions_enabled=True, # Toggle for automatic suggestion processing
            reason_keyword=None, # Optional keyword to require in Reason field (case-insensitive)
        )
        # Add storage for tracking suggestions and their GitHub issue status
        self.config.init_custom("suggestions", 1)
        self.config.register_custom("suggestions",
            suggestions={}  # message_id -> {"suggestion_number": str, "github_issue_url": str, "created_at": str, "status": str}
        )
        self.log = logging.getLogger(f"red.{__name__}")
        # Placeholder for any startup logic, such as loading cache or setting up background tasks

    def _replace_wildcards(self, text: str, suggestion_name: str, suggestion_number: str, message_link: str, issue_body: str, suggesting_user: str = "") -> str:
        """Replace wildcards in template text with actual values"""
        if not text:
            return text

        replacements = {
            "{suggestion_name}": suggestion_name,
            "{suggestion_number}": suggestion_number,
            "{message_link}": message_link,
            "{issue_body}": issue_body,
            "{suggesting_user}": suggesting_user,
        }

        result = text
        for wildcard, value in replacements.items():
            result = result.replace(wildcard, value)

        return result

    async def _store_suggestion_status(self, guild_id: int, message_id: int, suggestion_number: str, github_issue_url: Optional[str] = None, status: str = "pending"):
        """Store the status of a suggestion"""
        async with self.config.custom("suggestions", guild_id).suggestions() as suggestions:
            suggestions[str(message_id)] = {
                "suggestion_number": suggestion_number,
                "github_issue_url": github_issue_url,
                "created_at": datetime.utcnow().isoformat(),
                "status": status
            }

    async def _get_suggestion_status(self, guild_id: int, message_id: int) -> Optional[Dict[str, Any]]:
        """Get the status of a suggestion"""
        suggestions = await self.config.custom("suggestions", guild_id).suggestions()
        return suggestions.get(str(message_id))

    async def _create_github_issue(self, ctx: Optional[commands.Context], message: discord.Message, suggestion_text: str, reason_text: Optional[str], results_text: Optional[str], suggesting_user: str = "") -> Optional[str]:
        """Create a GitHub issue for a suggestion and return the issue URL or None if failed"""
        guild = ctx.guild if ctx else message.guild
        if not guild:
            return None

        config = self.config.guild(guild)
        repo_name = await config.github_repo()
        token = await config.github_token()

        if not repo_name or not token:
            return None

        schema_yaml = await config.github_schema()
        schema = yaml.safe_load(schema_yaml) if schema_yaml else None
        template = await config.github_template()

        suggestion_name = message.content
        m = re.search(r"#(\d+)", message.content)
        suggestion_number = m.group(1) if m else ""
        message_link = message.jump_url

        # Compose issue body
        issue_body = f"**Suggestion:**\n{suggestion_text}\n\n"
        if reason_text:
            issue_body += f"**Reason:**\n{reason_text}\n\n"
        issue_body += f"**Results:**\n{results_text}\n"

        try:
            gh = Github(token)
            repo = gh.get_repo(repo_name)

            if schema and template:
                # Parse the stored template responses
                import ast
                template_responses = ast.literal_eval(template)

                # Build issue body from schema, using template responses and auto-filled values
                body_md = ""
                for item in schema.get("body", []):
                    # Skip markdown sections - they're only for form display, not issue content
                    if item.get("type") == "markdown":
                        continue
                    elif item.get("type") in ("textarea", "input"):
                        field_id = item.get("id")
                        label = item["attributes"].get("label", field_id)

                        # Use template response if available, otherwise auto-fill
                        if field_id in template_responses:
                            value = template_responses[field_id]
                            # Replace wildcards in the template response
                            value = self._replace_wildcards(value, suggestion_name, suggestion_number, message_link, issue_body, suggesting_user)
                        elif field_id == "suggestion-link":
                            value = message_link
                        elif field_id == "about-bounty":
                            value = issue_body
                        else:
                            value = ""

                        # Only add the field if it has content
                        if value.strip():
                            body_md += f"### {label}\n\n{value}\n\n"

                # Use template title if available, otherwise schema default, and replace wildcards
                title = template_responses.get("title", schema.get("title", suggestion_name))
                title = self._replace_wildcards(title, suggestion_name, suggestion_number, message_link, issue_body, suggesting_user)
                labels = schema.get("labels", [])

                created_issue: Any = repo.create_issue(
                    title=title,
                    body=body_md,
                    labels=labels
                )
                return created_issue.html_url
            else:
                # Fallback to simple format
                created_issue = repo.create_issue(
                    title=suggestion_name,
                    body=f"{suggestion_name}\n\n{issue_body}\n\n[View Suggestion]({message_link})"
                )
                return created_issue.html_url

        except Exception as e:
            self.log.error(f"Failed to create GitHub issue: {e}")
            return None

    @commands.group() # type: ignore
    @commands.admin_or_permissions(manage_guild=True)
    async def suggestbountyset(self, ctx: commands.Context):
        """
        Configuration commands for SuggestBounties.
        """
        pass

    @commands.command(name="bountyhelp")
    async def bounty_help(self, ctx: commands.Context):
        """
        Show help for all bounty-related commands.
        """
        embed = discord.Embed(
            title="SuggestBounties Commands",
            description="Commands for managing GitHub bounty creation from suggestions",
            color=await ctx.embed_color()
        )

        # Configuration commands
        embed.add_field(
            name="🔧 Configuration",
            value=(
                "`suggestbountyset repo <owner/repo>` - Set GitHub repository\n"
                "`suggestbountyset token <token>` - Set GitHub token\n"
                "`suggestbountyset channel <#channel>` - Set suggestion channel\n"
                "`suggestbountyset schema` - Upload GitHub issue template\n"
                "`suggestbountyset show` - Show current configuration\n"
                "`suggestbountyset reasonflag [keyword]` - Set/clear reason requirement\n"
                "`suggestbountyset toggle` - Toggle auto-processing"
            ),
            inline=False
        )

        # Retry and status commands
        embed.add_field(
            name="🔄 Retry & Status",
            value=(
                "`retrybounty [number]` - Retry failed bounty creation\n"
                "`retryallbounties` - Retry all failed bounties at once\n"
                "`bountystatus [number]` - Check bounty creation status\n"
                "`clearbountytracking [number]` - Clear tracking data\n"
                "`syncbounties` - Sync existing bounties from channel reactions"
            ),
            inline=False
        )

        embed.add_field(
            name="📋 Usage Examples",
            value=(
                "`retrybounty` - List all failed suggestions\n"
                "`retrybounty 123` - Retry suggestion #123\n"
                "`bountystatus` - Show overview of all suggestions\n"
                "`bountystatus 123` - Show status of suggestion #123"
            ),
            inline=False
        )

        await ctx.send(embed=embed)

    @suggestbountyset.command()
    async def repo(self, ctx: commands.Context, repo: str):
        """
        Set the GitHub repository to use for bounties. Format: owner/repo
        """
        if not ctx.guild:
            await ctx.send("This command must be used in a guild.")
            return
        if not re.match(r"^[\w.-]+\/[\w.-]+$", repo):
            await ctx.send("❌ Invalid repository format. Use `owner/repo`.")
            return
        await self.config.guild(ctx.guild).github_repo.set(repo)
        await ctx.send(f"✅ GitHub repository set to `{repo}`.")
        await ctx.tick()

    @suggestbountyset.command()
    async def token(self, ctx: commands.Context, token: str):
        """
        Set the GitHub personal access token for issue creation.
        """
        if not ctx.guild:
            await ctx.send("This command must be used in a guild.")
            try:
                await ctx.message.delete()
            except Exception:
                pass
            return
        if not (len(token) >= 40 and re.match(r"^[a-zA-Z0-9_\-]+$", token)):
            await ctx.send("❌ Invalid token format. Please check your token.")
            try:
                await ctx.message.delete()
            except Exception:
                pass
            return
        # Validate the token using PyGithub
        try:
            gh = Github(token)
            user = gh.get_user()
            _ = user.login  # This will raise if the token is invalid
        except GithubException:
            await ctx.send("❌ Token validation failed. Please check your token.")
            try:
                await ctx.message.delete()
            except Exception:
                pass
            return
        except Exception:
            await ctx.send("❌ An error occurred while validating the token.")
            try:
                await ctx.message.delete()
            except Exception:
                pass
            return
        await self.config.guild(ctx.guild).github_token.set(token)
        await ctx.send("✅ GitHub token set and validated.")
        await ctx.tick()
        # Attempt to delete the invoking message to avoid leaking sensitive data
        try:
            await ctx.message.delete()
        except Exception:
            pass

    @suggestbountyset.command(name="channel")
    async def suggestion_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """
        Set the channel for suggestions to be tracked as bounties.
        """
        if not ctx.guild:
            await ctx.send("This command must be used in a guild.")
            return
        await self.config.guild(ctx.guild).suggestion_channel.set(channel.id)
        await ctx.send(f"✅ Suggestion channel set to {channel.mention}.")
        await ctx.tick()

    @suggestbountyset.command()
    async def schema(self, ctx: commands.Context):
        """
        Upload a GitHub issue template schema (YAML) and configure the template format.
        """
        await ctx.send("Please upload your GitHub issue template YAML file.")

        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel and m.attachments

        try:
            msg = await ctx.bot.wait_for("message", check=check, timeout=120)
        except Exception as e:
            await ctx.send("Timed out waiting for a file upload.")
            return
        attachment = msg.attachments[0]
        if not attachment.filename.endswith(('.yml', '.yaml')):
            await ctx.send("File must be a .yml or .yaml file.")
            return
        content = await attachment.read()
        try:
            schema = yaml.safe_load(content)
        except Exception as e:
            await ctx.send(f"Failed to parse YAML: {e}")
            return
        # Prompt user to configure the template using the schema
        async def on_confirm(responses, interaction):
            if not ctx.guild:
                await interaction.response.send_message("This command must be used in a guild.", ephemeral=True)
                return
            await self.config.guild(ctx.guild).github_schema.set(content.decode())
            await self.config.guild(ctx.guild).github_template.set(str(responses))
            await interaction.response.send_message("Template and schema saved!", ephemeral=True)

            # Edit the original message to show completion and remove buttons
            try:
                # Clear the view to remove buttons and stop the timer
                view.clear_items()
                view.stop()

                success_message = (
                    "✅ **Template and schema saved successfully!**\n\n"
                    "Your GitHub issue template has been configured and is ready to use. "
                    "When approved suggestions are detected, they will automatically be converted to GitHub issues using your template."
                )

                await message.edit(content=success_message, view=view)
            except Exception as e:
                pass

            try:
                await ctx.tick()
            except Exception as e:
                pass
        async def on_cancel():
            # Edit the original message to show cancellation and remove buttons
            try:
                view.clear_items()
                view.stop()

                cancel_message = (
                    "❌ **Template configuration cancelled.**\n\n"
                    "No changes were made to your configuration. "
                    "Run the command again if you want to set up the template."
                )

                await message.edit(content=cancel_message, view=view)
            except Exception as e:
                pass

        # Create the instruction message with wildcard information
        instructions = (
            "Configure your template using the form that matches your GitHub schema.\n\n"
            "**Available wildcards - use these in any field and they'll be replaced when creating issues:**\n"
            "```\n"
            "{suggestion_name}    - The full suggestion message (e.g., 'Suggestion #123')\n"
            "{suggestion_number}  - Just the number (e.g., '123')\n"
            "{message_link}       - Discord message URL\n"
            "{issue_body}         - Formatted suggestion content with reason and results\n"
            "{suggesting_user}    - The user who made the suggestion\n"
            "```\n"
            "**Example:** `Bounty for {suggestion_name} - See {message_link}`\n"
            "**Note:** Fields like `suggestion-link` and `about-bounty` are auto-filled if not provided."
        )

        view = TemplateConfigView(schema, on_confirm, on_cancel, author_id=ctx.author.id)
        message = await ctx.send(instructions, view=view)
        view.message = message

    @suggestbountyset.command()
    async def show(self, ctx: commands.Context):
        """
        Show the current configuration for this guild.
        """
        if not ctx.guild:
            await ctx.send("This command must be used in a guild.")
            return
        data = await self.config.guild(ctx.guild).all()
        repo = data.get("github_repo") or "Not set"
        token = "Set" if data.get("github_token") else "Not set"
        channel_id = data.get("suggestion_channel")
        channel = ctx.guild.get_channel(channel_id) if channel_id else None
        channel_display = channel.mention if channel else "Not set"

        schema = data.get("github_schema")
        template = data.get("github_template")
        auto_enabled = data.get("auto_suggestions_enabled", True)
        reason_keyword = data.get("reason_keyword")

        embed = discord.Embed(title="SuggestBounties Configuration", color=await ctx.embed_color())
        embed.add_field(name="GitHub Repo", value=repo, inline=False)
        embed.add_field(name="GitHub Token", value=token, inline=False)
        embed.add_field(name="Suggestion Channel", value=channel_display, inline=False)
        embed.add_field(name="Auto Suggestions", value="✅ Enabled" if auto_enabled else "❌ Disabled", inline=False)
        embed.add_field(name="Reason Flag", value=reason_keyword or "Not set", inline=False)
        embed.add_field(name="GitHub Schema", value="Set (see attached file)" if schema else "Not set", inline=False)
        embed.add_field(name="GitHub Template", value="Set (see attached file)" if template else "Not set", inline=False)

        files = []
        if schema:
            schema_file = discord.File(
                fp=io.BytesIO(schema.encode('utf-8')),
                filename="github_schema.yml"
            )
            files.append(schema_file)

        if template:
            template_file = discord.File(
                fp=io.BytesIO(template.encode('utf-8')),
                filename="github_template.txt"
            )
            files.append(template_file)

        await ctx.send(embed=embed, files=files)
        await ctx.tick()

    @suggestbountyset.command(name="reasonflag")
    async def reasonflag(self, ctx: commands.Context, *, keyword: Optional[str] = None):
        """
        Set or clear the required keyword in the Reason field to allow sending to GitHub.

        - Provide a keyword (e.g. "[BOUNTY]") to require it in the Reason field (case-insensitive).
        - Call without a keyword to clear this requirement.
        """
        if not ctx.guild:
            await ctx.send("This command must be used in a guild.")
            return

        if keyword is None or not keyword.strip():
            await self.config.guild(ctx.guild).reason_keyword.clear()
            await ctx.send("✅ Reason flag cleared. All approved suggestions will be processed.")
            await ctx.tick()
            return

        value = keyword.strip()
        await self.config.guild(ctx.guild).reason_keyword.set(value)
        await ctx.send(f"✅ Reason flag set. Only approved suggestions with '{value}' in the Reason field will be processed.")
        await ctx.tick()

    @suggestbountyset.command()
    async def toggle(self, ctx: commands.Context):
        """
        Toggle automatic suggestion processing on or off.
        """
        if not ctx.guild:
            await ctx.send("This command must be used in a guild.")
            return

        current_state = await self.config.guild(ctx.guild).auto_suggestions_enabled()
        new_state = not current_state
        await self.config.guild(ctx.guild).auto_suggestions_enabled.set(new_state)

        status = "✅ enabled" if new_state else "❌ disabled"
        await ctx.send(f"Automatic suggestion processing is now {status}.")
        await ctx.tick()

    @commands.command(name="retrybounty")
    @commands.admin_or_permissions(manage_guild=True)
    async def retry_bounty(self, ctx: commands.Context, suggestion_number: Optional[int] = None):
        """
        Retry creating a GitHub bounty for a failed suggestion.

        Usage:
        - `[p]retrybounty` - List all failed suggestions that can be retried
        - `[p]retrybounty <number>` - Retry creating a bounty for suggestion #<number>
        """
        if not ctx.guild:
            await ctx.send("This command must be used in a guild.")
            return

        config = self.config.guild(ctx.guild)
        repo_name = await config.github_repo()
        token = await config.github_token()

        if not repo_name or not token:
            await ctx.send("❌ GitHub repository or token not configured. Please configure them first.")
            return

        suggestions = await self.config.custom("suggestions", ctx.guild.id).suggestions()

        if not suggestions:
            await ctx.send("No suggestions have been tracked yet.")
            return

        if suggestion_number is None:
            # List all failed suggestions
            failed_suggestions = []
            for msg_id, data in suggestions.items():
                if not data.get("github_issue_url"):
                    failed_suggestions.append((msg_id, data))

            if not failed_suggestions:
                await ctx.send("✅ All tracked suggestions have GitHub issues created successfully!")
                return

            embed = discord.Embed(
                title="Failed Suggestions - Ready for Retry",
                description="The following suggestions failed to create GitHub issues and can be retried:",
                color=await ctx.embed_color()
            )

            for msg_id, data in failed_suggestions[:10]:  # Limit to 10 for display
                embed.add_field(
                    name=f"Suggestion #{data['suggestion_number']}",
                    value=f"Status: {data['status']}\nMessage ID: {msg_id}",
                    inline=True
                )

            if len(failed_suggestions) > 10:
                embed.set_footer(text=f"And {len(failed_suggestions) - 10} more...")

            embed.add_field(
                name="How to Retry",
                value=f"Use `{ctx.prefix}retrybounty <number>` to retry a specific suggestion",
                inline=False
            )

            await ctx.send(embed=embed)
            return

        # Retry specific suggestion
        suggestion_found = None
        msg_id = None

        for mid, data in suggestions.items():
            if data.get("suggestion_number") == str(suggestion_number):
                suggestion_found = data
                msg_id = int(mid)
                break

        if not suggestion_found:
            await ctx.send(f"❌ Suggestion #{suggestion_number} not found in tracked suggestions.")
            return

        if suggestion_found.get("github_issue_url"):
            await ctx.send(f"✅ Suggestion #{suggestion_number} already has a GitHub issue: {suggestion_found['github_issue_url']}")
            return

        # Find the original message
        suggestion_channel_id = await config.suggestion_channel()
        if not suggestion_channel_id:
            await ctx.send("❌ Suggestion channel not configured.")
            return

        try:
            channel = ctx.guild.get_channel(suggestion_channel_id)
            if not channel or not isinstance(channel, discord.TextChannel):
                await ctx.send("❌ Suggestion channel not found.")
                return

            message = await channel.fetch_message(msg_id)
        except discord.NotFound:
            await ctx.send(f"❌ Original suggestion message not found (ID: {msg_id})")
            return
        except Exception as e:
            await ctx.send(f"❌ Error fetching message: {e}")
            return

        # Parse the message to extract suggestion details
        if not message.embeds:
            await ctx.send("❌ Message doesn't contain an embed.")
            return

        embed = message.embeds[0]
        if not embed.description:
            await ctx.send("❌ Embed doesn't contain suggestion description.")
            return

        suggestion_text = embed.description.strip()

        # Parse embed fields for Reason and Results
        reason_text = None
        results_text = None

        for field in embed.fields:
            if field.name == "Reason" and field.value:
                reason_text = field.value.strip()
            elif field.name == "Results" and field.value:
                results_text = field.value.strip()

        if not results_text:
            await ctx.send("❌ Embed doesn't contain Results field.")
            return

        # Extract suggesting user from embed footer
        suggesting_user = ""
        if embed.footer and embed.footer.text:
            footer_match = re.search(r"Suggested by (.+?) \(\d+\)", embed.footer.text)
            if footer_match:
                suggesting_user = footer_match.group(1)

        # Check if reason keyword is required
        reason_keyword = await config.reason_keyword()
        if reason_keyword:
            if not reason_text or reason_keyword.lower() not in reason_text.lower():
                await ctx.send(f"❌ Suggestion requires '{reason_keyword}' in the Reason field.")
                return

        # Try to create the GitHub issue
        await ctx.send(f"🔄 Attempting to create GitHub bounty for Suggestion #{suggestion_number}...")

        issue_url = await self._create_github_issue(
            ctx, message, suggestion_text, reason_text, results_text, suggesting_user
        )

        if issue_url:
            # Update the suggestion status
            await self._store_suggestion_status(
                ctx.guild.id, msg_id, str(suggestion_number), issue_url, "success"
            )

            # Update the original message reaction
            try:
                await message.clear_reactions()
                await message.add_reaction("✅")
            except Exception:
                pass

            await ctx.send(f"✅ Successfully created GitHub bounty for Suggestion #{suggestion_number}!\n{issue_url}")
        else:
            # Update status to failed
            await self._store_suggestion_status(
                ctx.guild.id, msg_id, str(suggestion_number), None, "failed"
            )
            await ctx.send(f"❌ Failed to create GitHub bounty for Suggestion #{suggestion_number}. Check the logs for details.")

    @commands.command(name="retryallbounties")
    @commands.admin_or_permissions(manage_guild=True)
    async def retry_all_bounties(self, ctx: commands.Context):
        """
        Retry creating GitHub bounties for all failed suggestions.

        This command will attempt to recreate GitHub issues for all suggestions that previously failed.
        """
        if not ctx.guild:
            await ctx.send("This command must be used in a guild.")
            return

        config = self.config.guild(ctx.guild)
        repo_name = await config.github_repo()
        token = await config.github_token()

        if not repo_name or not token:
            await ctx.send("❌ GitHub repository or token not configured. Please configure them first.")
            return

        suggestions = await self.config.custom("suggestions", ctx.guild.id).suggestions()

        if not suggestions:
            await ctx.send("No suggestions have been tracked yet.")
            return

        # Find all failed suggestions
        failed_suggestions = []
        for msg_id, data in suggestions.items():
            if not data.get("github_issue_url"):
                failed_suggestions.append((msg_id, data))

        if not failed_suggestions:
            await ctx.send("✅ All tracked suggestions have GitHub issues created successfully!")
            return

        await ctx.send(f"🔄 Attempting to retry {len(failed_suggestions)} failed suggestions...")

        success_count = 0
        failed_count = 0

        for msg_id, data in failed_suggestions:
            try:
                suggestion_number = data['suggestion_number']
                await ctx.send(f"🔄 Processing Suggestion #{suggestion_number}...")

                # Find the original message
                suggestion_channel_id = await config.suggestion_channel()
                if not suggestion_channel_id:
                    await ctx.send(f"❌ Suggestion channel not configured for Suggestion #{suggestion_number}")
                    failed_count += 1
                    continue

                try:
                    channel = ctx.guild.get_channel(suggestion_channel_id)
                    if not channel or not isinstance(channel, discord.TextChannel):
                        await ctx.send(f"❌ Suggestion channel not found for Suggestion #{suggestion_number}")
                        failed_count += 1
                        continue

                    message = await channel.fetch_message(int(msg_id))
                except discord.NotFound:
                    await ctx.send(f"❌ Original suggestion message not found for Suggestion #{suggestion_number}")
                    failed_count += 1
                    continue
                except Exception as e:
                    await ctx.send(f"❌ Error fetching message for Suggestion #{suggestion_number}: {e}")
                    failed_count += 1
                    continue

                # Parse the message to extract suggestion details
                if not message.embeds:
                    await ctx.send(f"❌ Message doesn't contain an embed for Suggestion #{suggestion_number}")
                    failed_count += 1
                    continue

                embed = message.embeds[0]
                if not embed.description:
                    await ctx.send(f"❌ Embed doesn't contain suggestion description for Suggestion #{suggestion_number}")
                    failed_count += 1
                    continue

                suggestion_text = embed.description.strip()

                # Parse embed fields for Reason and Results
                reason_text = None
                results_text = None

                for field in embed.fields:
                    if field.name == "Reason" and field.value:
                        reason_text = field.value.strip()
                    elif field.name == "Results" and field.value:
                        results_text = field.value.strip()

                if not results_text:
                    await ctx.send(f"❌ Embed doesn't contain Results field for Suggestion #{suggestion_number}")
                    failed_count += 1
                    continue

                # Extract suggesting user from embed footer
                suggesting_user = ""
                if embed.footer and embed.footer.text:
                    footer_match = re.search(r"Suggested by (.+?) \(\d+\)", embed.footer.text)
                    if footer_match:
                        suggesting_user = footer_match.group(1)

                # Check if reason keyword is required
                reason_keyword = await config.reason_keyword()
                if reason_keyword:
                    if not reason_text or reason_keyword.lower() not in reason_text.lower():
                        await ctx.send(f"❌ Suggestion #{suggestion_number} requires '{reason_keyword}' in the Reason field.")
                        failed_count += 1
                        continue

                # Try to create the GitHub issue
                issue_url = await self._create_github_issue(
                    ctx, message, suggestion_text, reason_text, results_text, suggesting_user
                )

                if issue_url:
                    # Update the suggestion status
                    await self._store_suggestion_status(
                        ctx.guild.id, int(msg_id), suggestion_number, issue_url, "success"
                    )

                    # Update the original message reaction
                    try:
                        await message.clear_reactions()
                        await message.add_reaction("✅")
                    except Exception:
                        pass

                    await ctx.send(f"✅ Successfully created GitHub bounty for Suggestion #{suggestion_number}!")
                    success_count += 1
                else:
                    # Update status to failed
                    await self._store_suggestion_status(
                        ctx.guild.id, int(msg_id), suggestion_number, None, "failed"
                    )
                    await ctx.send(f"❌ Failed to create GitHub bounty for Suggestion #{suggestion_number}")
                    failed_count += 1

            except Exception as e:
                await ctx.send(f"❌ Error processing Suggestion #{data.get('suggestion_number', 'unknown')}: {e}")
                failed_count += 1

        # Final summary
        embed = discord.Embed(
            title="Bulk Retry Complete",
            color=await ctx.embed_color()
        )
        embed.add_field(name="✅ Successful", value=str(success_count), inline=True)
        embed.add_field(name="❌ Failed", value=str(failed_count), inline=True)
        embed.add_field(name="Total Processed", value=str(len(failed_suggestions)), inline=True)

        if failed_count > 0:
            embed.add_field(
                name="Note",
                value="Failed suggestions can be retried individually using `retrybounty <number>`",
                inline=False
            )

        await ctx.send(embed=embed)

    @commands.command(name="bountystatus")
    @commands.admin_or_permissions(manage_guild=True)
    async def bounty_status(self, ctx: commands.Context, suggestion_number: Optional[int] = None):
        """
        Check the status of suggestions and their GitHub bounty creation.

        Usage:
        - `[p]bountystatus` - Show overview of all tracked suggestions
        - `[p]bountystatus <number>` - Show detailed status for suggestion #<number>
        """
        if not ctx.guild:
            await ctx.send("This command must be used in a guild.")
            return

        suggestions = await self.config.custom("suggestions", ctx.guild.id).suggestions()

        if not suggestions:
            await ctx.send("No suggestions have been tracked yet.")
            return

        if suggestion_number is None:
            # Show overview
            total = len(suggestions)
            successful = sum(1 for data in suggestions.values() if data.get("github_issue_url"))
            failed = total - successful

            embed = discord.Embed(
                title="Bounty Status Overview",
                color=await ctx.embed_color()
            )
            embed.add_field(name="Total Tracked", value=str(total), inline=True)
            embed.add_field(name="✅ Successful", value=str(successful), inline=True)
            embed.add_field(name="❌ Failed", value=str(failed), inline=True)

            if failed > 0:
                embed.add_field(
                    name="Retry Failed",
                    value=f"Use `{ctx.prefix}retrybounty` to retry individual suggestions or `{ctx.prefix}retryallbounties` to retry all at once",
                    inline=False
                )

            await ctx.send(embed=embed)
            return

        # Show specific suggestion status
        suggestion_found = None
        for data in suggestions.values():
            if data.get("suggestion_number") == str(suggestion_number):
                suggestion_found = data
                break

        if not suggestion_found:
            await ctx.send(f"❌ Suggestion #{suggestion_number} not found in tracked suggestions.")
            return

        embed = discord.Embed(
            title=f"Bounty Status - Suggestion #{suggestion_number}",
            color=await ctx.embed_color()
        )

        embed.add_field(name="Status", value=suggestion_found.get("status", "unknown"), inline=True)
        embed.add_field(name="Created At", value=suggestion_found.get("created_at", "unknown"), inline=True)

        if suggestion_found.get("github_issue_url"):
            embed.add_field(name="GitHub Issue", value=suggestion_found["github_issue_url"], inline=False)
            embed.colour = discord.Color.green()
        else:
            embed.add_field(name="GitHub Issue", value="❌ Not created", inline=False)
            embed.colour = discord.Color.red()

        await ctx.send(embed=embed)

    @commands.command(name="clearbountytracking")
    @commands.admin_or_permissions(manage_guild=True)
    async def clear_bounty_tracking(self, ctx: commands.Context, suggestion_number: Optional[int] = None):
        """
        Clear tracking data for suggestions.

        Usage:
        - `[p]clearbountytracking` - Clear all tracking data (use with caution!)
        - `[p]clearbountytracking <number>` - Clear tracking data for a specific suggestion
        """
        if not ctx.guild:
            await ctx.send("This command must be used in a guild.")
            return

        if suggestion_number is None:
            # Clear all tracking data
            confirm_embed = discord.Embed(
                title="⚠️ Clear All Bounty Tracking?",
                description="This will remove tracking data for **ALL** suggestions. This action cannot be undone!",
                color=discord.Color.red()
            )
            confirm_embed.add_field(
                name="Confirmation Required",
                value="Type `CONFIRM` to proceed with clearing all tracking data.",
                inline=False
            )

            await ctx.send(embed=confirm_embed)

            def check(m):
                return m.author == ctx.author and m.channel == ctx.channel and m.content == "CONFIRM"

            try:
                await ctx.bot.wait_for("message", check=check, timeout=30)
            except Exception:
                await ctx.send("❌ Confirmation timed out. No data was cleared.")
                return

            # Clear all data
            async with self.config.custom("suggestions", ctx.guild.id).suggestions() as suggestions:
                suggestions.clear()

            await ctx.send("✅ All bounty tracking data has been cleared.")
            return

        # Clear specific suggestion
        suggestions = await self.config.custom("suggestions", ctx.guild.id).suggestions()

        suggestion_found = None
        msg_id = None

        for mid, data in suggestions.items():
            if data.get("suggestion_number") == str(suggestion_number):
                suggestion_found = data
                msg_id = mid
                break

        if not suggestion_found:
            await ctx.send(f"❌ Suggestion #{suggestion_number} not found in tracking data.")
            return

        # Remove the specific suggestion
        async with self.config.custom("suggestions", ctx.guild.id).suggestions() as suggestions:
            suggestions.pop(msg_id, None)

        await ctx.send(f"✅ Tracking data for Suggestion #{suggestion_number} has been cleared.")

    @commands.command(name="syncbounties")
    @commands.admin_or_permissions(manage_guild=True)
    async def sync_bounties(self, ctx: commands.Context):
        """
        Manually sync existing bounties by checking bot reactions on suggestion posts.

        This command scans the suggestion channel for existing posts and checks if the bot
        has already reacted with ✅ (success) or ❌ (failed) to determine their status.
        """
        if not ctx.guild:
            await ctx.send("This command must be used in a guild.")
            return

        config = self.config.guild(ctx.guild)
        suggestion_channel_id = await config.suggestion_channel()

        if not suggestion_channel_id:
            await ctx.send("❌ Suggestion channel not configured. Please configure it first.")
            return

        channel = ctx.guild.get_channel(suggestion_channel_id)
        if not channel:
            await ctx.send("❌ Suggestion channel not found.")
            return

        await ctx.send(f"🔄 Scanning {channel.mention} for existing bounty posts...")

        # Get existing tracked suggestions
        existing_suggestions = await self.config.custom("suggestions", ctx.guild.id).suggestions()
        existing_message_ids = set(existing_suggestions.keys())

        # Scan channel for suggestion posts
        scanned_count = 0
        synced_count = 0
        new_tracked_count = 0

        try:
            # Fetch messages from the channel (limit to last 100 to avoid rate limits)
            if not isinstance(channel, discord.TextChannel):
                await ctx.send("❌ Configured suggestion channel is not a text channel.")
                return
            async for message in channel.history(limit=100):
                # Check if this is a suggestion post
                if not message.content.startswith("Suggestion #"):
                    continue

                scanned_count += 1
                message_id_str = str(message.id)

                # Skip if already tracked
                if message_id_str in existing_message_ids:
                    continue

                # Check if this is an approved suggestion
                if not message.embeds or not message.embeds[0].title == "Approved Suggestion":
                    continue

                # Extract suggestion number
                suggestion_match = re.search(r"#(\d+)", message.content)
                if not suggestion_match:
                    continue

                suggestion_number = suggestion_match.group(1)

                # Check bot reactions to determine status
                bot_reactions = []
                for reaction in message.reactions:
                    if reaction.emoji in ["✅", "❌"]:
                        # Check if the bot reacted with this emoji
                        async for user in reaction.users():
                            if user.id == self.bot.user.id:
                                bot_reactions.append(str(reaction.emoji))
                                break

                # Determine status based on reactions
                status = "unknown"
                github_issue_url = None

                if "✅" in bot_reactions:
                    status = "success"
                    # Try to find corresponding GitHub issue by searching the repository
                    github_issue_url = await self._find_existing_github_issue(ctx, suggestion_number, message.content)
                elif "❌" in bot_reactions:
                    status = "failed"
                else:
                    # No reaction, assume pending
                    status = "pending"

                # Store the suggestion status
                await self._store_suggestion_status(
                    ctx.guild.id, message.id, suggestion_number, github_issue_url, status
                )

                new_tracked_count += 1

                # Update progress every 10 messages
                if new_tracked_count % 10 == 0:
                    await ctx.send(f"🔄 Scanned {scanned_count} messages, tracked {new_tracked_count} new suggestions...")

        except Exception as e:
            await ctx.send(f"❌ Error during scanning: {e}")
            return

        # Final summary
        embed = discord.Embed(
            title="Bounty Sync Complete",
            color=await ctx.embed_color()
        )
        embed.add_field(name="📊 Scan Results", value=f"Messages scanned: {scanned_count}", inline=True)
        embed.add_field(name="🆕 Newly Tracked", value=str(new_tracked_count), inline=True)
        embed.add_field(name="📈 Total Tracked", value=str(len(await self.config.custom("suggestions", ctx.guild.id).suggestions())), inline=True)

        if new_tracked_count > 0:
            embed.add_field(
                name="✅ Next Steps",
                value=f"Use `{ctx.prefix}bountystatus` to view all tracked suggestions or `{ctx.prefix}retrybounty` to retry failed ones",
                inline=False
            )
        else:
            embed.add_field(
                name="ℹ️ Note",
                value="No new suggestions were found to track. All existing posts may already be tracked.",
                inline=False
            )

        await ctx.send(embed=embed)

    async def _find_existing_github_issue(self, ctx: commands.Context, suggestion_number: str, suggestion_title: str) -> Optional[str]:
        """
        Try to find an existing GitHub issue that corresponds to a suggestion.
        This is a best-effort attempt to match suggestions with existing issues.
        """
        if not ctx.guild:
            return None

        config = self.config.guild(ctx.guild)
        repo_name = await config.github_repo()
        token = await config.github_token()

        if not repo_name or not token:
            return None

        try:
            gh = Github(token)
            repo = gh.get_repo(repo_name)

            # Search for issues that might match this suggestion
            # Look for issues with similar titles or containing the suggestion number
            search_queries = [
                f"repo:{repo_name} {suggestion_title}",
                f"repo:{repo_name} suggestion #{suggestion_number}",
                f"repo:{repo_name} bounty #{suggestion_number}",
                f"repo:{repo_name} {suggestion_number}"
            ]

            # Predeclare loop variable types for strict linters
            gh_issue: Any
            for query in search_queries:
                try:
                    issues = gh.search_issues(query=query, state="all")
                    for gh_issue in issues[:5]:  # type: ignore[misc]
                        # Check if this issue seems to match our suggestion
                        if self._issue_matches_suggestion(gh_issue, suggestion_number, suggestion_title):  # type: ignore[type-arg]
                            return gh_issue.html_url
                except Exception:
                    continue

        except Exception as e:
            self.log.error(f"Error searching for existing GitHub issue: {e}")

        return None

    def _issue_matches_suggestion(self, issue, suggestion_number: str, suggestion_title: str) -> bool:
        """
        Determine if a GitHub issue matches a Discord suggestion.
        This uses heuristics to make a best guess match.
        """
        # Check if the issue title contains the suggestion number
        if suggestion_number in issue.title:
            return True

        # Check if the issue body contains the suggestion number
        if issue.body and suggestion_number in issue.body:
            return True

        # Check if the issue title is similar to the suggestion title
        suggestion_words = set(suggestion_title.lower().split())
        issue_words = set(issue.title.lower().split())

        # If more than 50% of words match, consider it a match
        if len(suggestion_words.intersection(issue_words)) / len(suggestion_words) > 0.5:
            return True

        return False

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild:
            return
        config = self.config.guild(message.guild)

        # Check if automatic suggestions are enabled
        auto_enabled = await config.auto_suggestions_enabled()
        if not auto_enabled:
            return

        suggestion_channel_id = await config.suggestion_channel()
        if not suggestion_channel_id or message.channel.id != suggestion_channel_id:
            return
        if not message.embeds:
            return
        embed = message.embeds[0]
        if not embed.description:
            return
        if not message.content.startswith("Suggestion #"):
            return
        # Use embed title for status, and embed description for suggestion text
        if not embed.title or embed.title != "Approved Suggestion":
            return
        suggestion_text = embed.description.strip() if embed.description else ""

        # Parse embed fields for Reason and Results
        reason_text = None
        results_text = None

        for field in embed.fields:
            if field.name == "Reason" and field.value:
                reason_text = field.value.strip()
            elif field.name == "Results" and field.value:
                results_text = field.value.strip()

        if not results_text:
            return

        # If a reason keyword is configured, ensure it exists in the Reason field (case-insensitive)
        reason_keyword = await config.reason_keyword()
        if reason_keyword:
            if not reason_text or reason_keyword.lower() not in reason_text.lower():
                return

        # Extract suggesting user from embed footer
        suggesting_user = ""
        if embed.footer and embed.footer.text:
            # Footer format: "Suggested by username (ID)"
            footer_match = re.search(r"Suggested by (.+?) \(\d+\)", embed.footer.text)
            if footer_match:
                suggesting_user = footer_match.group(1)

        # Extract suggestion number
        suggestion_number_match = re.search(r"#(\d+)", message.content)
        suggestion_number = suggestion_number_match.group(1) if suggestion_number_match else ""

        # Store initial suggestion status
        await self._store_suggestion_status(message.guild.id, message.id, suggestion_number, None, "pending")

        # Try to create GitHub issue
        issue_url = await self._create_github_issue(
            ctx=None, message=message, suggestion_text=suggestion_text, reason_text=reason_text, results_text=results_text, suggesting_user=suggesting_user
        )

        if issue_url:
            # Update status to success
            await self._store_suggestion_status(
                message.guild.id, message.id, suggestion_number, issue_url, "success"
            )
            await message.add_reaction("✅")
            self.log.info(f"Successfully created GitHub issue {issue_url}")
        else:
            # Update status to failed
            await self._store_suggestion_status(
                message.guild.id, message.id, suggestion_number, None, "failed"
            )
            await message.add_reaction("❌")
            self.log.error(f"Failed to create GitHub issue for suggestion #{suggestion_number}")
