from redbot.core import commands, Config
import discord
from typing import Optional, List, Dict, Any
import re
from github import Github, GithubException
import yaml
import logging
import io

class TemplateConfigView(discord.ui.View):
    def __init__(self, schema, on_confirm, on_cancel, message=None):
        super().__init__(timeout=60)  # 1 minute timeout
        self.schema = schema
        self.on_confirm = on_confirm
        self.on_cancel = on_cancel
        self.message = message
        self.timed_out = False

    async def on_timeout(self):
        """Called when the view times out"""
        self.timed_out = True
        # Disable all buttons
        for item in self.children:
            if hasattr(item, 'disabled'):
                item.disabled = True  # type: ignore
        
        if self.message:
            try:
                await self.message.edit(
                    content="⏰ Template configuration timed out. Please run the command again.",
                    view=self
                )
            except discord.NotFound:
                # Message was deleted
                pass
            except Exception:
                # If editing fails, try to send a new message
                try:
                    await self.message.channel.send("⏰ Template configuration timed out. Please run the command again.")
                except Exception:
                    pass

    @discord.ui.button(label="Configure Template", style=discord.ButtonStyle.green)
    async def submit(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.timed_out:
            await interaction.response.send_message("This configuration has timed out. Please run the command again.", ephemeral=True)
            return
            
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
        if self.timed_out:
            await interaction.response.send_message("This configuration has timed out. Please run the command again.", ephemeral=True)
            return
            
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
        self.responses = {}
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
        )
        self.log = logging.getLogger(f"red.{__name__}")
        # Placeholder for any startup logic, such as loading cache or setting up background tasks

    def _replace_wildcards(self, text: str, suggestion_name: str, suggestion_number: str, message_link: str, issue_body: str) -> str:
        """Replace wildcards in template text with actual values"""
        if not text:
            return text
        
        replacements = {
            "{suggestion_name}": suggestion_name,
            "{suggestion_number}": suggestion_number,
            "{message_link}": message_link,
            "{issue_body}": issue_body,
        }
        
        result = text
        for wildcard, value in replacements.items():
            result = result.replace(wildcard, value)
        
        return result

    @commands.group() # type: ignore
    @commands.admin_or_permissions(manage_guild=True)
    async def suggestbountyset(self, ctx: commands.Context):
        """
        Configuration commands for SuggestBounties.
        """
        if ctx.invoked_subcommand is None:
            return await ctx.send_help()

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
            return
        if not (len(token) >= 40 and re.match(r"^[a-zA-Z0-9_\-]+$", token)):
            await ctx.send("❌ Invalid token format. Please check your token.")
            return
        # Validate the token using PyGithub
        try:
            gh = Github(token)
            user = gh.get_user()
            _ = user.login  # This will raise if the token is invalid
        except GithubException:
            await ctx.send("❌ Token validation failed. Please check your token.")
            return
        except Exception:
            await ctx.send("❌ An error occurred while validating the token.")
            return
        await self.config.guild(ctx.guild).github_token.set(token)
        await ctx.send("✅ GitHub token set and validated.")
        await ctx.tick()

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
        self.log.info(f"[{ctx.guild}] Starting schema upload process for {ctx.author}")
        await ctx.send("Please upload your GitHub issue template YAML file.")

        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel and m.attachments

        try:
            msg = await ctx.bot.wait_for("message", check=check, timeout=120)
        except Exception as e:
            self.log.error(f"[{ctx.guild}] Timed out waiting for schema file upload: {e}")
            await ctx.send("Timed out waiting for a file upload.")
            return
        attachment = msg.attachments[0]
        if not attachment.filename.endswith(('.yml', '.yaml')):
            self.log.error(f"[{ctx.guild}] Uploaded file is not a YAML file: {attachment.filename}")
            await ctx.send("File must be a .yml or .yaml file.")
            return
        content = await attachment.read()
        try:
            schema = yaml.safe_load(content)
            self.log.info(f"[{ctx.guild}] YAML schema parsed successfully.")
        except Exception as e:
            self.log.error(f"[{ctx.guild}] Failed to parse YAML: {e}")
            await ctx.send(f"Failed to parse YAML: {e}")
            return
        # Prompt user to configure the template using the schema
        async def on_confirm(responses, interaction):
            if not ctx.guild:
                self.log.error(f"[No Guild] Tried to save schema/template without a guild context.")
                await interaction.response.send_message("This command must be used in a guild.", ephemeral=True)
                return
            self.log.info(f"[{ctx.guild}] Saving schema and template responses for {ctx.author}")
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
                self.log.error(f"[{ctx.guild}] Could not edit message after schema save: {e}")
            
            try:
                await ctx.tick()
            except Exception as e:
                self.log.error(f"[{ctx.guild}] Could not tick after schema save: {e}")
        async def on_cancel():
            self.log.info(f"[{ctx.guild}] Schema/template setup cancelled by {ctx.author}")
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
                self.log.error(f"[{ctx.guild}] Could not edit message after cancellation: {e}")
        
        # Create the instruction message with wildcard information
        instructions = (
            "Configure your template using the form that matches your GitHub schema.\n\n"
            "**Available wildcards - use these in any field and they'll be replaced when creating issues:**\n"
            "```\n"
            "{suggestion_name}    - The full suggestion message (e.g., 'Suggestion #123')\n"
            "{suggestion_number}  - Just the number (e.g., '123')\n"
            "{message_link}       - Discord message URL\n"
            "{issue_body}         - Formatted suggestion content with reason and results\n"
            "```\n"
            "**Example:** `Bounty for {suggestion_name} - See {message_link}`\n"
            "**Note:** Fields like `suggestion-link` and `about-bounty` are auto-filled if not provided.\n"
            "⏰ This configuration will timeout in 1 minute."
        )
        
        view = TemplateConfigView(schema, on_confirm, on_cancel)
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
        embed = discord.Embed(title="SuggestBounties Configuration", color=await ctx.embed_color())
        embed.add_field(name="GitHub Repo", value=repo, inline=False)
        embed.add_field(name="GitHub Token", value=token, inline=False)
        embed.add_field(name="Suggestion Channel", value=channel_display, inline=False)
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

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild:
            return self.log.warning(f"[{message.guild}] Message {message.id} not in a guild, skipping.")
        config = self.config.guild(message.guild)
        suggestion_channel_id = await config.suggestion_channel()
        if not suggestion_channel_id or message.channel.id != suggestion_channel_id:
            return self.log.warning(f"[{message.guild}] Message {message.id} not in suggestion channel, skipping.")
        if not message.embeds:
            return self.log.warning(f"[{message.guild}] Message {message.id} has no embeds, skipping.")
        embed = message.embeds[0]
        if not embed.description:
            return self.log.warning(f"[{message.guild}] Message {message.id} has no description, skipping.")
        if not message.content.startswith("Suggestion #"):
            return self.log.warning(f"[{message.guild}] Message {message.id} does not start with 'Suggestion #', skipping.")
        # Use embed title for status, and embed description for suggestion text
        if not embed.title or embed.title != "Approved Suggestion":
            return self.log.warning(f"[{message.guild}] Embed title is not 'Approved Suggestion' in message {message.id}, skipping.")
        suggestion_text = embed.description.strip() if embed.description else ""
        self.log.info(f"[{message.guild}] Found 'Approved Suggestion' embed with description: {suggestion_text}")
        
        # Parse embed fields for Reason and Results
        reason_text = None
        results_text = None
        
        for field in embed.fields:
            if field.name == "Reason" and field.value:
                reason_text = field.value.strip()
                self.log.info(f"[{message.guild}] Found 'Reason' field: {reason_text}")
            elif field.name == "Results" and field.value:
                results_text = field.value.strip()
                self.log.info(f"[{message.guild}] Found 'Results' field: {results_text}")
        
        if not results_text:
            return self.log.warning(f"[{message.guild}] 'Results' field not found in message {message.id}, skipping.")
        # Compose issue body
        issue_body = f"**Suggestion:**\n{suggestion_text}\n\n"
        if reason_text:
            issue_body += f"**Reason:**\n{reason_text}\n\n"
        issue_body += f"**Results:**\n{results_text}\n"
        repo_name = await config.github_repo()
        token = await config.github_token()
        schema_yaml = await config.github_schema()
        schema = yaml.safe_load(schema_yaml) if schema_yaml else None
        template = await config.github_template()
        suggestion_name = message.content
        suggestion_number = re.search(r"#(\d+)", message.content)
        suggestion_number = suggestion_number.group(1) if suggestion_number else ""
        message_link = message.jump_url
        if schema and template:
            self.log.info(f"[{message.guild}] Creating GitHub issue using schema template for message {message.id}")
            try:
                # Parse the stored template responses
                import ast
                template_responses = ast.literal_eval(template)
            except Exception as e:
                self.log.error(f"[{message.guild}] Error parsing template responses: {e}")
                template_responses = {}
            
            # Build issue body from schema, using template responses and auto-filled values
            # Only include textarea and input fields, skip markdown sections
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
                        value = self._replace_wildcards(value, suggestion_name, suggestion_number, message_link, issue_body)
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
            title = self._replace_wildcards(title, suggestion_name, suggestion_number, message_link, issue_body)
            labels = schema.get("labels", [])
            
            try:
                self.log.info(f"[{message.guild}] Connecting to GitHub repo {repo_name} to create issue.")
                gh = Github(token)
                repo = gh.get_repo(repo_name)
                issue = repo.create_issue(
                    title=title,
                    body=body_md,
                    labels=labels
                )
                self.log.info(f"[{message.guild}] Issue created: {issue.html_url}")
                await message.add_reaction("✅")
            except Exception as e:
                self.log.error(f"[{message.guild}] Failed to create GitHub issue: {e}")
                await message.add_reaction("❌")
            return
        try:
            self.log.info(f"[{message.guild}] Creating GitHub issue using fallback for message {message.id}")
            gh = Github(token)
            repo = gh.get_repo(repo_name)
            issue = repo.create_issue(
                title=suggestion_name,
                body=f"{suggestion_name}\n\n{issue_body}\n\n[View Suggestion]({message_link})"
            )
            self.log.info(f"[{message.guild}] Issue created: {issue.html_url}")
            await message.add_reaction("✅")
        except Exception as e:
            self.log.error(f"[{message.guild}] Failed to create fallback GitHub issue: {e}")
            await message.add_reaction("❌")

    # Placeholder: Add listeners, commands, and background tasks here

    # Placeholder: Future methods will use PyGithub for all GitHub interactions
