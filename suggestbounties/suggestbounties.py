from redbot.core import commands, Config
import discord
from typing import Optional, List, Dict, Any
import re
from github import Github, GithubException
import yaml

class TemplateConfigView(discord.ui.View):
    def __init__(self, schema_fields, on_confirm, on_cancel):
        super().__init__(timeout=300)
        self.schema_fields = schema_fields
        self.on_confirm = on_confirm
        self.on_cancel = on_cancel
        self.template_input = None

    @discord.ui.button(label="Submit Template", style=discord.ButtonStyle.green)
    async def submit(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = TemplateInputModal(self.schema_fields, self.on_confirm)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("Template configuration cancelled.", ephemeral=True)
        await self.on_cancel()

class TemplateInputModal(discord.ui.Modal):
    def __init__(self, schema_fields, on_confirm):
        super().__init__(title="Configure Template Format")
        self.schema_fields = schema_fields
        self.on_confirm = on_confirm
        self.template = discord.ui.TextInput(
            label="Template Format",
            style=discord.TextStyle.paragraph,
            placeholder="Enter your template using {issue_body}, {suggestion_name}, {suggestion_number}, {message_link}",
            required=True,
            max_length=2000
        )
        self.add_item(self.template)

    async def on_submit(self, interaction: discord.Interaction):
        await self.on_confirm(self.template.value, interaction)

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
        # Parse schema body
        for item in schema.get("body", []):
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
        # If less than 1 field, add a dummy so modal is valid
        if not self.field_map:
            self.add_item(discord.ui.TextInput(label="(No fields)", required=False, style=discord.TextStyle.short, custom_id="dummy"))

    async def on_submit(self, interaction: discord.Interaction):
        for child in self.children:
            if hasattr(child, 'custom_id'):
                self.responses[child.custom_id] = child.value  # type: ignore
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
        # Placeholder for any startup logic, such as loading cache or setting up background tasks

    @commands.group() # type: ignore
    @commands.admin_or_permissions(manage_guild=True)
    async def suggestbountyset(self, ctx: commands.Context):
        """
        Configuration commands for SuggestBounties.
        """
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

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
        except Exception:
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
        # Extract fields from the schema (for form-based templates)
        schema_fields = []
        if 'body' in schema:
            for item in schema['body']:
                if 'id' in item:
                    schema_fields.append(item['id'])
        # Prompt user to enter a template format string
        async def on_confirm(template, interaction):
            if not ctx.guild:
                await interaction.response.send_message("This command must be used in a guild.", ephemeral=True)
                return
            await self.config.guild(ctx.guild).github_schema.set(content.decode())
            await self.config.guild(ctx.guild).github_template.set(template)
            await interaction.response.send_message("Template and schema saved!", ephemeral=True)
        async def on_cancel():
            pass
        view = TemplateConfigView(schema_fields, on_confirm, on_cancel)
        await ctx.send(
            "Now configure your template format. Use {issue_body}, {suggestion_name}, {suggestion_number}, {message_link} as placeholders.",
            view=view
        )

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
        schema = data.get("github_schema") or "Not set"
        template = data.get("github_template") or "Not set"
        embed = discord.Embed(title="SuggestBounties Configuration", color=await ctx.embed_color())
        embed.add_field(name="GitHub Repo", value=repo, inline=False)
        embed.add_field(name="GitHub Token", value=token, inline=False)
        embed.add_field(name="Suggestion Channel", value=channel_display, inline=False)
        embed.add_field(name="GitHub Schema", value=schema, inline=False)
        embed.add_field(name="GitHub Template", value=template, inline=False)
        await ctx.send(embed=embed)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        config = self.config.guild(message.guild)
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
        lines = embed.description.split("\n")
        try:
            approved_idx = lines.index("Approved Suggestion")
            reason_idx = lines.index("Reason")
            results_idx = lines.index("Results")
        except ValueError:
            return
        suggestion_text = lines[approved_idx + 1].strip() if approved_idx + 1 < len(lines) else ""
        reason_text = lines[reason_idx + 1].strip() if reason_idx + 1 < len(lines) else ""
        results_lines = []
        for i in range(results_idx + 1, len(lines)):
            if lines[i].startswith("Suggested by"):
                break
            results_lines.append(lines[i])
        results_text = "\n".join(results_lines).strip()
        issue_body = (
            f"**Suggestion:**\n{suggestion_text}\n\n"
            f"**Reason:**\n{reason_text}\n\n"
            f"**Results:**\n{results_text}\n"
        )
        repo_name = await config.github_repo()
        token = await config.github_token()
        schema_yaml = await config.github_schema()
        schema = yaml.safe_load(schema_yaml) if schema_yaml else None
        suggestion_name = message.content
        suggestion_number = re.search(r"#(\d+)", message.content)
        suggestion_number = suggestion_number.group(1) if suggestion_number else ""
        message_link = message.jump_url
        # If schema is set, prompt user to fill modal
        if schema:
            async def on_modal_submit(modal: SchemaModal, interaction: discord.Interaction):
                # Build issue body from schema, inserting responses
                body_md = ""
                for item in schema.get("body", []):
                    if item.get("type") == "markdown":
                        body_md += item["attributes"]["value"] + "\n\n"
                    elif item.get("type") in ("textarea", "input"):
                        field_id = item.get("id")
                        label = item["attributes"].get("label", field_id)
                        value = modal.responses.get(field_id, "")
                        body_md += f"### {label}\n{value}\n\n"
                # Compose title
                title = schema.get("title", suggestion_name)
                if "{suggestion_name}" in title:
                    title = title.replace("{suggestion_name}", suggestion_name)
                # Labels
                labels = schema.get("labels", [])
                try:
                    gh = Github(token)
                    repo = gh.get_repo(repo_name)
                    issue = repo.create_issue(
                        title=title,
                        body=body_md,
                        labels=labels
                    )
                    await message.add_reaction("✅")
                    await interaction.response.send_message(f"GitHub issue created: {issue.html_url}", ephemeral=True)
                except Exception as e:
                    await message.add_reaction("❌")
                    await interaction.response.send_message(f"Failed to create issue: {e}", ephemeral=True)
            # Show info text as ephemeral message before modal
            info_texts = []
            for item in schema.get("body", []):
                if item.get("type") == "markdown":
                    info_texts.append(item["attributes"].get("value", ""))
            if info_texts:
                await message.channel.send("\n".join(info_texts), reference=message, mention_author=False, delete_after=30)
            modal = SchemaModal(
                schema=schema,
                message_link=message_link,
                suggestion_name=suggestion_name,
                suggestion_number=suggestion_number,
                issue_body=issue_body,
                on_submit_callback=on_modal_submit
            )
            # Prompt the user who posted the suggestion to fill the modal
            try:
                await message.author.send("Please fill out the bounty submission form:")
                await message.author.send_modal(modal)  # type: ignore
            except Exception:
                await message.channel.send(f"{message.author.mention}, please enable DMs to fill out the bounty form.")
            return
        # Fallback: no schema, use default
        try:
            gh = Github(token)
            repo = gh.get_repo(repo_name)
            issue = repo.create_issue(
                title=suggestion_name,
                body=f"{suggestion_name}\n\n{issue_body}\n\n[View Suggestion]({message_link})"
            )
            await message.add_reaction("✅")
        except Exception as e:
            await message.add_reaction("❌")

    # Placeholder: Add listeners, commands, and background tasks here

    # Placeholder: Future methods will use PyGithub for all GitHub interactions
