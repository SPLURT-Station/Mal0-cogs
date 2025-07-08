import logging
import json
import discord
from discord.ext import commands
from redbot.core import commands, Config, checks
from redbot.core.bot import Red
from redbot.core.utils import chat_formatting
from redbot.core.utils.views import SimpleMenu

class SS13Verify(commands.Cog):
    """
    SS13 Discord <-> ckey verification and linking system.
    Handles ticket-based verification, role assignment, and database linking.
    """
    __author__ = "Mal0"
    __version__ = "0.1.0"

    def __init__(self, bot: Red):
        self.bot = bot
        self.log = logging.getLogger("red.ss13_verify")
        self.config = Config.get_conf(self, identifier=908039527271104514, force_registration=True)
        # Per-guild config
        default_guild = {
            "ticket_channel": None,  # Channel ID for ticket panel
            "ticket_category": None, # Category ID for ticket channels
            "panel_message": None,   # Message ID for the panel embed
            "panel_embed": {},       # JSON dict for the panel embed
            "ticket_embed": {},      # JSON dict for the ticket embed
            "verification_roles": [], # List of role IDs to assign on verification
            "db_host": None,
            "db_port": 3306,
            "db_user": None,
            "db_password": None,
            "db_name": None,
            "mysql_prefix": "",
        }
        self.config.register_guild(**default_guild)
        # Per-user config (for future use, e.g. to track open tickets)
        default_member = {
            "open_ticket": None,  # Channel ID of open ticket, if any
        }
        self.config.register_member(**default_member)

    def cog_unload(self):
        """Cleanup when cog is unloaded."""
        pass

    @commands.group()
    @checks.admin_or_permissions(manage_guild=True)
    async def ss13verify(self, ctx):
        """SS13 Verification system configuration."""
        pass

    @ss13verify.group()
    async def panel(self, ctx):
        """Configure the verification panel."""
        pass

    @panel.command(name="setchannel")
    async def set_panel_channel(self, ctx, channel: discord.TextChannel):
        """Set the channel for the verification panel."""
        embed_data = await self.config.guild(ctx.guild).panel_embed()
        if not embed_data:
            await ctx.send("❌ You must set up the panel embed first using `[p]ss13verify panel setembed`")
            return

        await self.config.guild(ctx.guild).ticket_channel.set(channel.id)
        await ctx.send(f"✅ Panel channel set to {channel.mention}")

        # Create and send the panel message
        await self.create_panel_message(ctx.guild, channel)

    @panel.command(name="setcategory")
    async def set_ticket_category(self, ctx, category: discord.CategoryChannel):
        """Set the category where verification tickets will be created."""
        await self.config.guild(ctx.guild).ticket_category.set(category.id)
        await ctx.send(f"✅ Ticket category set to {category.name}")

    @panel.command(name="setembed")
    async def set_panel_embed(self, ctx):
        """Set the embed for the verification panel using an attached JSON file."""
        if not ctx.message.attachments:
            await ctx.send("❌ Please attach a JSON file containing the embed data to this command message.")
            return
        attachment = ctx.message.attachments[0]
        if not attachment.filename.lower().endswith(".json"):
            await ctx.send("❌ The attached file must be a .json file.")
            return
        try:
            file_bytes = await attachment.read()
            file_text = file_bytes.decode("utf-8")
            embed_dict = json.loads(file_text)
            embed = discord.Embed.from_dict(embed_dict)
            await self.config.guild(ctx.guild).panel_embed.set(embed_dict)
            await ctx.send("✅ Panel embed set successfully!")
            await ctx.send("**Preview:**", embed=embed)
        except json.JSONDecodeError:
            await ctx.send("❌ Invalid JSON format in the attached file.")
        except Exception as e:
            await ctx.send(f"❌ Error creating embed: {str(e)}")

    @panel.command(name="setticketembed")
    async def set_ticket_embed(self, ctx):
        """Set the embed for the verification ticket using an attached JSON file."""
        if not ctx.message.attachments:
            await ctx.send("❌ Please attach a JSON file containing the embed data to this command message.")
            return
        attachment = ctx.message.attachments[0]
        if not attachment.filename.lower().endswith(".json"):
            await ctx.send("❌ The attached file must be a .json file.")
            return
        try:
            file_bytes = await attachment.read()
            file_text = file_bytes.decode("utf-8")
            embed_dict = json.loads(file_text)
            embed = discord.Embed.from_dict(embed_dict)
            await self.config.guild(ctx.guild).ticket_embed.set(embed_dict)
            await ctx.send("✅ Ticket embed set successfully!")
            await ctx.send("**Preview:**", embed=embed)
        except json.JSONDecodeError:
            await ctx.send("❌ Invalid JSON format in the attached file.")
        except Exception as e:
            await ctx.send(f"❌ Error creating embed: {str(e)}")

    @panel.command(name="create")
    async def create_panel(self, ctx):
        """Create the verification panel in the configured channel."""
        channel_id = await self.config.guild(ctx.guild).ticket_channel()
        embed_data = await self.config.guild(ctx.guild).panel_embed()

        if not channel_id:
            await ctx.send("❌ Panel channel not set. Use `[p]ss13verify panel setchannel` first.")
            return

        if not embed_data:
            await ctx.send("❌ Panel embed not set. Use `[p]ss13verify panel setembed` first.")
            return

        channel = ctx.guild.get_channel(channel_id)
        if not channel:
            await ctx.send("❌ Configured channel not found.")
            return

        await self.create_panel_message(ctx.guild, channel)
        await ctx.send("✅ Verification panel created!")

    async def create_panel_message(self, guild: discord.Guild, channel: discord.TextChannel):
        """Create or update the verification panel message."""
        embed_data = await self.config.guild(guild).panel_embed()
        embed = discord.Embed.from_dict(embed_data)

        # Add verification button
        view = VerificationButtonView(self)

        # Delete old panel message if it exists
        old_message_id = await self.config.guild(guild).panel_message()
        if old_message_id:
            try:
                old_message = await channel.fetch_message(old_message_id)
                await old_message.delete()
            except:
                pass  # Message doesn't exist or can't be deleted

        # Send new panel message
        message = await channel.send(embed=embed, view=view)
        await self.config.guild(guild).panel_message.set(message.id)

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        """Handle button interactions."""
        if interaction.type == discord.InteractionType.component:
            if interaction.custom_id == "verify_button":
                await self.handle_verification_request(interaction)

    async def handle_verification_request(self, interaction: discord.Interaction):
        """Handle when a user clicks the verification button."""
        guild = interaction.guild
        user = interaction.user

        # Check if user already has an open ticket
        open_ticket = await self.config.member(user).open_ticket()
        if open_ticket:
            channel = guild.get_channel(open_ticket)
            if channel:
                await interaction.response.send_message(
                    f"You already have an open verification ticket: {channel.mention}",
                    ephemeral=True
                )
                return

        # Check if ticket category is configured
        category_id = await self.config.guild(guild).ticket_category()
        if not category_id:
            await interaction.response.send_message(
                "❌ Ticket category not configured. Please contact an administrator.",
                ephemeral=True
            )
            return

        category = guild.get_channel(category_id)
        if not category:
            await interaction.response.send_message(
                "❌ Configured ticket category not found. Please contact an administrator.",
                ephemeral=True
            )
            return

        # Check if ticket embed is configured
        ticket_embed_data = await self.config.guild(guild).ticket_embed()
        if not ticket_embed_data:
            await interaction.response.send_message(
                "❌ Verification ticket embed not configured. Please contact an administrator.",
                ephemeral=True
            )
            return

        # Create verification ticket
        await self.create_verification_ticket(interaction, user, category, ticket_embed_data)

    async def create_verification_ticket(self, interaction: discord.Interaction, user: discord.Member, category: discord.CategoryChannel, ticket_embed_data):
        """Create a verification ticket channel for the user."""
        guild = interaction.guild

        # Create ticket channel
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
            guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True, manage_channels=True)
        }

        channel_name = f"verify-{user.name}"
        try:
            ticket_channel = await guild.create_text_channel(
                name=channel_name,
                category=category,
                overwrites=overwrites,
                reason=f"Verification ticket for {user}"
            )

            # Store ticket info
            await self.config.member(user).open_ticket.set(ticket_channel.id)

            # Send welcome message
            embed = discord.Embed.from_dict(ticket_embed_data)
            await ticket_channel.send(f"{user.mention}", embed=embed)

            await interaction.response.send_message(
                f"✅ Verification ticket created: {ticket_channel.mention}",
                ephemeral=True
            )

        except Exception as e:
            self.log.error(f"Error creating verification ticket: {e}")
            await interaction.response.send_message(
                "❌ Error creating verification ticket. Please try again or contact an administrator.",
                ephemeral=True
            )

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Automatically close the verification ticket if the user leaves before finishing verification."""
        open_ticket = await self.config.member(member).open_ticket()
        if open_ticket:
            guild = member.guild
            channel = guild.get_channel(open_ticket)
            if channel:
                try:
                    await channel.delete(reason="User left before finishing verification")
                except Exception as e:
                    self.log.warning(f"Failed to delete verification ticket channel for {member}: {e}")
            await self.config.member(member).open_ticket.clear()

class VerificationButtonView(discord.ui.View):
    """View for the verification button."""

    def __init__(self, cog: SS13Verify):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Verify Discord",
        style=discord.ButtonStyle.primary,
        custom_id="verify_button",
        emoji="✅"
    )
    async def verify_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_verification_request(interaction)
