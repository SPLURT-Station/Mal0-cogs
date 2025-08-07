"""
Verification commands mixin for the CkeyTools cog.
This module contains all Discord verification related commands.
"""

import logging
import json
import datetime
from typing import Optional, Union

import discord
from discord.ext import commands
from redbot.core import commands as red_commands, checks
from redbot.core.utils import chat_formatting

from ..helpers import normalise_to_ckey
from .ui_components import VerificationButtonView, DeverifyConfirmView

log = logging.getLogger("red.ss13_verify.verify.commands")


class VerificationCommandsMixin:
    """Mixin class providing Discord verification commands."""

    # Settings subgroup (will be added to ckeytools group)
    @red_commands.group()
    async def ckeytools(self, ctx):
        """CkeyTools - SS13 Discord verification and management system."""
        pass

    @ckeytools.group(name="settings")
    async def ckeytools_settings(self, ctx):
        """Configure CkeyTools behavior settings."""
        pass

    # Database configuration commands
    @ckeytools_settings.group()
    @checks.admin_or_permissions(administrator=True)
    async def database(self, ctx):
        """Configure database connection for SS13 verification."""
        pass

    @database.command()
    async def host(self, ctx, host: str):
        """
        Set the database hostname or IP address.

        This is the hostname or IP address where your MySQL/MariaDB server is running.
        For local installations, this is usually 'localhost' or '127.0.0.1'.

        Example: `[p]ckeytools settings database host localhost`
        """
        await self.config.guild(ctx.guild).db_host.set(host)
        await ctx.send(f"Database host set to `{host}`.")
        await ctx.tick()

    @database.command()
    async def port(self, ctx, port: int):
        """
        Set the database port number.

        This is the port number your MySQL/MariaDB server is listening on.
        The default MySQL port is 3306. Only change this if your database
        server is configured to use a different port.

        Example: `[p]ckeytools settings database port 3306`
        """
        await self.config.guild(ctx.guild).db_port.set(port)
        await ctx.send(f"Database port set to `{port}`.")
        await ctx.tick()

    @database.command()
    async def user(self, ctx, user: str):
        """
        Set the database username.

        This is the username that the bot will use to connect to your MySQL/MariaDB server.
        Make sure this user has the necessary permissions to read from and write to
        the discord_links table.

        Example: `[p]ckeytools settings database user ss13_bot`
        """
        await self.config.guild(ctx.guild).db_user.set(user)
        await ctx.send(f"Database user set to `{user}`.")
        await ctx.tick()

    @database.command()
    async def password(self, ctx, password: str):
        """
        Set the database password.

        This is the password for the database user. For security reasons,
        consider using this command in a private channel or DM.

        Example: `[p]ckeytools settings database password your_secure_password`
        """
        await self.config.guild(ctx.guild).db_password.set(password)
        await ctx.send("Database password has been set.")
        await ctx.tick()
        
        # Try to delete the message for security
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass

    @database.command()
    async def name(self, ctx, database_name: str):
        """
        Set the database name.

        This is the name of the MySQL/MariaDB database that contains your SS13 server data.
        The bot will look for the discord_links table in this database.

        Example: `[p]ckeytools settings database name ss13_database`
        """
        await self.config.guild(ctx.guild).db_name.set(database_name)
        await ctx.send(f"Database name set to `{database_name}`.")
        await ctx.tick()

    @database.command()
    async def prefix(self, ctx, prefix: str = ""):
        """
        Set the database table prefix.

        If your SS13 server uses a prefix for table names (like 'ss13_discord_links'
        instead of just 'discord_links'), set it here. Leave empty for no prefix.

        Example: `[p]ckeytools settings database prefix ss13_`
        """
        await self.config.guild(ctx.guild).mysql_prefix.set(prefix)
        if prefix:
            await ctx.send(f"Database table prefix set to `{prefix}`.")
        else:
            await ctx.send("Database table prefix cleared.")
        await ctx.tick()

    @database.command()
    async def reconnect(self, ctx):
        """
        Reconnect to the database.

        Use this command after configuring your database settings to test the connection.
        The bot will attempt to connect using the configured settings.
        """
        await ctx.send("Attempting to reconnect to database...")
        await self.reconnect_database(ctx.guild)
        
        if self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("✅ Database connection successful!")
        else:
            await ctx.send("❌ Database connection failed. Please check your settings.")
        await ctx.tick()

    # Role management commands
    @ckeytools_settings.group()
    async def roles(self, ctx):
        """Manage verification roles."""
        pass

    @roles.command()
    async def add(self, ctx, role: discord.Role):
        """Add a role to be assigned on verification."""
        verification_roles = await self.config.guild(ctx.guild).verification_roles()
        if role.id not in verification_roles:
            verification_roles.append(role.id)
            await self.config.guild(ctx.guild).verification_roles.set(verification_roles)
            await ctx.send(f"Added {role.mention} to verification roles.")
        else:
            await ctx.send(f"{role.mention} is already a verification role.")
        await ctx.tick()

    @roles.command()
    async def remove(self, ctx, role: discord.Role):
        """Remove a role from verification roles."""
        verification_roles = await self.config.guild(ctx.guild).verification_roles()
        if role.id in verification_roles:
            verification_roles.remove(role.id)
            await self.config.guild(ctx.guild).verification_roles.set(verification_roles)
            await ctx.send(f"Removed {role.mention} from verification roles.")
        else:
            await ctx.send(f"{role.mention} is not a verification role.")
        await ctx.tick()

    @roles.command()
    async def list(self, ctx):
        """List current verification roles."""
        verification_roles = await self.config.guild(ctx.guild).verification_roles()
        if not verification_roles:
            await ctx.send("No verification roles configured.")
            return
        
        role_mentions = []
        for role_id in verification_roles:
            role = ctx.guild.get_role(role_id)
            if role:
                role_mentions.append(role.mention)
            else:
                role_mentions.append(f"<@&{role_id}> (deleted)")
        
        embed = discord.Embed(
            title="Verification Roles",
            description="\n".join(role_mentions),
            color=await ctx.embed_color()
        )
        await ctx.send(embed=embed)

    @roles.command()
    async def clear(self, ctx):
        """Clear all verification roles."""
        await self.config.guild(ctx.guild).verification_roles.set([])
        await ctx.send("Cleared all verification roles.")
        await ctx.tick()

    # Panel configuration commands
    @ckeytools_settings.group()
    async def panel(self, ctx):
        """Configure the verification panel."""
        pass

    @panel.command()
    async def setchannel(self, ctx, channel: discord.TextChannel):
        """Set the channel for the verification panel."""
        await self.config.guild(ctx.guild).ticket_channel.set(channel.id)
        await ctx.send(f"Verification panel channel set to {channel.mention}.")
        await ctx.tick()

    @panel.command()
    async def setcategory(self, ctx, category: discord.CategoryChannel):
        """Set the category for verification tickets."""
        await self.config.guild(ctx.guild).ticket_category.set(category.id)
        await ctx.send(f"Verification ticket category set to **{category.name}**.")
        await ctx.tick()

    @panel.command()
    async def setembed(self, ctx):
        """Set the panel embed using an attached JSON file."""
        if not ctx.message.attachments:
            await ctx.send("❌ Please attach a JSON file containing the embed data.")
            return
        
        attachment = ctx.message.attachments[0]
        if not attachment.filename.endswith('.json'):
            await ctx.send("❌ Please attach a JSON file.")
            return
        
        try:
            embed_data = json.loads(await attachment.read())
            # Validate embed by creating it
            discord.Embed.from_dict(embed_data)
            
            await self.config.guild(ctx.guild).panel_embed.set(embed_data)
            await ctx.send("✅ Panel embed configured successfully!")
            await ctx.tick()
        except json.JSONDecodeError:
            await ctx.send("❌ Invalid JSON format.")
        except Exception as e:
            await ctx.send(f"❌ Invalid embed format: {e}")

    @panel.command()
    async def setticketembed(self, ctx):
        """Set the ticket embed using an attached JSON file."""
        if not ctx.message.attachments:
            await ctx.send("❌ Please attach a JSON file containing the embed data.")
            return
        
        attachment = ctx.message.attachments[0]
        if not attachment.filename.endswith('.json'):
            await ctx.send("❌ Please attach a JSON file.")
            return
        
        try:
            embed_data = json.loads(await attachment.read())
            # Validate embed by creating it
            discord.Embed.from_dict(embed_data)
            
            await self.config.guild(ctx.guild).ticket_embed.set(embed_data)
            await ctx.send("✅ Ticket embed configured successfully!")
            await ctx.tick()
        except json.JSONDecodeError:
            await ctx.send("❌ Invalid JSON format.")
        except Exception as e:
            await ctx.send(f"❌ Invalid embed format: {e}")

    @panel.command()
    async def create(self, ctx):
        """Create the verification panel in the configured channel."""
        config = await self.config.guild(ctx.guild).all()
        
        if not config["ticket_channel"]:
            await ctx.send("❌ Panel channel not configured. Use `[p]ckeytools settings panel setchannel` first.")
            return
        
        if not config["panel_embed"]:
            await ctx.send("❌ Panel embed not configured. Use `[p]ckeytools settings panel setembed` first.")
            return
        
        if not config["ticket_embed"]:
            await ctx.send("❌ Ticket embed not configured. Use `[p]ckeytools settings panel setticketembed` first.")
            return
        
        channel = ctx.guild.get_channel(config["ticket_channel"])
        if not channel:
            await ctx.send("❌ Panel channel not found. Please reconfigure.")
            return
        
        try:
            embed = discord.Embed.from_dict(config["panel_embed"])
            view = VerificationButtonView(self)
            message = await channel.send(embed=embed, view=view)
            
            await self.config.guild(ctx.guild).panel_message.set(message.id)
            await ctx.send(f"✅ Verification panel created in {channel.mention}!")
            await ctx.tick()
        except Exception as e:
            await ctx.send(f"❌ Failed to create panel: {e}")

    # System toggles
    @ckeytools_settings.command()
    async def invalidateonleave(self, ctx, enabled: bool = None):
        """Toggle whether to invalidate verification when users leave."""
        if enabled is None:
            current = await self.config.guild(ctx.guild).invalidate_on_leave()
            await ctx.send(f"Invalidate on leave is currently **{'enabled' if current else 'disabled'}**.")
            return
        
        await self.config.guild(ctx.guild).invalidate_on_leave.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"Invalidate on leave is now **{status}**.")
        await ctx.tick()

    @ckeytools_settings.command()
    async def verification(self, ctx, enabled: bool = None):
        """Toggle the verification system."""
        if enabled is None:
            current = await self.config.guild(ctx.guild).verification_enabled()
            await ctx.send(f"Verification system is currently **{'enabled' if current else 'disabled'}**.")
            return
        
        await self.config.guild(ctx.guild).verification_enabled.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"Verification system is now **{status}**.")
        await ctx.tick()

    @ckeytools_settings.command()
    async def autoverification(self, ctx, enabled: bool = None):
        """Toggle auto-verification for returning users."""
        if enabled is None:
            current = await self.config.guild(ctx.guild).autoverification_enabled()
            await ctx.send(f"Auto-verification is currently **{'enabled' if current else 'disabled'}**.")
            return
        
        await self.config.guild(ctx.guild).autoverification_enabled.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"Auto-verification is now **{status}**.")
        await ctx.tick()

    @ckeytools_settings.command()
    async def autoverifyonjoin(self, ctx, enabled: bool = None):
        """Toggle auto-verification when users join."""
        if enabled is None:
            current = await self.config.guild(ctx.guild).autoverify_on_join_enabled()
            await ctx.send(f"Auto-verify on join is currently **{'enabled' if current else 'disabled'}**.")
            return
        
        await self.config.guild(ctx.guild).autoverify_on_join_enabled.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"Auto-verify on join is now **{status}**.")
        await ctx.tick()

    # User management commands
    @ckeytools.command()
    async def checkuser(self, ctx, user: Union[discord.Member, discord.User, int]):
        """Check a user's verification status and link information."""
        if isinstance(user, int):
            user_id = user
            user_obj = self.bot.get_user(user_id)
            display_name = user_obj.display_name if user_obj else f"User ID {user_id}"
        else:
            user_id = user.id
            display_name = user.display_name
        
        if not self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("❌ Database not connected. Please check database configuration.")
            return
        
        try:
            # Get valid link
            valid_link = await self.db_manager.get_valid_link_by_discord_id(ctx.guild.id, user_id)
            
            # Check if user is deverified
            deverified_users = await self.config.guild(ctx.guild).deverified_users()
            is_deverified = user_id in deverified_users
            
            embed = discord.Embed(
                title=f"User Information: {display_name}",
                color=discord.Color.green() if valid_link and not is_deverified else discord.Color.red(),
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            
            embed.add_field(
                name="Discord ID",
                value=f"`{user_id}`",
                inline=True
            )
            
            if valid_link:
                embed.add_field(
                    name="Linked Ckey",
                    value=f"**{valid_link.ckey}**",
                    inline=True
                )
                embed.add_field(
                    name="Link Date",
                    value=f"<t:{int(valid_link.timestamp.timestamp())}:F>",
                    inline=True
                )
                embed.add_field(
                    name="Verification Status",
                    value="✅ **Verified**" if not is_deverified else "❌ **Deverified**",
                    inline=False
                )
            else:
                embed.add_field(
                    name="Verification Status",
                    value="❌ **Not Verified**",
                    inline=False
                )
            
            if is_deverified:
                embed.add_field(
                    name="⚠️ Deverified",
                    value="This user has been manually deverified and cannot auto-verify.",
                    inline=False
                )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            log.error(f"Error checking user {user_id}: {e}")
            await ctx.send(f"❌ Error checking user: {e}")

    @ckeytools.command()
    async def ckeys(self, ctx, user: Union[discord.Member, discord.User, int]):
        """List all historical ckeys for a Discord user."""
        if isinstance(user, int):
            user_id = user
            user_obj = self.bot.get_user(user_id)
            display_name = user_obj.display_name if user_obj else f"User ID {user_id}"
        else:
            user_id = user.id
            display_name = user.display_name
        
        if not self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("❌ Database not connected. Please check database configuration.")
            return
        
        try:
            links = await self.db_manager.get_all_links_by_discord_id(ctx.guild.id, user_id)
            
            if not links:
                await ctx.send(f"No ckeys found for {display_name}.")
                return
            
            embed = discord.Embed(
                title=f"Historical Ckeys for {display_name}",
                color=await ctx.embed_color(),
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            
            ckey_info = []
            for link in links:
                status = "✅ Valid" if link.valid else "❌ Invalid"
                timestamp = f"<t:{int(link.timestamp.timestamp())}:R>"
                ckey_info.append(f"**{link.ckey}** - {status} ({timestamp})")
            
            embed.add_field(
                name="Ckeys",
                value="\n".join(ckey_info[:10]) + ("\n..." if len(ckey_info) > 10 else ""),
                inline=False
            )
            
            if len(ckey_info) > 10:
                embed.set_footer(text=f"Showing 10 of {len(ckey_info)} total links")
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            log.error(f"Error getting ckeys for user {user_id}: {e}")
            await ctx.send(f"❌ Error getting ckeys: {e}")

    @ckeytools.command()
    async def discords(self, ctx, ckey: str):
        """List all historical Discord accounts for a ckey."""
        ckey = normalise_to_ckey(ckey)
        
        if not self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("❌ Database not connected. Please check database configuration.")
            return
        
        try:
            links = await self.db_manager.get_all_links_by_ckey(ctx.guild.id, ckey)
            
            if not links:
                await ctx.send(f"No Discord accounts found for ckey **{ckey}**.")
                return
            
            embed = discord.Embed(
                title=f"Historical Discord Accounts for {ckey.title()}",
                color=await ctx.embed_color(),
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )
            
            discord_info = []
            for link in links:
                if link.discord_id:
                    status = "✅ Valid" if link.valid else "❌ Invalid"
                    timestamp = f"<t:{int(link.timestamp.timestamp())}:R>"
                    discord_info.append(f"<@{link.discord_id}> - {status} ({timestamp})")
            
            if discord_info:
                embed.add_field(
                    name="Discord Accounts",
                    value="\n".join(discord_info[:10]) + ("\n..." if len(discord_info) > 10 else ""),
                    inline=False
                )
                
                if len(discord_info) > 10:
                    embed.set_footer(text=f"Showing 10 of {len(discord_info)} total links")
            else:
                embed.add_field(
                    name="Discord Accounts",
                    value="No linked Discord accounts found.",
                    inline=False
                )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            log.error(f"Error getting Discord accounts for ckey {ckey}: {e}")
            await ctx.send(f"❌ Error getting Discord accounts: {e}")

    # Deverify command (standalone, not under ckeytools group)
    @red_commands.command()
    async def deverify(self, ctx, user: Optional[Union[discord.Member, discord.User]] = None):
        """
        Deverify a user (remove their verification link).
        
        If no user is specified, deverifies the command author.
        Requires admin permissions to deverify other users.
        """
        # Determine target user
        if user is None:
            target_user = ctx.author
            is_self = True
        else:
            target_user = user
            is_self = False
        
        # Permission check
        if not is_self:
            if not (ctx.author.guild_permissions.kick_members or ctx.author.guild_permissions.administrator):
                await ctx.send("❌ You need kick members or administrator permissions to deverify other users.")
                return
        
        # Check if user is verified
        if not await self.is_user_verified(ctx.guild, target_user.id):
            await ctx.send(f"❌ {target_user.display_name} is not currently verified.")
            return
        
        # Send confirmation view
        view = DeverifyConfirmView(self, target_user, ctx.author)
        
        if is_self:
            message = f"Are you sure you want to deverify yourself? This will remove your verification link and kick you from the server."
        else:
            message = f"Are you sure you want to deverify **{target_user.display_name}**? This will remove their verification link and kick them from the server."
        
        await ctx.send(message, view=view, ephemeral=True)
