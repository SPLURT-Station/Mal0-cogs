import logging
import json
import asyncio
import os
import discord
from typing import Optional, Dict, Any
from discord.ext import tasks
from redbot.core import commands, Config, checks
from redbot.core.bot import Red
from redbot.core.utils import chat_formatting
import hashlib
import datetime
from discord import ui
from .helpers import normalise_to_ckey
from .database import DatabaseManager
import tomlkit
import aiohttp
import csv
import io
try:
    from dateutil import parser as date_parser
except ImportError:
    # Fallback if dateutil is not available
    date_parser = None


class CkeyTools(commands.Cog):
    """
    A cog for managing SS13 server integrations with Discord.

    Features:
    - MySQL-based verification system linking Discord users to SS13 ckeys
    - Ticket system for manual verification with configurable permissions
    - Auto-verification on join with role assignment
    - Autoroles system that exports role-mapped ckey lists to TOML
    - Database configuration and management tools
    """
    __author__ = "Mosley"
    __version__ = "4.0.0"

    def __init__(self, bot: Red):
        self.bot = bot
        self.log = logging.getLogger("red.ckeytools")
        self.config = Config.get_conf(self, identifier=908039527271104514, force_registration=True)
        self.db_manager = DatabaseManager()
        # Per-guild config
        default_guild: Dict[str, Any] = {
            "ticket_channel": None,  # Channel ID for ticket panel
            "ticket_category": None, # Category ID for ticket channels
            "panel_message": None,   # Message ID for the panel embed
            "panel_embed": {},       # JSON dict for the panel embed
            "ticket_embed": {},      # JSON dict for the ticket embed
            "verification_roles": [], # List of role IDs to assign on verification
            "db_host": "127.0.0.1",
            "db_port": 3306,
            "db_user": None,
            "db_password": None,
            "db_name": None,
            "mysql_prefix": "",
            "invalidate_on_leave": False,  # Whether to invalidate verification when user leaves
            "verification_enabled": False,  # Whether verification system is enabled
            "autoverification_enabled": False,  # Whether auto-verification is enabled
            "autoverify_on_join_enabled": False,  # Whether auto-verification on join is enabled
            "deverified_users": [],  # List of user IDs who have been manually deverified
            # Ticket permission system
            "ticket_default_permissions": {},  # Default permissions for @everyone in tickets
            "ticket_staff_roles": [],  # List of role IDs that get staff access to tickets
            "ticket_staff_permissions": {},  # Permissions for staff roles in tickets
            "ticket_opener_permissions": {},  # Permissions for the user who opened the ticket
            # autoroles (role → TOML path) system
            "autoroles_enabled": False,
            "autoroles_config_folder": None,
            "autoroles_file_name": "donator.toml",
            "autoroles_role_paths": {},  # {role_id(str): "path.to.key"}
            # agevet system
            "agevet_enabled": False,  # Whether age vetting system is enabled
            "agevet_api_url": None,  # BackgroundCheck API URL
            "agevet_api_key": None,  # API key for BackgroundCheck
            "agevet_role": None,  # Role ID to assign to age-vetted users
        }
        self.config.register_guild(**default_guild)
        # Per-user config (for future use, e.g. to track open tickets)
        default_member = {
            "open_ticket": None,  # Channel ID of open ticket, if any
        }
        self.config.register_member(**default_member)

    async def cog_load(self):
        # First, connect to databases for all guilds with config
        for guild in self.bot.guilds:
            conf = await self.config.guild(guild).all()
            if all([conf["db_host"], conf["db_port"], conf["db_user"], conf["db_password"], conf["db_name"]]):
                await self.reconnect_database(guild)

        # Then add persistent views back to the bot so buttons work after reload
        try:
            self.bot.add_view(VerificationButtonView(self))
            self.bot.add_view(VerificationCodeView(self, None, None))  # Generic view for handling all verify_code_button interactions
            self.log.info("Successfully registered persistent views for CkeyTools")
        except Exception as e:
            self.log.error(f"Failed to register persistent views: {e}")

        # Start autoroles periodic updater
        try:
            if not self.autoroles_update.is_running():
                self.autoroles_update.start()
        except Exception as e:
            self.log.error(f"Failed to start autoroles updater: {e}")

    async def cog_unload(self):
        # Close all database connections when cog is unloaded
        await self.db_manager.disconnect_all()
        # Stop autoroles periodic updater
        try:
            if self.autoroles_update.is_running():
                self.autoroles_update.cancel()
        except Exception:
            pass

    async def reconnect_database(self, guild):
        """Reconnect the database for a guild using SQLAlchemy."""
        conf = await self.config.guild(guild).all()

        success = await self.db_manager.connect_guild(
            guild_id=guild.id,
            host=conf["db_host"],
            port=conf["db_port"],
            user=conf["db_user"],
            password=conf["db_password"],
            database=conf["db_name"],
            prefix=conf["mysql_prefix"] or ""
        )

        if success:
            self.log.info(f"Connected to database for guild {guild.name} ({guild.id})")
        else:
            self.log.error(f"Failed to connect to database for guild {guild.name} ({guild.id})")


    # Root command group for this cog
    @commands.group(name="ckeytools")
    async def ckeytools(self, ctx: commands.Context):
        """Main command group for SS13 verification and autoroles tools."""
        pass


    def _get_all_discord_permissions(self):
        """Get a list of all available Discord permissions."""
        return [
            'add_reactions', 'administrator', 'attach_files', 'ban_members', 'change_nickname',
            'connect', 'create_instant_invite', 'create_private_threads', 'create_public_threads',
            'deafen_members', 'embed_links', 'external_emojis', 'external_stickers',
            'kick_members', 'manage_channels', 'manage_emojis', 'manage_events', 'manage_guild',
            'manage_messages', 'manage_nicknames', 'manage_permissions', 'manage_roles',
            'manage_threads', 'manage_webhooks', 'mention_everyone', 'moderate_members',
            'move_members', 'mute_members', 'priority_speaker', 'read_message_history',
            'read_messages', 'request_to_speak', 'send_messages', 'send_messages_in_threads',
            'send_tts_messages', 'speak', 'stream', 'use_application_commands', 'use_embedded_activities',
            'use_external_emojis', 'use_external_stickers', 'use_voice_activation', 'view_audit_log',
            'view_channel', 'view_guild_insights'
        ]

    def _parse_permission_value(self, value):
        """Parse permission value string to boolean or None."""
        value_lower = value.lower()
        if value_lower in ['allow', 'true', 'yes', '1']:
            return True
        elif value_lower in ['deny', 'false', 'no', '0']:
            return False
        elif value_lower in ['passthrough', 'neutral', 'none', 'null']:
            return None
        else:
            raise ValueError(f"Invalid permission value: {value}. Use 'allow', 'deny', or 'passthrough'.")

    def _parse_permission_args(self, args_string):
        """
        Parse command-line style permission arguments using regex.

        Supports formats like:
        --permission value --permission2 value2
        --permission=value --permission2=value2
        -permission value -permission2 value2
        -permission=value -permission2=value2

        Returns dict of {permission: value}
        """
        import re

        if not args_string.strip():
            return {}

        permissions = {}

        # Regex pattern to match permission arguments
        # Matches: --perm=value, --perm value, -perm=value, -perm value
        # Supports quoted values: --perm="quoted value" or --perm 'quoted value'
        pattern = r'''
            (-{1,2})                    # Group 1: One or two dashes
            ([a-zA-Z_][a-zA-Z0-9_]*)    # Group 2: Permission name (starts with letter/underscore)
            (?:
                =                       # Equals sign (for --perm=value format)
                (?:
                    "([^"]*)"           # Group 3: Double-quoted value
                    |'([^']*)'          # Group 4: Single-quoted value
                    |([^\s]+)           # Group 5: Unquoted value
                )
                |                       # OR
                \s+                     # Whitespace (for --perm value format)
                (?=(?:                  # Lookahead for next value
                    "([^"]*)"           # Group 6: Double-quoted value with space
                    |'([^']*)'          # Group 7: Single-quoted value with space
                    |([^\s-][^\s]*)     # Group 8: Unquoted value with space (not starting with -)
                ))
            )?                          # Value is optional
        '''

        # Find all permission matches
        matches = re.finditer(pattern, args_string, re.VERBOSE)

        for match in matches:
            dashes, perm_name = match.group(1, 2)

            # Extract value from the appropriate group
            value = None
            for group_idx in range(3, 9):  # Groups 3-8 contain possible values
                if match.group(group_idx) is not None:
                    value = match.group(group_idx)
                    break

            # If no value found, default to 'allow'
            if value is None:
                value = 'allow'

            permissions[perm_name] = value

        # Alternative regex for space-separated format if first pattern doesn't catch everything
        # This handles cases where the lookahead might miss some patterns
        if not permissions:
            # Simpler pattern for basic cases
            simple_pattern = r'(-{1,2})([a-zA-Z_][a-zA-Z0-9_]*)'
            simple_matches = re.finditer(simple_pattern, args_string)

            # Convert to list to allow indexing
            all_matches = list(simple_matches)

            for i, match in enumerate(all_matches):
                perm_name = match.group(2)

                # Look for value after this permission
                start_pos = match.end()

                # Check if there's a next permission match
                if i + 1 < len(all_matches):
                    end_pos = all_matches[i + 1].start()
                    text_between = args_string[start_pos:end_pos].strip()
                else:
                    text_between = args_string[start_pos:].strip()

                # Extract value from text between matches
                if text_between:
                    # Remove any leading = sign
                    if text_between.startswith('='):
                        text_between = text_between[1:].strip()

                    # Extract first word/quoted string as value
                    value_match = re.match(r'^(?:"([^"]*)"|\'([^\']*)\'|([^\s]+))', text_between)
                    if value_match:
                        value = value_match.group(1) or value_match.group(2) or value_match.group(3)
                    else:
                        value = 'allow'
                else:
                    value = 'allow'

                permissions[perm_name] = value

        # Validate permissions and convert values
        result = {}
        for perm, value in permissions.items():
            if perm not in self._get_all_discord_permissions():
                raise ValueError(f"Invalid permission: {perm}")

            try:
                parsed_value = self._parse_permission_value(value)
                result[perm] = parsed_value
            except ValueError as e:
                raise ValueError(f"Error parsing permission '{perm}': {e}")

        return result

    def _permission_value_to_string(self, value):
        """Convert permission boolean/None to string representation."""
        if value is True:
            return "Allow"
        elif value is False:
            return "Deny"
        else:
            return "Passthrough"

    async def _build_ticket_overwrites(self, guild, ticket_opener):
        """Build permission overwrites for ticket channels based on configuration."""
        conf = await self.config.guild(guild).all()
        overwrites = {}

        # Default role (@everyone) permissions
        default_perms = conf["ticket_default_permissions"]
        if default_perms:
            overwrite_kwargs = {}
            for perm, value in default_perms.items():
                if perm in self._get_all_discord_permissions():
                    overwrite_kwargs[perm] = value
            if overwrite_kwargs:
                overwrites[guild.default_role] = discord.PermissionOverwrite(**overwrite_kwargs)
        else:
            # Default behavior: deny read access to @everyone
            overwrites[guild.default_role] = discord.PermissionOverwrite(view_channel=False)

        # Staff roles permissions
        staff_role_ids = conf["ticket_staff_roles"]
        staff_perms = conf["ticket_staff_permissions"]
        for role_id in staff_role_ids:
            role = guild.get_role(role_id)
            if role:
                if staff_perms:
                    overwrite_kwargs = {}
                    for perm, value in staff_perms.items():
                        if perm in self._get_all_discord_permissions():
                            overwrite_kwargs[perm] = value
                    if overwrite_kwargs:
                        overwrites[role] = discord.PermissionOverwrite(**overwrite_kwargs)
                else:
                    # Default staff permissions
                    overwrites[role] = discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                        read_message_history=True,
                        manage_messages=True,
                        manage_channels=True
                    )

        # Ticket opener permissions
        opener_perms = conf["ticket_opener_permissions"]
        if opener_perms:
            overwrite_kwargs = {}
            for perm, value in opener_perms.items():
                if perm in self._get_all_discord_permissions():
                    overwrite_kwargs[perm] = value
            if overwrite_kwargs:
                overwrites[ticket_opener] = discord.PermissionOverwrite(**overwrite_kwargs)
        else:
            # Default opener permissions
            overwrites[ticket_opener] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True
            )

        # Bot permissions (always ensure bot can manage the ticket)
        overwrites[guild.me] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            manage_channels=True,
            manage_messages=True
        )

        return overwrites

    @ckeytools.group(name="verify")
    @checks.admin_or_permissions(manage_guild=True)
    async def verify(self, ctx):
        """SS13 Verification system configuration."""
        pass

    @ckeytools.group(name="database")
    @checks.admin_or_permissions(administrator=True)
    async def database(self, ctx):
        """Configure database connection used by verification and autoroles."""
        pass

    @database.command()
    async def host(self, ctx, host: str):
        """
        Set the database hostname or IP address.

        This is the hostname or IP address where your MySQL/MariaDB server is running.
        For local installations, this is usually 'localhost' or '127.0.0.1'.

        Example: `[p]ckeytools database host localhost`
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

        Example: `[p]ckeytools database port 3306`
        """
        await self.config.guild(ctx.guild).db_port.set(port)
        await ctx.send(f"Database port set to `{port}`.")
        await ctx.tick()

    @database.command()
    async def user(self, ctx, user: str):
        """
        Set the database username.

        This is the username for the MySQL/MariaDB account that has access
        to your SS13 database. This account needs SELECT, INSERT, and UPDATE
        permissions on the discord_links table.

        Example: `[p]ckeytools database user ss13_bot`
        """
        await self.config.guild(ctx.guild).db_user.set(user)
        await ctx.send(f"Database user set to `{user}`.")
        await ctx.tick()

    @database.command()
    async def password(self, ctx, password: str):
        """
        Set the database password.

        This is the password for the MySQL/MariaDB account specified with the user command.

        **Security Note:** The password will be stored in the bot's configuration.
        Make sure to use a dedicated database account with limited permissions.

        Example: `[p]ckeytools database password your_secure_password`
        """
        await self.config.guild(ctx.guild).db_password.set(password)
        await ctx.send("Database password set.")
        await ctx.tick()

    @database.command()
    async def name(self, ctx, name: str):
        """
        Set the database name.

        This is the name of the MySQL/MariaDB database that contains your
        SS13 server data, including the discord_links table.

        This is typically something like 'ss13_database' or 'tgstation'.

        Example: `[p]ckeytools database name ss13_database`
        """
        await self.config.guild(ctx.guild).db_name.set(name)
        await ctx.send(f"Database name set to `{name}`.")
        await ctx.tick()

    @database.command()
    async def prefix(self, ctx, prefix: str):
        """
        Set the MySQL table prefix.

        This is the prefix used for your SS13 database tables. Most SS13 servers
        use an empty prefix (no prefix), but some use prefixes like 'ss13_' or 'tg_'.

        If your discord_links table is just called 'discord_links', leave this empty.
        If it's called something like 'ss13_discord_links', set this to 'ss13_'.

        Example: `[p]ckeytools database prefix ss13_`
        Example (no prefix): `[p]ckeytools database prefix ""`
        """
        await self.config.guild(ctx.guild).mysql_prefix.set(prefix)
        await ctx.send(f"MySQL table prefix set to `{prefix}`.")
        await ctx.tick()

    @database.command()
    async def reconnect(self, ctx):
        """
        Reconnect to the database with the current settings.

        Use this command after configuring all database settings to test
        the connection and establish the connection pool. The bot will
        automatically attempt to connect when loaded if all settings are present.

        This command is also useful if the database connection is lost and
        needs to be re-established.

        Example: `[p]ckeytools database reconnect`
        """
        await self.reconnect_database(ctx.guild)
        if self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("✅ Database reconnected successfully.")
            await ctx.tick()
        else:
            await ctx.send("❌ Failed to reconnect to the database. Check your settings and try again.")

    @database.command(name="status")
    async def database_status(self, ctx: commands.Context):
        """Check the status of the database connection and show details."""
        import time
        try:
            from sqlalchemy import text
        except Exception:
            text = None  # type: ignore

        conf = await self.config.guild(ctx.guild).all()
        host = conf.get("db_host")
        port = conf.get("db_port")
        user = conf.get("db_user")
        db_name = conf.get("db_name")
        prefix = conf.get("mysql_prefix") or ""

        configured = all([host, port, user, conf.get("db_password"), db_name])
        connected = self.db_manager.is_connected(ctx.guild.id) if ctx.guild else False

        ok = False
        ping_ms = None
        error_msg = None

        if configured and connected:
            # Try a very small query to validate connection
            try:
                if ctx.guild:
                    engine = self.db_manager.engines.get(ctx.guild.id)
                    if engine and text is not None:
                        start = time.perf_counter()
                        async with engine.connect() as conn:
                            await conn.execute(text("SELECT 1"))
                        ping_ms = (time.perf_counter() - start) * 1000.0
                        ok = True
                    else:
                        # Fallback: use session open/close as a check
                        start = time.perf_counter()
                        async with self.db_manager.get_session(ctx.guild.id) as _:
                            pass
                        ping_ms = (time.perf_counter() - start) * 1000.0
                        ok = True
            except Exception as e:
                error_msg = str(e)
                ok = False
        else:
            ok = False

        color = discord.Color.green() if ok else discord.Color.red()

        embed = discord.Embed(
            title="Database Status",
            color=color,
            timestamp=discord.utils.utcnow()
        )

        embed.add_field(name="Configured", value="✅ Yes" if configured else "❌ No", inline=True)
        embed.add_field(name="Connected", value="✅ Yes" if connected else "❌ No", inline=True)
        embed.add_field(name="Ping", value=(f"{ping_ms:.1f} ms" if ping_ms is not None else "N/A"), inline=True)

        embed.add_field(name="Host", value=str(host or "-"), inline=True)
        embed.add_field(name="Port", value=str(port or "-"), inline=True)
        embed.add_field(name="Database", value=str(db_name or "-"), inline=True)
        embed.add_field(name="Prefix", value=f"`{prefix}`" if prefix else "(none)", inline=True)

        if error_msg and not ok:
            # Truncate if too long for safety
            if len(error_msg) > 1000:
                error_msg = error_msg[:1000] + "…"
            embed.add_field(name="Error", value=chat_formatting.box(error_msg, "text"), inline=False)

        await ctx.send(embed=embed)

    @verify.group(name="config")
    async def verify_config(self, ctx):
        """Configure CkeyTools behavior settings."""
        pass

    @verify_config.group()
    async def roles(self, ctx):
        """Configure verification roles."""
        pass

    @roles.command(name="add")
    async def add_verification_role(self, ctx, role: discord.Role):
        """Add a role to be assigned upon successful verification."""
        role_ids = await self.config.guild(ctx.guild).verification_roles()
        if role.id not in role_ids:
            role_ids.append(role.id)
            await self.config.guild(ctx.guild).verification_roles.set(role_ids)
            await ctx.send(f"✅ Added {role.mention} to verification roles.")
            await ctx.tick()
        else:
            await ctx.send(f"❌ {role.mention} is already a verification role.")

    @roles.command(name="remove")
    async def remove_verification_role(self, ctx, role: discord.Role):
        """Remove a role from being assigned upon verification."""
        role_ids = await self.config.guild(ctx.guild).verification_roles()
        if role.id in role_ids:
            role_ids.remove(role.id)
            await self.config.guild(ctx.guild).verification_roles.set(role_ids)
            await ctx.send(f"✅ Removed {role.mention} from verification roles.")
            await ctx.tick()
        else:
            await ctx.send(f"❌ {role.mention} is not a verification role.")

    @roles.command(name="list")
    async def list_verification_roles(self, ctx):
        """List all roles that will be assigned upon verification."""
        role_ids = await self.config.guild(ctx.guild).verification_roles()
        if not role_ids:
            await ctx.send("❌ No verification roles configured.")
            return

        roles = [ctx.guild.get_role(rid) for rid in role_ids if ctx.guild.get_role(rid)]
        if not roles:
            await ctx.send("❌ No valid verification roles found. Some roles may have been deleted.")
            return

        role_mentions = [role.mention for role in roles]
        await ctx.send(f"**Verification Roles:**\n{', '.join(role_mentions)}")

    @roles.command(name="clear")
    async def clear_verification_roles(self, ctx):
        """Clear all verification roles."""
        await self.config.guild(ctx.guild).verification_roles.set([])
        await ctx.send("✅ Cleared all verification roles.")
        await ctx.tick()

    @verify_config.group()
    async def panel(self, ctx):
        """Configure the verification panel."""
        pass

    @verify_config.group()
    async def permissions(self, ctx):
        """Configure ticket permission system."""
        pass

    @permissions.command(name="defaultset")
    async def set_default_permission(self, ctx, *, args: str):
        """
        Set default permissions for @everyone in tickets using command-line style arguments.

        Supports multiple formats:
        --permission value --permission2 value2
        --permission=value --permission2=value2
        -permission value -permission2 value2
        -permission=value -permission2=value2

        Values: allow, deny, or passthrough
        If no value is specified, defaults to 'allow'

        Examples:
        `[p]ckeytools verify config permissions defaultset --view_channel deny --send_messages allow`
        `[p]ckeytools verify config permissions defaultset -view_channel=deny -send_messages=allow`
        `[p]ckeytools verify config permissions defaultset --embed_links --attach_files deny`
        """
        try:
            new_perms = self._parse_permission_args(args)
        except ValueError as e:
            await ctx.send(f"❌ {e}")
            return

        if not new_perms:
            await ctx.send("❌ No valid permissions provided. Use `--permission value` format.")
            return

        current_perms = await self.config.guild(ctx.guild).ticket_default_permissions()
        current_perms.update(new_perms)
        await self.config.guild(ctx.guild).ticket_default_permissions.set(current_perms)

        # Build response
        updated = []
        for perm, value in new_perms.items():
            value_str = self._permission_value_to_string(value)
            updated.append(f"`{perm}`: {value_str}")

        await ctx.send(f"✅ Updated default permissions for @everyone:\n{', '.join(updated)}")
        await ctx.tick()

    @permissions.command(name="defaultremove")
    async def remove_default_permission(self, ctx, *, permissions: str):
        """
        Remove default permission settings for @everyone in tickets.

        Can remove multiple permissions at once by listing them separated by spaces.

        Examples:
        `[p]ckeytools verify config permissions defaultremove view_channel`
        `[p]ckeytools verify config permissions defaultremove view_channel send_messages attach_files`
        """
        permission_list = permissions.split()
        current_perms = await self.config.guild(ctx.guild).ticket_default_permissions()

        removed = []
        not_found = []

        for permission in permission_list:
            if permission in current_perms:
                del current_perms[permission]
                removed.append(permission)
            else:
                not_found.append(permission)

        if removed:
            await self.config.guild(ctx.guild).ticket_default_permissions.set(current_perms)
            await ctx.send(f"✅ Removed default permission settings for: {', '.join(f'`{p}`' for p in removed)}")
            await ctx.tick()

        if not_found:
            await ctx.send(f"❌ No settings found for: {', '.join(f'`{p}`' for p in not_found)}")

        if not removed and not not_found:
            await ctx.send("❌ No permissions specified.")

    @permissions.command(name="defaultlist")
    async def list_default_permissions(self, ctx):
        """List all configured default permissions for @everyone in tickets."""
        current_perms = await self.config.guild(ctx.guild).ticket_default_permissions()
        if not current_perms:
            await ctx.send("❌ No default permissions configured for @everyone in tickets.")
            return

        embed = discord.Embed(
            title="Default Ticket Permissions (@everyone)",
            color=await ctx.embed_color(),
            description="These permissions apply to @everyone role in verification tickets."
        )

        perm_text = ""
        for perm, value in current_perms.items():
            value_str = self._permission_value_to_string(value)
            perm_text += f"**{perm}**: {value_str}\n"

        embed.add_field(name="Permissions", value=perm_text, inline=False)
        await ctx.send(embed=embed)

    @permissions.command(name="staffadd")
    async def add_staff_role(self, ctx, role: discord.Role):
        """
        Add a role as a staff role for tickets.

        Example: `[p]ckeytools verify config permissions staffadd @Moderator`
        """
        staff_roles = await self.config.guild(ctx.guild).ticket_staff_roles()
        if role.id not in staff_roles:
            staff_roles.append(role.id)
            await self.config.guild(ctx.guild).ticket_staff_roles.set(staff_roles)
            await ctx.send(f"✅ Added {role.mention} as a staff role for tickets.")
            await ctx.tick()
        else:
            await ctx.send(f"❌ {role.mention} is already a staff role for tickets.")

    @permissions.command(name="staffremove")
    async def remove_staff_role(self, ctx, role: discord.Role):
        """
        Remove a role from staff roles for tickets.

        Example: `[p]ckeytools verify config permissions staffremove @Moderator`
        """
        staff_roles = await self.config.guild(ctx.guild).ticket_staff_roles()
        if role.id in staff_roles:
            staff_roles.remove(role.id)
            await self.config.guild(ctx.guild).ticket_staff_roles.set(staff_roles)
            await ctx.send(f"✅ Removed {role.mention} from staff roles for tickets.")
            await ctx.tick()
        else:
            await ctx.send(f"❌ {role.mention} is not a staff role for tickets.")

    @permissions.command(name="stafflist")
    async def list_staff_roles(self, ctx):
        """List all staff roles for tickets."""
        staff_role_ids = await self.config.guild(ctx.guild).ticket_staff_roles()
        if not staff_role_ids:
            await ctx.send("❌ No staff roles configured for tickets.")
            return

        roles = [ctx.guild.get_role(rid) for rid in staff_role_ids if ctx.guild.get_role(rid)]
        if not roles:
            await ctx.send("❌ No valid staff roles found. Some roles may have been deleted.")
            return

        role_mentions = [role.mention for role in roles]
        await ctx.send(f"**Staff Roles for Tickets:**\n{', '.join(role_mentions)}")

    @permissions.command(name="staffset")
    async def set_staff_permission(self, ctx, *, args: str):
        """
        Set permissions for staff roles in tickets using command-line style arguments.

        Supports multiple formats:
        --permission value --permission2 value2
        --permission=value --permission2=value2
        -permission value -permission2 value2
        -permission=value -permission2=value2

        Values: allow, deny, or passthrough
        If no value is specified, defaults to 'allow'

        Examples:
        `[p]ckeytools verify config permissions staffset --manage_messages allow --kick_members deny`
        `[p]ckeytools verify config permissions staffset -manage_messages=allow -kick_members=deny`
        `[p]ckeytools verify config permissions staffset --view_audit_log --manage_channels`
        """
        try:
            new_perms = self._parse_permission_args(args)
        except ValueError as e:
            await ctx.send(f"❌ {e}")
            return

        if not new_perms:
            await ctx.send("❌ No valid permissions provided. Use `--permission value` format.")
            return

        current_perms = await self.config.guild(ctx.guild).ticket_staff_permissions()
        current_perms.update(new_perms)
        await self.config.guild(ctx.guild).ticket_staff_permissions.set(current_perms)

        # Build response
        updated = []
        for perm, value in new_perms.items():
            value_str = self._permission_value_to_string(value)
            updated.append(f"`{perm}`: {value_str}")

        await ctx.send(f"✅ Updated staff permissions:\n{', '.join(updated)}")
        await ctx.tick()

    @permissions.command(name="staffpermlist")
    async def list_staff_permissions(self, ctx):
        """List all configured staff permissions for tickets."""
        current_perms = await self.config.guild(ctx.guild).ticket_staff_permissions()
        if not current_perms:
            await ctx.send("❌ No staff permissions configured for tickets.")
            return

        embed = discord.Embed(
            title="Staff Ticket Permissions",
            color=await ctx.embed_color(),
            description="These permissions apply to staff roles in verification tickets."
        )

        perm_text = ""
        for perm, value in current_perms.items():
            value_str = self._permission_value_to_string(value)
            perm_text += f"**{perm}**: {value_str}\n"

        embed.add_field(name="Permissions", value=perm_text, inline=False)
        await ctx.send(embed=embed)

    @permissions.command(name="openerupdate")
    async def set_opener_permission(self, ctx, *, args: str):
        """
        Set permissions for ticket openers in their tickets using command-line style arguments.

        Supports multiple formats:
        --permission value --permission2 value2
        --permission=value --permission2=value2
        -permission value -permission2 value2
        -permission=value -permission2=value2

        Values: allow, deny, or passthrough
        If no value is specified, defaults to 'allow'

        Examples:
        `[p]ckeytools verify config permissions openerupdate --attach_files allow --embed_links deny`
        `[p]ckeytools verify config permissions openerupdate -attach_files=allow -embed_links=deny`
        `[p]ckeytools verify config permissions openerupdate --add_reactions --use_external_emojis`
        """
        try:
            new_perms = self._parse_permission_args(args)
        except ValueError as e:
            await ctx.send(f"❌ {e}")
            return

        if not new_perms:
            await ctx.send("❌ No valid permissions provided. Use `--permission value` format.")
            return

        current_perms = await self.config.guild(ctx.guild).ticket_opener_permissions()
        current_perms.update(new_perms)
        await self.config.guild(ctx.guild).ticket_opener_permissions.set(current_perms)

        # Build response
        updated = []
        for perm, value in new_perms.items():
            value_str = self._permission_value_to_string(value)
            updated.append(f"`{perm}`: {value_str}")

        await ctx.send(f"✅ Updated opener permissions:\n{', '.join(updated)}")
        await ctx.tick()

    @permissions.command(name="openerlist")
    async def list_opener_permissions(self, ctx):
        """List all configured opener permissions for tickets."""
        current_perms = await self.config.guild(ctx.guild).ticket_opener_permissions()
        if not current_perms:
            await ctx.send("❌ No opener permissions configured for tickets.")
            return

        embed = discord.Embed(
            title="Ticket Opener Permissions",
            color=await ctx.embed_color(),
            description="These permissions apply to users who open verification tickets."
        )

        perm_text = ""
        for perm, value in current_perms.items():
            value_str = self._permission_value_to_string(value)
            perm_text += f"**{perm}**: {value_str}\n"

        embed.add_field(name="Permissions", value=perm_text, inline=False)
        await ctx.send(embed=embed)

    @permissions.command(name="reset")
    async def reset_permissions(self, ctx):
        """Reset all ticket permission configurations to default."""
        await self.config.guild(ctx.guild).ticket_default_permissions.set({})
        await self.config.guild(ctx.guild).ticket_staff_roles.set([])
        await self.config.guild(ctx.guild).ticket_staff_permissions.set({})
        await self.config.guild(ctx.guild).ticket_opener_permissions.set({})
        await ctx.send("✅ All ticket permission configurations have been reset to default.")
        await ctx.tick()

    @permissions.command(name="listall")
    async def list_all_permissions(self, ctx):
        """List all available Discord permissions."""
        perms = self._get_all_discord_permissions()
        chunks = [perms[i:i+10] for i in range(0, len(perms), 10)]

        embed = discord.Embed(
            title="Available Discord Permissions",
            color=await ctx.embed_color(),
            description="These are all the Discord permissions you can configure for tickets."
        )

        for i, chunk in enumerate(chunks, 1):
            perm_text = "\n".join([f"• {perm}" for perm in chunk])
            embed.add_field(name=f"Permissions ({i})", value=perm_text, inline=True)

        embed.set_footer(text="Use values: allow, deny, or passthrough")
        await ctx.send(embed=embed)

    @permissions.command(name="testparse")
    async def test_parse_args(self, ctx, *, args: str):
        """
        Test the argument parsing functionality.

        This command shows how your arguments would be parsed without actually changing any settings.

        Example: `[p]ckeytools verify config permissions testparse --view_channel deny --send_messages=allow -embed_links`
        """
        try:
            parsed = self._parse_permission_args(args)
            if not parsed:
                await ctx.send("❌ No valid permissions found in the arguments.")
                return

            embed = discord.Embed(
                title="Parsed Arguments",
                color=await ctx.embed_color(),
                description="Here's how your arguments would be interpreted:"
            )

            perm_text = ""
            for perm, value in parsed.items():
                value_str = self._permission_value_to_string(value)
                perm_text += f"**{perm}**: {value_str}\n"

            embed.add_field(name="Permissions", value=perm_text, inline=False)
            embed.set_footer(text="Use this to verify your syntax before applying changes")
            await ctx.send(embed=embed)

        except ValueError as e:
            await ctx.send(f"❌ Parse error: {e}")

    @permissions.command(name="help")
    async def permission_help(self, ctx):
        """Show detailed help for the permission system."""
        embed = discord.Embed(
            title="CkeyTools Permission System Help",
            color=await ctx.embed_color(),
            description="Configure fine-grained permissions for verification tickets."
        )

        embed.add_field(
            name="🎯 Command-Line Style Arguments",
            value=(
                "All permission commands support flexible argument formats:\n"
                "`--permission value` - Long form with space\n"
                "`--permission=value` - Long form with equals\n"
                "`-permission value` - Short form with space\n"
                "`-permission=value` - Short form with equals\n"
                "You can mix and match these formats in one command!"
            ),
            inline=False
        )

        embed.add_field(
            name="✅ Permission Values",
            value=(
                "`allow` / `true` / `yes` / `1` - ✅ Grant permission\n"
                "`deny` / `false` / `no` / `0` - ❌ Deny permission\n"
                "`passthrough` / `neutral` / `none` / `null` - ➡️ Use Discord default\n"
                "\nIf no value is specified, defaults to `allow`"
            ),
            inline=False
        )

        embed.add_field(
            name="📝 Example Commands",
            value=(
                "`defaultset --view_channel deny --send_messages allow`\n"
                "`staffset -manage_messages=allow -kick_members=deny --view_audit_log`\n"
                "`openerupdate --embed_links --attach_files deny --add_reactions=allow`\n"
                "`testparse --view_channel deny --send_messages` (test syntax)"
            ),
            inline=False
        )

        embed.add_field(
            name="🔧 Quick Setup",
            value=(
                "1. `listall` - See all available permissions\n"
                "2. `staffadd @Role` - Add staff roles\n"
                "3. `defaultset --view_channel deny` - Hide from @everyone\n"
                "4. `staffset --view_channel allow --manage_messages allow` - Staff access\n"
                "5. `openerupdate --attach_files allow` - Let users attach files"
            ),
            inline=False
        )

        await ctx.send(embed=embed)

    @permissions.command(name="defaultshow")
    async def show_default_permissions_cmdline(self, ctx):
        """Show current default permissions in command-line format for easy copying."""
        current_perms = await self.config.guild(ctx.guild).ticket_default_permissions()

        if not current_perms:
            await ctx.send("❌ No default permissions configured for @everyone in tickets.")
            return

        # Build command-line format
        args = []
        for perm, value in current_perms.items():
            value_str = self._permission_value_to_string(value).lower()
            args.append(f"--{perm} {value_str}")

        cmdline = " ".join(args)

        embed = discord.Embed(
            title="Default Permissions (Command-Line Format)",
            color=await ctx.embed_color(),
            description="Current @everyone permissions in command-line format:"
        )

        # Split into chunks if too long for Discord
        if len(cmdline) > 1024:
            chunks = [cmdline[i:i+1000] for i in range(0, len(cmdline), 1000)]
            for i, chunk in enumerate(chunks, 1):
                embed.add_field(
                    name=f"Arguments ({i})" if len(chunks) > 1 else "Arguments",
                    value=f"```\n{chunk}\n```",
                    inline=False
                )
        else:
            embed.add_field(
                name="Arguments",
                value=f"```\n{cmdline}\n```",
                inline=False
            )

        embed.set_footer(text="Copy and paste these arguments to replicate the configuration")
        await ctx.send(embed=embed)

    @permissions.command(name="staffshow")
    async def show_staff_permissions_cmdline(self, ctx):
        """Show current staff permissions in command-line format for easy copying."""
        current_perms = await self.config.guild(ctx.guild).ticket_staff_permissions()

        if not current_perms:
            await ctx.send("❌ No staff permissions configured for tickets.")
            return

        # Build command-line format
        args = []
        for perm, value in current_perms.items():
            value_str = self._permission_value_to_string(value).lower()
            args.append(f"--{perm} {value_str}")

        cmdline = " ".join(args)

        embed = discord.Embed(
            title="Staff Permissions (Command-Line Format)",
            color=await ctx.embed_color(),
            description="Current staff permissions in command-line format:"
        )

        # Split into chunks if too long for Discord
        if len(cmdline) > 1024:
            chunks = [cmdline[i:i+1000] for i in range(0, len(cmdline), 1000)]
            for i, chunk in enumerate(chunks, 1):
                embed.add_field(
                    name=f"Arguments ({i})" if len(chunks) > 1 else "Arguments",
                    value=f"```\n{chunk}\n```",
                    inline=False
                )
        else:
            embed.add_field(
                name="Arguments",
                value=f"```\n{cmdline}\n```",
                inline=False
            )

        embed.set_footer(text="Copy and paste these arguments to replicate the configuration")
        await ctx.send(embed=embed)

    @permissions.command(name="openershow")
    async def show_opener_permissions_cmdline(self, ctx):
        """Show current opener permissions in command-line format for easy copying."""
        current_perms = await self.config.guild(ctx.guild).ticket_opener_permissions()

        if not current_perms:
            await ctx.send("❌ No opener permissions configured for tickets.")
            return

        # Build command-line format
        args = []
        for perm, value in current_perms.items():
            value_str = self._permission_value_to_string(value).lower()
            args.append(f"--{perm} {value_str}")

        cmdline = " ".join(args)

        embed = discord.Embed(
            title="Opener Permissions (Command-Line Format)",
            color=await ctx.embed_color(),
            description="Current opener permissions in command-line format:"
        )

        # Split into chunks if too long for Discord
        if len(cmdline) > 1024:
            chunks = [cmdline[i:i+1000] for i in range(0, len(cmdline), 1000)]
            for i, chunk in enumerate(chunks, 1):
                embed.add_field(
                    name=f"Arguments ({i})" if len(chunks) > 1 else "Arguments",
                    value=f"```\n{chunk}\n```",
                    inline=False
                )
        else:
            embed.add_field(
                name="Arguments",
                value=f"```\n{cmdline}\n```",
                inline=False
            )

        embed.set_footer(text="Copy and paste these arguments to replicate the configuration")
        await ctx.send(embed=embed)

    @permissions.command(name="options")
    async def show_available_options(self, ctx):
        """Show all available permission options in command-line format."""
        perms = self._get_all_discord_permissions()

        embed = discord.Embed(
            title="Available Permission Options",
            color=await ctx.embed_color(),
            description="All available Discord permissions you can configure with --option format:"
        )

        # Create command-line format list
        options = [f"--{perm}" for perm in perms]

        # Split into chunks for display
        chunks = [options[i:i+15] for i in range(0, len(options), 15)]

        for i, chunk in enumerate(chunks, 1):
            option_text = " ".join(chunk)
            # Split further if still too long
            if len(option_text) > 1024:
                # Split by spaces into smaller chunks
                words = chunk
                sub_chunks = []
                current_chunk = []
                current_length = 0

                for word in words:
                    if current_length + len(word) + 1 > 1000:  # +1 for space
                        sub_chunks.append(" ".join(current_chunk))
                        current_chunk = [word]
                        current_length = len(word)
                    else:
                        current_chunk.append(word)
                        current_length += len(word) + 1

                if current_chunk:
                    sub_chunks.append(" ".join(current_chunk))

                for j, sub_chunk in enumerate(sub_chunks):
                    field_name = f"Options ({i}.{j+1})" if len(sub_chunks) > 1 else f"Options ({i})"
                    embed.add_field(
                        name=field_name,
                        value=f"```\n{sub_chunk}\n```",
                        inline=False
                    )
            else:
                embed.add_field(
                    name=f"Options ({i})",
                    value=f"```\n{option_text}\n```",
                    inline=False
                )

        embed.add_field(
            name="💡 Usage",
            value=(
                "Use these options with permission commands:\n"
                "`defaultset --view_channel deny --send_messages allow`\n"
                "`staffset --manage_messages allow --kick_members deny`\n"
                "\nValues: allow, deny, passthrough"
            ),
            inline=False
        )

        await ctx.send(embed=embed)

    @permissions.command(name="showall")
    async def show_all_permissions_cmdline(self, ctx):
        """Show all current permission configurations in command-line format."""
        conf = await self.config.guild(ctx.guild).all()

        embed = discord.Embed(
            title="All Permission Configurations (Command-Line Format)",
            color=await ctx.embed_color(),
            description="All current permission settings in command-line format for easy copying:"
        )

        # Default permissions
        default_perms = conf["ticket_default_permissions"]
        if default_perms:
            args = []
            for perm, value in default_perms.items():
                value_str = self._permission_value_to_string(value).lower()
                args.append(f"--{perm} {value_str}")

            cmdline = " ".join(args)
            embed.add_field(
                name="🌐 Default Permissions (@everyone)",
                value=f"```defaultset {cmdline}```",
                inline=False
            )

        # Staff permissions
        staff_perms = conf["ticket_staff_permissions"]
        if staff_perms:
            args = []
            for perm, value in staff_perms.items():
                value_str = self._permission_value_to_string(value).lower()
                args.append(f"--{perm} {value_str}")

            cmdline = " ".join(args)
            embed.add_field(
                name="👮 Staff Permissions",
                value=f"```staffset {cmdline}```",
                inline=False
            )

        # Opener permissions
        opener_perms = conf["ticket_opener_permissions"]
        if opener_perms:
            args = []
            for perm, value in opener_perms.items():
                value_str = self._permission_value_to_string(value).lower()
                args.append(f"--{perm} {value_str}")

            cmdline = " ".join(args)
            embed.add_field(
                name="🎫 Opener Permissions",
                value=f"```openerupdate {cmdline}```",
                inline=False
            )

        # Staff roles
        staff_role_ids = conf["ticket_staff_roles"]
        if staff_role_ids:
            roles = [ctx.guild.get_role(rid) for rid in staff_role_ids if ctx.guild.get_role(rid)]
            if roles:
                role_mentions = [role.mention for role in roles]
                embed.add_field(
                    name="👥 Staff Roles",
                    value=f"Configured: {', '.join(role_mentions)}",
                    inline=False
                )

        if not any([default_perms, staff_perms, opener_perms, staff_role_ids]):
            embed.add_field(
                name="ℹ️ No Configurations",
                value="No permission configurations found. Use `options` to see available permissions.",
                inline=False
            )

        embed.set_footer(text="Copy command snippets to replicate configurations")
        await ctx.send(embed=embed)

    @panel.command(name="setchannel")
    async def set_panel_channel(self, ctx, channel: discord.TextChannel):
        """Set the channel for the verification panel."""
        embed_data = await self.config.guild(ctx.guild).panel_embed()
        if not embed_data:
            await ctx.send("❌ You must set up the panel embed first using `[p]ckeytools verify panel setembed`")
            return

        await self.config.guild(ctx.guild).ticket_channel.set(channel.id)
        await ctx.send(f"✅ Panel channel set to {channel.mention}")
        await ctx.tick()

        # Create and send the panel message
        await self.create_panel_message(ctx.guild, channel)

    @panel.command(name="setcategory")
    async def set_ticket_category(self, ctx, category: discord.CategoryChannel):
        """Set the category where verification tickets will be created."""
        await self.config.guild(ctx.guild).ticket_category.set(category.id)
        await ctx.send(f"✅ Ticket category set to {category.name}")
        await ctx.tick()

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
            await ctx.send("**Preview:**", embed=embed)
        except json.JSONDecodeError:
            await ctx.send("❌ Invalid JSON format in the attached file.")
        except Exception as e:
            await ctx.send(f"❌ Error creating embed: {str(e)}")
        else:
            await ctx.send("✅ Panel embed set successfully!")
            await ctx.tick()

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
            await ctx.tick()
        except json.JSONDecodeError:
            await ctx.send("❌ Invalid JSON format in the attached file.")
        except Exception as e:
            await ctx.send(f"❌ Error creating embed: {str(e)}")
        else:
            await ctx.send("✅ Ticket embed set successfully!")
            await ctx.send("**Preview:**", embed=embed)

    @panel.command(name="create")
    async def create_panel(self, ctx):
        """Create the verification panel in the configured channel."""
        channel_id = await self.config.guild(ctx.guild).ticket_channel()
        embed_data = await self.config.guild(ctx.guild).panel_embed()

        if not channel_id:
            await ctx.send("❌ Panel channel not set. Use `[p]ckeytools verify panel setchannel` first.")
            return

        if not embed_data:
            await ctx.send("❌ Panel embed not set. Use `[p]ckeytools verify panel setembed` first.")
            return

        channel = ctx.guild.get_channel(channel_id)
        if not channel:
            await ctx.send("❌ Configured channel not found.")
            return

        await self.create_panel_message(ctx.guild, channel)
        await ctx.send("✅ Verification panel created!")
        await ctx.tick()


    @verify_config.command(name="invalidateonleave")
    async def toggle_invalidate_on_leave(self, ctx, enabled: Optional[bool] = None):
        """
        Toggle whether to invalidate user verification when they leave the server.

        When enabled, if a verified user leaves the Discord server, their latest
        valid verification link will be set as invalid in the database.
        """
        if enabled is None:
            current = await self.config.guild(ctx.guild).invalidate_on_leave()
            await ctx.send(f"Invalidate verification on leave is currently **{'enabled' if current else 'disabled'}**.")
            return

        await self.config.guild(ctx.guild).invalidate_on_leave.set(enabled)
        if enabled:
            await ctx.send("✅ Verified users will now be invalidated when they leave the server.")
        else:
            await ctx.send("✅ Verified users will no longer be invalidated when they leave the server.")
        await ctx.tick()

    @verify_config.command(name="verification")
    async def toggle_verification(self, ctx, enabled: Optional[bool] = None):
        """
        Toggle the entire verification system.

        When disabled, all verification-related functionality will be disabled,
        including tickets, panels, and verification attempts.
        """
        if enabled is None:
            current = await self.config.guild(ctx.guild).verification_enabled()
            await ctx.send(f"Verification system is currently **{'enabled' if current else 'disabled'}**.")
            return

        await self.config.guild(ctx.guild).verification_enabled.set(enabled)
        if enabled:
            await ctx.send("✅ Verification system has been enabled.")
        else:
            await ctx.send("✅ Verification system has been disabled. All verification attempts will fail.")
        await ctx.tick()

    @verify_config.command(name="autoverification")
    async def toggle_autoverification(self, ctx, enabled: Optional[bool] = None):
        """
        Toggle automatic verification functionality.

        When disabled, all auto-verification attempts will fail, requiring
        users to manually enter verification codes in tickets.
        """
        if enabled is None:
            current = await self.config.guild(ctx.guild).autoverification_enabled()
            await ctx.send(f"Auto-verification is currently **{'enabled' if current else 'disabled'}**.")
            return

        await self.config.guild(ctx.guild).autoverification_enabled.set(enabled)
        if enabled:
            await ctx.send("✅ Auto-verification has been enabled.")
        else:
            await ctx.send("✅ Auto-verification has been disabled. Users will need to manually enter codes.")
        await ctx.tick()

    @verify_config.command(name="autoverifyonjoin")
    async def toggle_autoverify_on_join(self, ctx, enabled: Optional[bool] = None):
        """
        Toggle automatic verification when users join the server.

        When disabled, users will not receive DM verification attempts when
        they join the server, but auto-verification in tickets may still work
        (if autoverification is enabled).
        """
        if enabled is None:
            current = await self.config.guild(ctx.guild).autoverify_on_join_enabled()
            await ctx.send(f"Auto-verification on join is currently **{'enabled' if current else 'disabled'}**.")
            return

        await self.config.guild(ctx.guild).autoverify_on_join_enabled.set(enabled)
        if enabled:
            await ctx.send("✅ Auto-verification on join has been enabled.")
        else:
            await ctx.send("✅ Auto-verification on join has been disabled.")
        await ctx.tick()

    @verify_config.command(name="invalidategone")
    @checks.admin_or_permissions(administrator=True)
    async def invalidate_gone_users(self, ctx):
        """
        Manually invalidate verification for all users who are no longer in the server.

        This command will check all verified users in the database and invalidate
        those who are no longer members of this Discord server.
        """
        async with ctx.typing():
            try:
                if not self.db_manager.is_connected(ctx.guild.id):
                    await ctx.send("❌ Database is not connected. Please configure the database connection first.")
                    return

                # Get all valid links for this server
                links = await self.db_manager.get_all_valid_links(ctx.guild.id)

                invalidated_count = 0
                for link in links:
                    discord_id = link.discord_id
                    member = ctx.guild.get_member(discord_id)
                    if not member:  # User is no longer in the server
                        # Invalidate their link
                        count = await self.db_manager.invalidate_links_by_discord_id(ctx.guild.id, discord_id)
                        invalidated_count += count
                        self.log.info(f"Invalidated verification link for user {discord_id} (ckey: {link.ckey}) who left {ctx.guild.name}")

                await ctx.send(f"✅ **{invalidated_count}** verification links have been invalidated for users who left the server.")
                await ctx.tick()

            except Exception as e:
                self.log.error(f"Error during manual invalidation in {ctx.guild.name}: {e}")
                await ctx.send("❌ An error occurred while invalidating gone users. Check the logs for details.")

    @verify.command()
    async def status(self, ctx):
        """Show the current CkeyTools configuration status."""
        conf = await self.config.guild(ctx.guild).all()

        # Database configuration status
        db_configured = all([conf["db_host"], conf["db_port"], conf["db_user"], conf["db_password"], conf["db_name"]])
        db_status = "✅ Configured" if db_configured else "❌ Not configured"

        # Panel configuration status
        panel_channel = ctx.guild.get_channel(conf["ticket_channel"]) if conf["ticket_channel"] else None
        ticket_category = ctx.guild.get_channel(conf["ticket_category"]) if conf["ticket_category"] else None

        # Verification roles
        role_ids = conf["verification_roles"]
        roles = [ctx.guild.get_role(rid) for rid in role_ids if ctx.guild.get_role(rid)]

        embed = discord.Embed(
            title="CkeyTools Configuration Status",
            color=await ctx.embed_color(),
            timestamp=discord.utils.utcnow()
        )

        embed.add_field(
            name="🗄️ Database",
            value=f"{db_status}\nConnected: {'✅ Yes' if self.db_manager.is_connected(ctx.guild.id) else '❌ No'}",
            inline=True
        )

        embed.add_field(
            name="📋 Panel Channel",
            value=panel_channel.mention if panel_channel else "❌ Not set",
            inline=True
        )

        embed.add_field(
            name="📁 Ticket Category",
            value=ticket_category.name if ticket_category else "❌ Not set",
            inline=True
        )

        embed.add_field(
            name="🎭 Verification Roles",
            value=f"{len(roles)} configured" if roles else "❌ None set",
            inline=True
        )

        embed.add_field(
            name="📜 Panel Embed",
            value="✅ Configured" if conf["panel_embed"] else "❌ Not set",
            inline=True
        )

        embed.add_field(
            name="🎫 Ticket Embed",
            value="✅ Configured" if conf["ticket_embed"] else "❌ Not set",
            inline=True
        )

        embed.add_field(
            name="🚪 Invalidate on Leave",
            value="✅ Enabled" if conf["invalidate_on_leave"] else "❌ Disabled",
            inline=True
        )

        # Add toggle settings
        embed.add_field(
            name="🔧 Verification System",
            value="✅ Enabled" if conf["verification_enabled"] else "❌ Disabled",
            inline=True
        )

        embed.add_field(
            name="🤖 Auto-Verification",
            value="✅ Enabled" if conf["autoverification_enabled"] else "❌ Disabled",
            inline=True
        )

        embed.add_field(
            name="👋 Auto-Verify on Join",
            value="✅ Enabled" if conf["autoverify_on_join_enabled"] else "❌ Disabled",
            inline=True
        )

        # Add deverified users count
        deverified_count = len(conf["deverified_users"])
        embed.add_field(
            name="🚫 Deverified Users",
            value=f"{deverified_count} users" if deverified_count > 0 else "None",
            inline=True
        )

        # Permission system status
        staff_role_count = len(conf["ticket_staff_roles"])
        default_perm_count = len(conf["ticket_default_permissions"])
        staff_perm_count = len(conf["ticket_staff_permissions"])
        opener_perm_count = len(conf["ticket_opener_permissions"])

        embed.add_field(
            name="🔐 Ticket Permissions",
            value=f"Staff Roles: {staff_role_count}\nDefault Perms: {default_perm_count}\nStaff Perms: {staff_perm_count}\nOpener Perms: {opener_perm_count}",
            inline=True
        )

        await ctx.send(embed=embed)

    @verify.command()
    async def checkuser(self, ctx, user: discord.Member):
        """Check the verification status of a user."""
        try:
            # Check for valid links
            link = None
            if self.db_manager.is_connected(ctx.guild.id):
                link = await self.db_manager.get_valid_link_by_discord_id(ctx.guild.id, user.id)

            embed = discord.Embed(
                title=f"Verification Status: {user.display_name}",
                color=await ctx.embed_color(),
                timestamp=discord.utils.utcnow()
            )

            # Check if user has been deverified
            deverified_users = await self.config.guild(ctx.guild).deverified_users()
            is_deverified = user.id in deverified_users

            if link:
                embed.colour = discord.Color.green()
                embed.add_field(name="Status", value="✅ Verified", inline=True)
                embed.add_field(name="Ckey", value=f"`{link.ckey}`", inline=True)
                embed.add_field(name="Linked Since", value=f"<t:{int(link.timestamp.timestamp())}:R>", inline=True)

                # Check if user has verification roles
                role_ids = await self.config.guild(ctx.guild).verification_roles()
                roles = [ctx.guild.get_role(rid) for rid in role_ids if ctx.guild.get_role(rid)]
                user_has_roles = any(role in user.roles for role in roles)
                embed.add_field(
                    name="Has Verification Roles",
                    value="✅ Yes" if user_has_roles else "❌ No",
                    inline=True
                )
            else:
                embed.colour = discord.Color.red()
                embed.add_field(name="Status", value="❌ Not verified", inline=True)

                # Check for open ticket
                open_ticket = await self.config.member(user).open_ticket()
                if open_ticket:
                    channel = ctx.guild.get_channel(open_ticket)
                    if channel:
                        embed.add_field(name="Open Ticket", value=channel.mention, inline=True)

            # Show deverified status
            embed.add_field(
                name="Deverified",
                value="🚫 Yes (Auto-verify blocked)" if is_deverified else "✅ No",
                inline=True
            )

            embed.set_thumbnail(url=user.display_avatar.url)
            await ctx.send(embed=embed)

        except Exception as e:
            self.log.error(f"Error checking user verification status: {e}")
            await ctx.send("❌ Error checking verification status.")

    @verify.command()
    async def ckeys(self, ctx, user: discord.User):
        """List all past ckeys this Discord user has verified with.

        Args:
            user: Discord user (works for users both in and outside the server)
        """
        if not self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("❌ Database is not connected.")
            return

        message = await ctx.send("Collecting ckeys for Discord user...")
        async with ctx.typing():
            try:
                links = await self.db_manager.get_all_links_by_discord_id(ctx.guild.id, user.id)

                embed = discord.Embed(color=await ctx.embed_color())
                embed.set_author(
                    name=f"Ckeys historically linked to {user.display_name}"
                )
                embed.set_thumbnail(url=user.display_avatar.url)

                if len(links) <= 0:
                    return await message.edit(
                        content="No ckeys found for this Discord user", embed=None
                    )

                # Check if user has been deverified
                deverified_users = await self.config.guild(ctx.guild).deverified_users()
                is_deverified = user.id in deverified_users

                names = ""
                for link in links:
                    validity_text = "✅ Valid" if link.valid else "❌ Invalid"
                    timestamp = link.timestamp
                    names += f"Ckey `{link.ckey}` linked on <t:{int(timestamp.timestamp())}:f>, status: {validity_text}\n"

                if len(names) > 1024:  # Discord embed field limit
                    # Split into multiple fields if too long
                    chunks = []
                    current_chunk = ""
                    for line in names.split('\n'):
                        if len(current_chunk + line + '\n') > 1024:
                            chunks.append(current_chunk)
                            current_chunk = line + '\n'
                        else:
                            current_chunk += line + '\n'
                    if current_chunk:
                        chunks.append(current_chunk)

                    for i, chunk in enumerate(chunks):
                        field_name = "__Ckeys__" if i == 0 else f"__Ckeys (cont. {i+1})__"
                        embed.add_field(name=field_name, value=chunk.strip(), inline=False)
                else:
                    embed.add_field(name="__Ckeys__", value=names.strip(), inline=False)

                # Add deverified status
                if is_deverified:
                    embed.add_field(
                        name="⚠️ Notice",
                        value="This user has been manually deverified and cannot auto-verify.",
                        inline=False
                    )

                await message.edit(content=None, embed=embed)

            except Exception as e:
                self.log.error(f"Error getting ckeys for user {user.id}: {e}")
                await message.edit(content="❌ Error retrieving ckey history.")

    @verify.command()
    async def discords(self, ctx, ckey: str):
        """List all past Discord accounts this ckey has verified with."""
        if not self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("❌ Database is not connected.")
            return

        ckey = normalise_to_ckey(ckey)
        message = await ctx.send("Collecting Discord accounts for ckey...")
        async with ctx.typing():
            try:
                links = await self.db_manager.get_all_links_by_ckey(ctx.guild.id, ckey)

                embed = discord.Embed(color=await ctx.embed_color())
                embed.set_author(
                    name=f"Discord accounts historically linked to {str(ckey).title()}"
                )

                if len(links) <= 0:
                    return await message.edit(
                        content="No Discord accounts found for this ckey", embed=None
                    )

                names = ""
                for link in links:
                    validity_text = "✅ Valid" if link.valid else "❌ Invalid"
                    timestamp = link.timestamp
                    discord_id = link.discord_id
                    if discord_id:
                        names += f"User <@{discord_id}> linked on <t:{int(timestamp.timestamp())}:f>, status: {validity_text}\n"
                    else:
                        names += f"Unlinked token created on <t:{int(timestamp.timestamp())}:f>, status: {validity_text}\n"

                if len(names) > 1024:  # Discord embed field limit
                    # Split into multiple fields if too long
                    chunks = []
                    current_chunk = ""
                    for line in names.split('\n'):
                        if len(current_chunk + line + '\n') > 1024:
                            chunks.append(current_chunk)
                            current_chunk = line + '\n'
                        else:
                            current_chunk += line + '\n'
                    if current_chunk:
                        chunks.append(current_chunk)

                    for i, chunk in enumerate(chunks):
                        field_name = "__Discord accounts__" if i == 0 else f"__Discord accounts (cont. {i+1})__"
                        embed.add_field(name=field_name, value=chunk.strip(), inline=False)
                else:
                    embed.add_field(name="__Discord accounts__", value=names.strip(), inline=False)

                await message.edit(content=None, embed=embed)

            except Exception as e:
                self.log.error(f"Error getting Discord accounts for ckey {ckey}: {e}")
                await message.edit(content="❌ Error retrieving Discord account history.")

    @commands.command()
    @commands.guild_only()
    async def deverify(self, ctx: commands.Context, user: Optional[discord.Member] = None):
        """
        Deverify a user, removing their verification and preventing auto-verification.

        Users can deverify themselves, or admins can deverify others.
        The user will be kicked from the server and can rejoin to verify with a new ckey.
        """
        # Determine target user
        target_user = user if user else ctx.author
        if not isinstance(target_user, discord.Member):
            await ctx.send("❌ Target user must be a guild member.")
            return

        # Permission check: users can only deverify themselves, admins can deverify anyone
        if target_user != ctx.author:
            if not isinstance(ctx.author, discord.Member) or not (ctx.author.guild_permissions.kick_members or
                   ctx.author.guild_permissions.administrator or
                   any(role.permissions.kick_members or role.permissions.administrator for role in ctx.author.roles)):
                await ctx.send("❌ You don't have permission to deverify other users.")
                return

        # Check if database is connected
        if not ctx.guild or not self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("❌ Database is not connected.")
            return

        # Send confirmation view
        if not isinstance(ctx.author, discord.Member):
            await ctx.send("❌ This command must be used in a guild.")
            return
        view = DeverifyConfirmView(self, ctx.author, target_user)
        if target_user == ctx.author:
            confirmation_msg = f"Are you sure you want to deverify yourself? This will:\n• Remove your current verification\n• Kick you from the server\n• Prevent auto-verification until you verify with a new ckey\n• Allow you to rejoin and verify with a different ckey"
        else:
            confirmation_msg = f"Are you sure you want to deverify {target_user.mention}? This will:\n• Remove their current verification\n• Kick them from the server\n• Prevent auto-verification until they verify with a new ckey\n• Allow them to rejoin and verify with a different ckey"

        await ctx.send(confirmation_msg, view=view)

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

    async def fetch_latest_discord_link(self, guild, discord_id):
        """Fetch the latest discord_links entry for a discord_id, ordered by timestamp desc."""
        if not self.db_manager.is_connected(guild.id):
            self.log.warning(f"Database not connected for guild {guild.name} when fetching latest link for user {discord_id}")
            # Try to reconnect automatically
            self.log.info(f"Attempting to reconnect database for guild {guild.name}")
            await self.reconnect_database(guild)

            # Check again after reconnection attempt
            if not self.db_manager.is_connected(guild.id):
                self.log.error(f"Failed to reconnect database for guild {guild.name}")
                return None

        try:
            link = await self.db_manager.get_latest_link_by_discord_id(guild.id, discord_id)
            return link.to_dict() if link else None
        except Exception as e:
            self.log.error(f"Error fetching latest link for user {discord_id} in guild {guild.name}: {e}")
            return None

    async def fetch_valid_discord_link(self, guild, discord_id):
        """Fetch the latest valid discord link for a user."""
        if not self.db_manager.is_connected(guild.id):
            self.log.warning(f"Database not connected for guild {guild.name} when checking valid link for user {discord_id}")
            # Try to reconnect automatically
            self.log.info(f"Attempting to reconnect database for guild {guild.name}")
            await self.reconnect_database(guild)

            # Check again after reconnection attempt
            if not self.db_manager.is_connected(guild.id):
                self.log.error(f"Failed to reconnect database for guild {guild.name}")
                return None

        try:
            link = await self.db_manager.get_valid_link_by_discord_id(guild.id, discord_id)
            return link.to_dict() if link else None
        except Exception as e:
            self.log.error(f"Error fetching valid link for user {discord_id} in guild {guild.name}: {e}")
            return None

    async def is_user_verified(self, guild, user):
        """Check if a user is already verified with a valid link."""
        self.log.info(f"Checking if user {user} ({user.id}) is already verified in guild {guild.name}")

        try:
            valid_link = await self.fetch_valid_discord_link(guild, user.id)
            is_verified = valid_link is not None
            self.log.info(f"User {user} verification status: {is_verified}")
            return is_verified
        except Exception as e:
            self.log.error(f"Error checking verification status for user {user}: {e}")
            return False

    async def ensure_user_roles(self, guild, user):
        """Ensure a verified user has the correct roles."""
        try:
            verification_roles = await self.config.guild(guild).verification_roles()
            if not verification_roles:
                return

            member = guild.get_member(user.id)
            if not member:
                return

            roles_to_add = []
            for role_id in verification_roles:
                role = guild.get_role(role_id)
                if role and role not in member.roles:
                    roles_to_add.append(role)

            if roles_to_add:
                await member.add_roles(*roles_to_add, reason="Ensuring verified user has correct roles")
                self.log.info(f"Added missing verification roles to {member} in {guild}")
        except Exception as e:
            self.log.error(f"Error ensuring roles for {user} in {guild}: {e}")

    def generate_auto_token(self, original_token, dt):
        """Generate a new one_time_token for auto-verification based on the original token and datetime."""
        hash_input = f"{original_token}:{dt.isoformat()}"
        return hashlib.sha256(hash_input.encode()).hexdigest()

    async def create_auto_link(self, guild, ckey, discord_id, original_token):
        """Create a new valid discord_links entry for auto-verification."""
        if not self.db_manager.is_connected(guild.id):
            raise RuntimeError(f"Database not connected for guild {guild.name}")

        now = datetime.datetime.now(datetime.timezone.utc)
        new_token = self.generate_auto_token(original_token, now)

        try:
            # First, invalidate all previous valid links for this ckey and discord_id
            invalidated_count = await self.db_manager.invalidate_previous_links(guild.id, ckey, discord_id)
            if invalidated_count > 0:
                self.log.info(f"Invalidated {invalidated_count} previous links before creating new auto link for {ckey} and discord_id {discord_id}")

            # Then create the new valid link
            await self.db_manager.create_link(
                guild_id=guild.id,
                ckey=ckey,
                discord_id=discord_id,
                one_time_token=new_token,
                valid=True
            )
            return new_token
        except Exception as e:
            self.log.error(f"Error creating auto link for {ckey} and discord_id {discord_id}: {e}")
            raise

    async def try_auto_verification(self, guild, user, channel=None, dm=False):
        """Attempt to auto-verify a user based on previous discord_links.
        If channel is provided, send messages there. If dm=True, send DMs to the user.
        Returns (success: bool, ckey: str or None)
        """
        # First check if user is already verified
        is_verified = await self.is_user_verified(guild, user)
        if is_verified:
            self.log.info(f"User {user} is already verified, skipping auto-verification")
            # User already has a valid link, just ensure they have roles
            await self.ensure_user_roles(guild, user)

            # Get their ckey for the message
            try:
                valid_link = await self.fetch_valid_discord_link(guild, user.id)
                ckey = valid_link.get('ckey', 'Unknown') if valid_link else 'Unknown'

                if channel:
                    await channel.send(f"You are already verified as `{ckey}`. Your roles have been updated if needed.")
                elif dm:
                    # For DMs, just send the success embed directly since they're already verified
                    await self.send_verification_success_dm(guild, user, ckey)

                return True, ckey
            except Exception as e:
                self.log.error(f"Error handling already verified user {user}: {e}")
                return False, None

        # Check if auto-verification is enabled
        autoverification_enabled = await self.config.guild(guild).autoverification_enabled()

        # Check if user has been manually deverified
        deverified_users = await self.config.guild(guild).deverified_users()
        user_is_deverified = user.id in deverified_users

        if not autoverification_enabled or user_is_deverified:
            # Auto-verification is disabled or user is deverified, simulate as if no link was found
            link = None
        else:
            link = await self.fetch_latest_discord_link(guild, user.id)

        if channel:
            msg = await channel.send("Attempting to auto verify...")
            async with channel.typing():
                if link:
                    ckey = link["ckey"]
                    original_token = link["one_time_token"]
                    new_token = await self.create_auto_link(guild, ckey, user.id, original_token)
                    await msg.edit(content=f"Automatic verification completed! Welcome back, `{ckey}`.")
                    return True, ckey
                else:
                    await msg.delete()
                    return False, None
        elif dm:
            try:
                dm_channel = user.dm_channel or await user.create_dm()
                typing_ctx = dm_channel.typing() if hasattr(dm_channel, 'typing') else None
                if typing_ctx:
                    await typing_ctx.__aenter__()
                dm_message = await dm_channel.send("Attempting to auto verify...")
                if link:
                    ckey = link["ckey"]
                    original_token = link["one_time_token"]
                    new_token = await self.create_auto_link(guild, ckey, user.id, original_token)
                    # Delete the "attempting" message since finish_verification will send the success DM embed
                    await dm_message.delete()
                    if typing_ctx:
                        await typing_ctx.__aexit__(None, None, None)
                    return True, ckey
                else:
                    # Fetch panel channel and message link
                    panel_channel_id = await self.config.guild(guild).ticket_channel()
                    panel_message_id = await self.config.guild(guild).panel_message()
                    panel_channel = guild.get_channel(panel_channel_id) if panel_channel_id else None
                    panel_channel_mention = panel_channel.mention if panel_channel else "the verification panel channel"
                    panel_message_link = None
                    if panel_channel_id and panel_message_id:
                        panel_message_link = f"https://discord.com/channels/{guild.id}/{panel_channel_id}/{panel_message_id}"

                    if user_is_deverified:
                        msg = f"You have been manually deverified and cannot auto-verify. Please use the verification panel at {panel_channel_mention} to verify with a new ckey."
                    elif not autoverification_enabled:
                        msg = f"Auto-verification is currently disabled. Please use the verification panel at {panel_channel_mention} to verify manually."
                    else:
                        msg = f"It seems you have no account linked. Please make sure to link your discord account to your ckey at {panel_channel_mention} in order to verify!"

                    if panel_message_link:
                        msg += f"\n<{panel_message_link}>"
                    await dm_message.edit(content=msg)
                    if typing_ctx:
                        await typing_ctx.__aexit__(None, None, None)
                    return False, None
            except Exception as e:
                self.log.warning(f"Failed to DM user {user}: {e}")
                return False, None
        else:
            # No channel or DM context provided
            return False, None

    async def finish_verification(self, guild, user, ckey, ticket_channel=None, dm_channel=None):
        """Assign roles, send confirmation, and close the ticket if needed."""
        # Remove user from deverified list if they were there
        deverified_users = await self.config.guild(guild).deverified_users()
        if user.id in deverified_users:
            deverified_users.remove(user.id)
            await self.config.guild(guild).deverified_users.set(deverified_users)

        # Assign roles using the helper function
        await self.ensure_user_roles(guild, user)

        # Send comprehensive DM embed to user
        await self.send_verification_success_dm(guild, user, ckey)

        # Send confirmation (only for ticket channels, not DMs since they get the embed)
        if ticket_channel:
            msg = f"Verification completed! Welcome, `{ckey}`."
            await ticket_channel.send(msg)
            # Wait a few seconds so the user can read the success message
            await asyncio.sleep(3)
            try:
                await ticket_channel.delete(reason="Verification completed")
            except Exception:
                pass
            await self.config.member(user).open_ticket.clear()
        # For DMs, the comprehensive embed is sufficient, no need for additional message

    async def send_verification_success_dm(self, guild, user, ckey):
        """Send a comprehensive DM embed to the user after successful verification."""
        try:
            # Get bot prefix for deverify command instruction
            prefix = await self.bot.get_valid_prefixes(guild)
            bot_prefix = prefix[0] if prefix else "!"

            # Get or create an invite URL for the guild
            invite_url = await self.get_or_create_guild_invite(guild)

            # Create the embed
            embed = discord.Embed(
                title="Discord and ckey linked!",
                description=f"Your Discord account has been successfully linked to your SS13 ckey!\n\n"
                           f"If you want to link a different ckey in the future, you can use `{bot_prefix}deverify` "
                           f"to unlink your current ckey and verify with a new one.",
                color=discord.Color.green(),
                timestamp=discord.utils.utcnow()
            )

            # Set author to guild name with guild icon and invite URL (if available)
            author_kwargs = {
                "name": guild.name,
                "icon_url": guild.icon.url if guild.icon else None
            }
            if invite_url:
                author_kwargs["url"] = invite_url
            embed.set_author(**author_kwargs)

            # Set thumbnail to user's avatar
            embed.set_thumbnail(url=user.display_avatar.url)

            # Add field showing the linked ckey
            embed.add_field(
                name="🔗 Linked Ckey",
                value=f"`{ckey}`",
                inline=False
            )

            # Set footer with guild name and verification system
            embed.set_footer(
                text=f"{guild.name} • Discord Verification",
                icon_url=guild.icon.url if guild.icon else None
            )

            # Send DM
            dm_channel = user.dm_channel or await user.create_dm()
            await dm_channel.send(embed=embed)

        except Exception as e:
            self.log.warning(f"Failed to send verification success DM to {user}: {e}")

    async def get_or_create_guild_invite(self, guild):
        """Get an existing unlimited invite or create a new one for the guild."""
        try:
            # First, try to find an existing unlimited invite
            invites = await guild.invites()
            for invite in invites:
                # Look for invites that never expire and have unlimited uses
                if invite.max_age == 0 and invite.max_uses == 0:
                    self.log.debug(f"Found existing unlimited invite for {guild.name}: {invite.url}")
                    return invite.url

            # No unlimited invite found, try to create one
            # Prefer the ticket channel first, then any channel we can create invites in
            invite_channel = None

            # Try ticket channel first
            ticket_channel_id = await self.config.guild(guild).ticket_channel()
            if ticket_channel_id:
                ticket_channel = guild.get_channel(ticket_channel_id)
                if ticket_channel and ticket_channel.permissions_for(guild.me).create_instant_invite:
                    invite_channel = ticket_channel

            # Fall back to any text channel we can create invites in
            if not invite_channel:
                for channel in guild.text_channels:
                    if channel.permissions_for(guild.me).create_instant_invite:
                        invite_channel = channel
                        break

            if invite_channel:
                invite = await invite_channel.create_invite(
                    max_age=0,      # Never expires
                    max_uses=0,     # Unlimited uses
                    reason="CkeyTools DM embed invite link"
                )
                self.log.info(f"Created new unlimited invite for {guild.name}: {invite.url}")
                return invite.url
            else:
                self.log.warning(f"No suitable channel found to create invite for {guild.name}")
                return None

        except Exception as e:
            self.log.warning(f"Failed to get or create invite for {guild.name}: {e}")
            return None

    async def perform_deverify(self, guild, target_user, command_author):
        """Perform the actual deverification process."""
        try:
            # Invalidate all valid links for the user in the database
            affected_rows = 0
            if self.db_manager.is_connected(guild.id):
                affected_rows = await self.db_manager.invalidate_links_by_discord_id(guild.id, target_user.id)

            # Add user to deverified list
            deverified_users = await self.config.guild(guild).deverified_users()
            if target_user.id not in deverified_users:
                deverified_users.append(target_user.id)
                await self.config.guild(guild).deverified_users.set(deverified_users)

            # Remove verification roles
            role_ids = await self.config.guild(guild).verification_roles()
            roles = [guild.get_role(rid) for rid in role_ids if guild.get_role(rid)]
            if roles:
                try:
                    await target_user.remove_roles(*roles, reason="User deverified")
                except Exception as e:
                    self.log.warning(f"Failed to remove roles from {target_user}: {e}")

            # Close any open verification ticket
            open_ticket = await self.config.member(target_user).open_ticket()
            if open_ticket:
                channel = guild.get_channel(open_ticket)
                if channel:
                    try:
                        await channel.delete(reason="User deverified")
                    except Exception as e:
                        self.log.warning(f"Failed to delete verification ticket for {target_user}: {e}")
                await self.config.member(target_user).open_ticket.clear()

            # Try to send DM with invite
            try:
                dm_channel = target_user.dm_channel or await target_user.create_dm()

                # Get or create permanent invite
                invite_url = await self.get_or_create_guild_invite(guild)

                dm_msg = f"You have been deverified from **{guild.name}**.\n\n"
                dm_msg += "This means your current verification has been removed and you can now verify with a different ckey.\n\n"
                dm_msg += f"You can rejoin the server to verify with a new account"
                if invite_url:
                    dm_msg += f": {invite_url}"
                else:
                    dm_msg += "."

                await dm_channel.send(dm_msg)

            except Exception as e:
                self.log.warning(f"Failed to DM {target_user} about deverification: {e}")

            # Kick the user
            try:
                kick_reason = f"Deverified by {command_author} - can rejoin to verify with new ckey"
                await target_user.kick(reason=kick_reason)
            except Exception as e:
                self.log.error(f"Failed to kick {target_user} after deverification: {e}")
                return False, f"Failed to kick user: {e}"

            self.log.info(f"User {target_user} ({target_user.id}) was deverified by {command_author} in {guild.name}")
            return True, f"Successfully deverified {target_user.mention}. They have been kicked and can rejoin to verify with a new ckey."

        except Exception as e:
            self.log.error(f"Error during deverification of {target_user}: {e}")
            return False, f"An error occurred during deverification: {e}"

    async def send_verification_prompt(self, user, ticket_channel, ticket_embed_data):
        """Send the ticket embed with a button to open the verification modal."""
        embed = discord.Embed.from_dict(ticket_embed_data)
        view = VerificationCodeView(self, user, ticket_channel.guild)
        await ticket_channel.send(f"{user.mention}", embed=embed, view=view)

    async def verify_code(self, guild, user, code):
        """Check if the code matches a valid, unlinked one_time_token in the database."""
        if not self.db_manager.is_connected(guild.id):
            return False, None

        try:
            link = await self.db_manager.verify_code(guild.id, code, user.id)
            if link:
                return True, link.ckey
            else:
                return False, None
        except Exception as e:
            self.log.error(f"Error verifying code for user {user} in guild {guild.name}: {e}")
            return False, None

    async def create_verification_ticket(self, interaction: discord.Interaction, user: discord.Member, category: discord.CategoryChannel, ticket_embed_data):
        guild = interaction.guild
        overwrites = await self._build_ticket_overwrites(guild, user)
        channel_name = f"verify-{user.name}"
        try:
            if not guild:
                await interaction.response.send_message("❌ Guild not found.", ephemeral=True)
                return
            ticket_channel = await guild.create_text_channel(
                name=channel_name,
                category=category,
                overwrites=overwrites,
                reason=f"Verification ticket for {user}"
            )
            await self.config.member(user).open_ticket.set(ticket_channel.id)
            # Set channel topic with opener id for convenience (best-effort)
            try:
                await ticket_channel.edit(topic=f"Verification ticket • opener_id={user.id}")
            except Exception:
                pass
            # Attempt auto-verification
            auto_verified, ckey = await self.try_auto_verification(guild, user, channel=ticket_channel, dm=False)
            if auto_verified:
                await self.finish_verification(guild, user, ckey, ticket_channel=ticket_channel)
                await interaction.response.send_message(
                    f"✅ Automatic verification completed! Welcome, `{ckey}`.", ephemeral=True
                )
                return
            # If not auto-verified, send the ticket embed with button
            ticket_embed = await self.config.guild(guild).ticket_embed()
            await self.send_verification_prompt(user, ticket_channel, ticket_embed)
            await interaction.response.send_message(
                f"✅ Verification ticket created: {ticket_channel.mention}", ephemeral=True
            )
        except Exception as e:
            self.log.error(f"Error creating verification ticket: {e}")
            await interaction.response.send_message(
                "❌ Error creating verification ticket. Please try again or contact an administrator.", ephemeral=True
            )

    async def handle_verification_request(self, interaction: discord.Interaction):
        """Handle when a user clicks the verification button."""
        guild = interaction.guild
        user = interaction.user

        # Check if verification system is enabled
        verification_enabled = await self.config.guild(guild).verification_enabled()
        if not verification_enabled:
            await interaction.response.send_message(
                "❌ Verification system is currently disabled. Please contact an administrator.",
                ephemeral=True
            )
            return

        # Check if user is already verified
        is_verified = await self.is_user_verified(guild, user)
        if is_verified:
            # Ensure they have the correct roles
            await self.ensure_user_roles(guild, user)

            # Get their ckey for the message
            try:
                valid_link = await self.fetch_valid_discord_link(guild, user.id)
                ckey = valid_link.get('ckey', 'Unknown') if valid_link else 'Unknown'
                await interaction.response.send_message(
                    f"✅ You are already verified as `{ckey}`. Your roles have been updated if needed.",
                    ephemeral=True
                )
            except Exception as e:
                self.log.error(f"Error fetching ckey for already verified user {user}: {e}")
                await interaction.response.send_message(
                    "✅ You are already verified. Your roles have been updated if needed.",
                    ephemeral=True
                )
            return

        # Check if user already has an open ticket
        open_ticket = await self.config.member(user).open_ticket()
        if open_ticket and guild:
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

        if not guild:
            await interaction.response.send_message("❌ Guild not found.", ephemeral=True)
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
        if not isinstance(user, discord.Member):
            await interaction.response.send_message("❌ User must be a guild member.", ephemeral=True)
            return
        if not isinstance(category, discord.CategoryChannel):
            await interaction.response.send_message("❌ Category must be a category channel.", ephemeral=True)
            return
        await self.create_verification_ticket(interaction, user, category, ticket_embed_data)

    @commands.hybrid_command(name="closeverification", description="Close this verification ticket")
    @commands.guild_only()
    async def close_verification(self, ctx: commands.Context, *, reason: Optional[str] = None):
        """Close the current verification ticket (opener or staff roles)."""
        guild = ctx.guild
        channel = ctx.channel

        # Must be a text channel in a guild
        if not isinstance(channel, discord.TextChannel) or guild is None:
            # Use ephemeral reply if slash-invoked
            try:
                interaction = getattr(ctx, "interaction", None)
                if interaction and not interaction.response.is_done():
                    await interaction.response.send_message("❌ This command must be used in a server text channel.", ephemeral=True)
                else:
                    await ctx.send("❌ This command must be used in a server text channel.")
            except Exception:
                pass
            return

        conf = await self.config.guild(guild).all()
        staff_role_ids = set(conf.get("ticket_staff_roles", []))

        # Determine opener via member.open_ticket mapping
        opener_id = None
        try:
            # Fast path: if the command invoker's open_ticket points to this channel, they're the opener
            if isinstance(ctx.author, discord.Member):
                user_open = await self.config.member(ctx.author).open_ticket()
                if user_open == channel.id:
                    opener_id = ctx.author.id
            # Otherwise, scan members with open_ticket pointing to this channel (bounded by channel members)
            if opener_id is None:
                for member in channel.members:
                    try:
                        user_open = await self.config.member(member).open_ticket()
                        if user_open == channel.id:
                            opener_id = member.id
                            break
                    except Exception:
                        continue
        except Exception:
            pass

        # Validate this is a verification ticket known to the system
        if opener_id is None:
            interaction = getattr(ctx, "interaction", None)
            if interaction and not interaction.response.is_done():
                await interaction.response.send_message("❌ This channel is not recognized as a verification ticket.", ephemeral=True)
            else:
                await ctx.send("❌ This channel is not recognized as a verification ticket.")
            return

        # Authorization: opener or staff role
        author: discord.Member = ctx.author  # type: ignore
        is_opener = isinstance(author, discord.Member) and author.id == opener_id
        has_staff_role = isinstance(author, discord.Member) and any((r.id in staff_role_ids) for r in getattr(author, "roles", []))

        if not (is_opener or has_staff_role):
            interaction = getattr(ctx, "interaction", None)
            if interaction and not interaction.response.is_done():
                await interaction.response.send_message("❌ You must be the ticket opener or a staff member to close this ticket.", ephemeral=True)
            else:
                await ctx.send("❌ You must be the ticket opener or a staff member to close this ticket.")
            return

        # Acknowledge and close
        try:
            closing_msg = "Closing this verification ticket..."
            interaction = getattr(ctx, "interaction", None)
            if interaction and not interaction.response.is_done():
                await interaction.response.send_message(closing_msg, ephemeral=is_opener and not has_staff_role)
            else:
                await ctx.send(closing_msg)
        except Exception:
            pass

        # Cleanup mappings
        try:
            # Clear member's open_ticket
            try:
                if opener_id is not None:
                    await self.config.member_from_ids(guild.id, opener_id).open_ticket.clear()
            except Exception:
                pass
        except Exception:
            pass

        # Delete the channel
        try:
            delete_reason = f"Ticket closed by {author}"
            if reason:
                delete_reason += f" • {reason}"
            await channel.delete(reason=delete_reason)
        except Exception as e:
            try:
                await ctx.send(f"❌ Failed to delete channel: {e}")
            except Exception:
                pass

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Handle user leaving the server: close verification tickets and optionally invalidate verification."""
        guild = member.guild
        if guild is None:
            return

        # Close verification ticket if user has one open
        open_ticket = await self.config.member(member).open_ticket()
        if open_ticket:
            channel = guild.get_channel(open_ticket)
            if channel:
                try:
                    await channel.delete(reason="User left before finishing verification")
                except Exception as e:
                    self.log.warning(f"Failed to delete verification ticket channel for {member}: {e}")
            await self.config.member(member).open_ticket.clear()

        # Invalidate verification if enabled and database is connected
        invalidate_enabled = await self.config.guild(guild).invalidate_on_leave()
        if invalidate_enabled and self.db_manager.is_connected(guild.id):
            try:
                affected_rows = await self.db_manager.invalidate_links_by_discord_id(guild.id, member.id)
                if affected_rows > 0:
                    self.log.info(f"Invalidated {affected_rows} verification link(s) for {member} ({member.id}) who left {guild.name}")
            except Exception as e:
                self.log.error(f"Failed to invalidate verification for {member} who left {guild.name}: {e}")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Attempt auto-verification when a user joins the server."""
        guild = member.guild

        # Check if verification system is enabled
        conf = await self.config.guild(guild).all()
        if not conf["verification_enabled"]:
            return  # Verification system is disabled

        # Check if auto-verification on join is enabled
        if not conf["autoverify_on_join_enabled"]:
            return  # Auto-verification on join is disabled

        # Only run if DB is configured
        if not all([conf["db_host"], conf["db_port"], conf["db_user"], conf["db_password"], conf["db_name"]]):
            return

        # Try auto-verification and DM the user
        auto_verified, ckey = await self.try_auto_verification(guild, member, channel=None, dm=True)
        if auto_verified:
            # Auto-verification succeeded, now assign roles
            try:
                dm_channel = member.dm_channel or await member.create_dm()
                await self.finish_verification(guild, member, ckey, dm_channel=dm_channel)
            except Exception as e:
                self.log.warning(f"Failed to complete auto-verification for {member}: {e}")
                # Still assign roles even if DM fails
                role_ids = await self.config.guild(guild).verification_roles()
                roles = [guild.get_role(rid) for rid in role_ids if guild.get_role(rid)]
                valid_roles = [role for role in roles if role is not None]
                try:
                    if valid_roles:
                        await member.add_roles(*valid_roles, reason="SS13 auto-verification successful")
                except Exception as role_e:
                    self.log.error(f"Failed to assign roles to {member} during auto-verification: {role_e}")

    # =========================
    # autoroles (ckey list → TOML)
    # =========================

    def _build_nested_dict_with_array(self, base: dict, path: str, values: list):
        """Given a dotted TOML path like 'donator.tier_1', set the array at that key.
        For single-token path like 'group', sets base['group'] = values.
        Merges with existing arrays if present.
        """
        tokens = [p for p in path.split('.') if p]
        if not tokens:
            return base
        current = base
        for parent in tokens[:-1]:
            if parent not in current or not isinstance(current[parent], dict):
                current[parent] = {}
            current = current[parent]
        leaf = tokens[-1]
        existing = current.get(leaf, [])
        if isinstance(existing, list):
            merged = list(dict.fromkeys([*existing, *values]))
            current[leaf] = merged
        else:
            current[leaf] = list(dict.fromkeys(values))
        return base

    # =========================
    # agevet HTTP client methods
    # =========================

    async def _get_agevet_headers(self, guild):
        """Get headers for agevet API requests."""
        api_key = await self.config.guild(guild).agevet_api_key()
        if not api_key:
            raise ValueError("AgeVet API key not configured")
        return {"X-API-Key": api_key, "Content-Type": "application/json"}

    async def _get_agevet_url(self, guild):
        """Get the agevet API URL for the guild."""
        api_url = await self.config.guild(guild).agevet_api_url()
        if not api_url:
            raise ValueError("AgeVet API URL not configured")
        return api_url.rstrip("/") + "/api/agevet/"

    async def _make_agevet_request(self, guild, method, data=None, params=None):
        """Make an HTTP request to the agevet API."""
        try:
            url = await self._get_agevet_url(guild)
            headers = await self._get_agevet_headers(guild)

            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=data,
                    params=params
                ) as response:
                    response_data = await response.json()

                    if response.status >= 400:
                        error_msg = response_data.get('error', f'HTTP {response.status}')
                        raise aiohttp.ClientError(f"API Error: {error_msg}")

                    return response_data, response.status
        except aiohttp.ClientError as e:
            self.log.error(f"AgeVet API request failed: {e}")
            raise
        except Exception as e:
            self.log.error(f"Unexpected error in agevet API request: {e}")
            raise

    async def get_agevet_record(self, guild, ckey):
        """Get an agevet record by ckey."""
        try:
            data, status = await self._make_agevet_request(
                guild, "GET", params={"ckey": ckey, "fields": "ckey,date_of_birth,created_at"}
            )
            return data
        except aiohttp.ClientError as e:
            if "Not found" in str(e):
                return None
            raise

    async def create_agevet_record(self, guild, ckey, date_of_birth):
        """Create a new agevet record."""
        data = {
            "ckey": ckey,
            "date_of_birth": date_of_birth.isoformat()
        }
        try:
            response_data, status = await self._make_agevet_request(guild, "POST", data=data)
            return response_data
        except aiohttp.ClientError as e:
            if "already exists" in str(e).lower() or "unique constraint" in str(e).lower():
                raise ValueError(f"AgeVet record for ckey '{ckey}' already exists")
            raise

    async def update_agevet_record(self, guild, ckey, date_of_birth):
        """Update an existing agevet record."""
        data = {
            "ckey": ckey,
            "date_of_birth": date_of_birth.isoformat()
        }
        try:
            response_data, status = await self._make_agevet_request(guild, "PATCH", data=data)
            return response_data
        except aiohttp.ClientError as e:
            if "Not found" in str(e):
                raise ValueError(f"AgeVet record for ckey '{ckey}' not found")
            raise

    async def delete_agevet_record(self, guild, ckey):
        """Delete an agevet record."""
        data = {"ckey": ckey}
        try:
            response_data, status = await self._make_agevet_request(guild, "DELETE", data=data)
            return response_data
        except aiohttp.ClientError as e:
            if "Not found" in str(e):
                raise ValueError(f"AgeVet record for ckey '{ckey}' not found")
            raise

    def _parse_date_of_birth(self, date_string: str) -> datetime.date:
        """Parse a date string into a date object with flexible format support."""
        if not date_string or not date_string.strip():
            raise ValueError("Date of birth cannot be empty")

        date_string = date_string.strip()

        # Try common formats first
        common_formats = [
            "%Y-%m-%d",      # 2000-01-01
            "%m/%d/%Y",      # 01/01/2000
            "%d/%m/%Y",      # 01/01/2000 (European)
            "%Y/%m/%d",      # 2000/01/01
            "%m-%d-%Y",      # 01-01-2000
            "%d-%m-%Y",      # 01-01-2000 (European)
            "%B %d, %Y",     # January 1, 2000
            "%b %d, %Y",     # Jan 1, 2000
            "%d %B %Y",      # 1 January 2000
            "%d %b %Y",      # 1 Jan 2000
        ]

        for fmt in common_formats:
            try:
                parsed_date = datetime.datetime.strptime(date_string, fmt).date()
                # Validate the date is reasonable (not in the future, not too old)
                today = datetime.date.today()
                if parsed_date > today:
                    raise ValueError("Date of birth cannot be in the future")
                if parsed_date < today - datetime.timedelta(days=365*150):  # 150 years ago
                    raise ValueError("Date of birth seems unreasonably old")
                return parsed_date
            except ValueError:
                continue

        # If common formats fail, try dateutil parser as fallback
        if date_parser is not None:
            try:
                parsed_date = date_parser.parse(date_string, fuzzy=False).date()
                # Validate the date is reasonable
                today = datetime.date.today()
                if parsed_date > today:
                    raise ValueError("Date of birth cannot be in the future")
                if parsed_date < today - datetime.timedelta(days=365*150):  # 150 years ago
                    raise ValueError("Date of birth seems unreasonably old")
                return parsed_date
            except Exception:
                pass  # Fall through to final error

        raise ValueError(f"Could not parse date '{date_string}'. Please use formats like YYYY-MM-DD, MM/DD/YYYY, or 'January 1, 2000'")

    async def _assign_agevet_role(self, guild, user):
        """Assign the agevet role to a user if configured."""
        try:
            agevet_role_id = await self.config.guild(guild).agevet_role()
            if not agevet_role_id:
                return False  # No role configured

            role = guild.get_role(agevet_role_id)
            if not role:
                self.log.warning(f"AgeVet role with ID {agevet_role_id} not found in guild {guild.name}")
                return False

            member = guild.get_member(user.id)
            if not member:
                self.log.warning(f"User {user} not found in guild {guild.name}")
                return False

            if role not in member.roles:
                await member.add_roles(role, reason="Age verification completed")
                self.log.info(f"Assigned agevet role {role.name} to {member} in {guild.name}")
                return True
            else:
                self.log.debug(f"User {member} already has agevet role {role.name}")
                return True
        except Exception as e:
            self.log.error(f"Error assigning agevet role to {user} in {guild.name}: {e}")
            return False

    async def _remove_agevet_role(self, guild, user):
        """Remove the agevet role from a user if configured."""
        try:
            agevet_role_id = await self.config.guild(guild).agevet_role()
            if not agevet_role_id:
                return False  # No role configured

            role = guild.get_role(agevet_role_id)
            if not role:
                self.log.warning(f"AgeVet role with ID {agevet_role_id} not found in guild {guild.name}")
                return False

            member = guild.get_member(user.id)
            if not member:
                self.log.warning(f"User {user} not found in guild {guild.name}")
                return False

            if role in member.roles:
                await member.remove_roles(role, reason="Age verification removed")
                self.log.info(f"Removed agevet role {role.name} from {member} in {guild.name}")
                return True
            else:
                self.log.debug(f"User {member} does not have agevet role {role.name}")
                return True
        except Exception as e:
            self.log.error(f"Error removing agevet role from {user} in {guild.name}: {e}")
            return False

    async def _collect_ckeys_for_role(self, guild: discord.Guild, role: discord.Role) -> list:
        """Collect latest ckeys for all members having the role using the guild DB."""
        if role is None:
            return []
        if not self.db_manager.is_connected(guild.id):
            return []
        ckeys = []
        for member in role.members:
            try:
                link = await self.db_manager.get_latest_link_by_discord_id(guild.id, member.id)
                if link and link.ckey:
                    ckeys.append(link.ckey)
            except Exception as e:
                self.log.debug(f"Failed to get link for member {member} in role {role}: {e}")
        # Deduplicate while preserving order
        return list(dict.fromkeys(ckeys))

    async def rebuild_autoroles_file(self, guild: discord.Guild):
        """Rebuild the TOML file from role→path mappings."""
        conf = await self.config.guild(guild).all()
        folder = conf.get("autoroles_config_folder")
        file_name = conf.get("autoroles_file_name") or "donator.toml"
        role_paths: dict = conf.get("autoroles_role_paths") or {}

        # Resolve folder path
        if not folder:
            folder = os.path.abspath(os.getcwd())
        folder = os.path.abspath(folder)
        os.makedirs(folder, exist_ok=True)
        file_path = os.path.join(folder, file_name)

        # Build nested structure
        output: dict = {}
        for role_id_str, toml_path in role_paths.items():
            try:
                role_id = int(role_id_str)
            except Exception:
                continue
            role = guild.get_role(role_id)
            if not role:
                continue
            ckeys = await self._collect_ckeys_for_role(guild, role)
            if not ckeys:
                # Still create key with empty array so consumers can rely on presence
                self._build_nested_dict_with_array(output, toml_path, [])
            else:
                self._build_nested_dict_with_array(output, toml_path, ckeys)

        # Write TOML using tomlkit to preserve formatting niceties
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(tomlkit.dumps(output))
        except Exception as e:
            self.log.error(f"Failed to write autoroles file for guild {guild.name}: {e}")

    @tasks.loop(minutes=5)
    async def autoroles_update(self):
        """Periodic updater for autoroles TOML file."""
        for guild in self.bot.guilds:
            try:
                enabled = await self.config.guild(guild).autoroles_enabled()
                if enabled:
                    await self.rebuild_autoroles_file(guild)
            except Exception as e:
                self.log.debug(f"autoroles update failed for {guild.name}: {e}")

    @autoroles_update.before_loop
    async def _before_autoroles_update(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """When roles change, refresh the file if autoroles is enabled."""
        if before.guild.id != after.guild.id:
            return
        if set(before.roles) == set(after.roles):
            return
        enabled = await self.config.guild(after.guild).autoroles_enabled()
        if not enabled:
            return
        try:
            await self.rebuild_autoroles_file(after.guild)
        except Exception as e:
            self.log.debug(f"autoroles rebuild on role change failed for {after.guild.name}: {e}")

    # Command surface: [p]ckeytools autoroles ...
    @ckeytools.group()
    @checks.is_owner()
    async def autoroles(self, ctx: commands.Context):
        """Commands for automatic management of donator-style role exports (TOML)."""
        pass

    @autoroles.command(name="update")
    async def autoroles_update_cmd(self, ctx: commands.Context):
        """Manually rebuild the autoroles TOML file."""
        if not ctx.guild:
            await ctx.send("❌ This command must be used in a guild.")
            return
        await self.rebuild_autoroles_file(ctx.guild)
        await ctx.tick()

    @autoroles.command(name="check")
    async def autoroles_check_file(self, ctx: commands.Context):
        """Show the current autoroles TOML file contents."""
        folder = await self.config.guild(ctx.guild).autoroles_config_folder()
        file_name = await self.config.guild(ctx.guild).autoroles_file_name()
        if not folder:
            folder = os.path.abspath(os.getcwd())
        file_path = os.path.abspath(os.path.join(folder, file_name))
        try:
            with open(file_path, "r", encoding="utf-8") as fp:
                await ctx.send(chat_formatting.box(fp.read(), "toml"))
        except FileNotFoundError:
            await ctx.send("❌ autoroles file not found. Try running `update` first.")

    @autoroles.group(name="config")
    async def autoroles_config(self, ctx: commands.Context):
        """Configuration for autoroles exports."""
        pass

    @autoroles_config.command(name="folder")
    async def autoroles_set_folder(self, ctx: commands.Context, *, folder_path: str):
        """Set the target folder where the TOML export will be saved."""
        folder_path = os.path.abspath(folder_path)
        if not (os.path.exists(folder_path) and os.path.isdir(folder_path)):
            await ctx.send("❌ This path is not a valid folder!")
            return
        await self.config.guild(ctx.guild).autoroles_config_folder.set(folder_path)
        await ctx.tick()

    @autoroles_config.command(name="filename")
    async def autoroles_set_filename(self, ctx: commands.Context, *, file_name: str):
        """Set the TOML file name (default: donator.toml)."""
        # Basic sanitization
        file_name = file_name.strip()
        if not file_name.lower().endswith(".toml"):
            file_name += ".toml"
        await self.config.guild(ctx.guild).autoroles_file_name.set(file_name)
        await ctx.tick()

    @autoroles_config.command(name="toggle")
    async def autoroles_toggle(self, ctx: commands.Context, *, on_or_off: Optional[str] = None):
        """Toggle automatic autoroles updates (periodic and on role change)."""
        current = await self.config.guild(ctx.guild).autoroles_enabled()
        if on_or_off is None:
            await ctx.send(f"This option is currently set to {'on' if current else 'off'}")
            return
        on_or_off_l = on_or_off.lower()
        if on_or_off_l not in {"on", "off"}:
            await ctx.send(f"This option is currently set to {'on' if current else 'off'}")
            return
        new_value = on_or_off_l == "on"
        await self.config.guild(ctx.guild).autoroles_enabled.set(new_value)
        await ctx.send("Donator export updates are now {}.".format("enabled" if new_value else "disabled"))

    @autoroles_config.command(name="map")
    async def autoroles_map_role(self, ctx: commands.Context, role: discord.Role, *, toml_path: str):
        """Map a role to a TOML path (e.g., donator.tier_1)."""
        if not ctx.guild or ctx.guild.get_role(role.id) is None:
            await ctx.send_help()
            return
        mappings = await self.config.guild(ctx.guild).autoroles_role_paths()
        mappings[str(role.id)] = toml_path.strip()
        await self.config.guild(ctx.guild).autoroles_role_paths.set(mappings)
        await ctx.send(f"Mapped role {role.name} to `{toml_path.strip()}`")
        # Rebuild after mapping change
        await self.rebuild_autoroles_file(ctx.guild)

    @autoroles_config.command(name="unmap")
    async def autoroles_unmap_role(self, ctx: commands.Context, role: discord.Role):
        """Remove a role→path mapping."""
        mappings = await self.config.guild(ctx.guild).autoroles_role_paths()
        if str(role.id) in mappings:
            del mappings[str(role.id)]
            await self.config.guild(ctx.guild).autoroles_role_paths.set(mappings)
            await ctx.send(f"Unmapped role {role.name} from autoroles configuration")
            if ctx.guild:
                await self.rebuild_autoroles_file(ctx.guild)
        else:
            await ctx.send("This role is not mapped.")

    @autoroles_config.command(name="list")
    async def autoroles_list_mappings(self, ctx: commands.Context):
        """List current role→path mappings."""
        mappings = await self.config.guild(ctx.guild).autoroles_role_paths()
        if not mappings:
            await ctx.send("No autoroles mappings configured.")
            return
        lines = []
        for role_id_str, path in mappings.items():
            role = ctx.guild.get_role(int(role_id_str)) if ctx.guild else None
            role_name = role.name if role else f"Unknown({role_id_str})"
            lines.append(f"- {role_name}: {path}")
        await ctx.send(chat_formatting.box("\n".join(lines), "yaml"))

    # =========================
    # agevet commands
    # =========================

    @ckeytools.group(name="agevet")
    @checks.admin_or_permissions(manage_guild=True)
    async def agevet(self, ctx: commands.Context):
        """Age verification system for Discord users linked to SS13 ckeys."""
        pass

    @agevet.command(name="vet")
    @checks.admin_or_permissions(manage_guild=True)
    async def agevet_vet(self, ctx: commands.Context, user: discord.Member, *, date_of_birth: str):
        """
        Age verify a Discord user with their date of birth.

        This command will:
        1. Get the user's linked ckey from the database
        2. Create or update their age verification record
        3. Assign the agevet role if configured

        Date formats supported:
        - YYYY-MM-DD (2000-01-01)
        - MM/DD/YYYY (01/01/2000)
        - DD/MM/YYYY (01/01/2000)
        - January 1, 2000
        - And many other common formats

        Examples:
        `[p]ckeytools agevet vet @user 2000-01-01`
        `[p]ckeytools agevet vet @user 01/01/2000`
        `[p]ckeytools agevet vet @user "January 1, 2000"`
        """
        if not ctx.guild:
            await ctx.send("❌ This command must be used in a guild.")
            return

        # Check if agevet system is enabled
        agevet_enabled = await self.config.guild(ctx.guild).agevet_enabled()
        if not agevet_enabled:
            await ctx.send("❌ Age verification system is not enabled. Use `[p]ckeytools agevet config enable` to enable it.")
            return

        # Check if database is connected
        if not self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("❌ Database is not connected. Please configure the database connection first.")
            return

        # Check if API is configured
        try:
            await self._get_agevet_headers(ctx.guild)
            await self._get_agevet_url(ctx.guild)
        except ValueError as e:
            await ctx.send(f"❌ Age verification API not configured: {e}")
            return

        async with ctx.typing():
            try:
                # Get user's ckey from database
                link = await self.db_manager.get_valid_link_by_discord_id(ctx.guild.id, user.id)
                if not link:
                    await ctx.send(f"❌ User {user.mention} is not verified with a ckey. They must verify their Discord account first.")
                    return

                ckey = link.ckey
                self.log.info(f"Age vetting user {user} ({user.id}) with ckey {ckey}")

                # Parse date of birth
                try:
                    dob = self._parse_date_of_birth(date_of_birth)
                except ValueError as e:
                    await ctx.send(f"❌ Invalid date format: {e}")
                    return

                # Check if user is already age vetted
                existing_record = await self.get_agevet_record(ctx.guild, ckey)

                if existing_record:
                    # Update existing record
                    await self.update_agevet_record(ctx.guild, ckey, dob)
                    action = "updated"
                    self.log.info(f"Updated age vetting record for {user} ({ckey}) with DOB {dob}")
                else:
                    # Create new record
                    await self.create_agevet_record(ctx.guild, ckey, dob)
                    action = "created"
                    self.log.info(f"Created age vetting record for {user} ({ckey}) with DOB {dob}")

                # Assign agevet role
                role_assigned = await self._assign_agevet_role(ctx.guild, user)

                # Send success message
                embed = discord.Embed(
                    title="Age Verification Successful",
                    color=discord.Color.green(),
                    timestamp=discord.utils.utcnow()
                )
                embed.add_field(name="User", value=f"{user.mention} ({user.display_name})", inline=True)
                embed.add_field(name="Ckey", value=f"`{ckey}`", inline=True)
                embed.add_field(name="Date of Birth", value=dob.strftime("%B %d, %Y"), inline=True)
                embed.add_field(name="Record", value=f"✅ {action.title()}", inline=True)
                embed.add_field(name="Role", value="✅ Assigned" if role_assigned else "❌ Not configured", inline=True)

                embed.set_thumbnail(url=user.display_avatar.url)
                embed.set_footer(text=f"Age verification {action} by {ctx.author.display_name}")

                await ctx.send(embed=embed)
                await ctx.tick()

            except ValueError as e:
                await ctx.send(f"❌ {e}")
            except Exception as e:
                self.log.error(f"Error during age vetting of {user}: {e}")
                await ctx.send(f"❌ An error occurred during age verification: {e}")

    @agevet.group(name="config")
    @checks.admin_or_permissions(administrator=True)
    async def agevet_config(self, ctx: commands.Context):
        """Configure the age verification system."""
        pass

    @agevet_config.command(name="enable")
    async def agevet_config_enable(self, ctx: commands.Context, enabled: Optional[bool] = None):
        """Enable or disable the age verification system."""
        if enabled is None:
            current = await self.config.guild(ctx.guild).agevet_enabled()
            await ctx.send(f"Age verification system is currently **{'enabled' if current else 'disabled'}**.")
            return

        await self.config.guild(ctx.guild).agevet_enabled.set(enabled)
        if enabled:
            await ctx.send("✅ Age verification system has been enabled.")
        else:
            await ctx.send("✅ Age verification system has been disabled.")
        await ctx.tick()

    @agevet_config.command(name="apiurl")
    async def agevet_config_api_url(self, ctx: commands.Context, *, api_url: str):
        """Set the BackgroundCheck API URL."""
        # Basic validation
        if not api_url.startswith(('http://', 'https://')):
            await ctx.send("❌ API URL must start with http:// or https://")
            return

        await self.config.guild(ctx.guild).agevet_api_url.set(api_url.rstrip('/'))
        await ctx.send(f"✅ AgeVet API URL set to `{api_url.rstrip('/')}`")
        await ctx.tick()

    @agevet_config.command(name="apikey")
    async def agevet_config_api_key(self, ctx: commands.Context, *, api_key: str):
        """Set the BackgroundCheck API key."""
        if not api_key.strip():
            await ctx.send("❌ API key cannot be empty")
            return

        await self.config.guild(ctx.guild).agevet_api_key.set(api_key.strip())
        await ctx.send("✅ AgeVet API key set.")
        await ctx.tick()

    @agevet_config.command(name="role")
    async def agevet_config_role(self, ctx: commands.Context, role: Optional[discord.Role] = None):
        """Set the role to assign to age-verified users."""
        if not ctx.guild:
            await ctx.send("❌ This command must be used in a guild.")
            return

        if role is None:
            current_role_id = await self.config.guild(ctx.guild).agevet_role()
            if current_role_id:
                current_role = ctx.guild.get_role(current_role_id)
                if current_role:
                    await ctx.send(f"Current agevet role: {current_role.mention}")
                else:
                    await ctx.send("❌ Configured agevet role not found. It may have been deleted.")
            else:
                await ctx.send("❌ No agevet role configured.")
            return

        await self.config.guild(ctx.guild).agevet_role.set(role.id)
        await ctx.send(f"✅ AgeVet role set to {role.mention}")
        await ctx.tick()

    @agevet_config.command(name="status")
    async def agevet_config_status(self, ctx: commands.Context):
        """Show the current age verification configuration status."""
        if not ctx.guild:
            await ctx.send("❌ This command must be used in a guild.")
            return

        conf = await self.config.guild(ctx.guild).all()

        # Check configuration
        enabled = conf["agevet_enabled"]
        api_url = conf["agevet_api_url"]
        api_key = conf["agevet_api_key"]
        role_id = conf["agevet_role"]

        # Check API connectivity
        api_configured = bool(api_url and api_key)
        api_connected = False
        if api_configured:
            try:
                await self._get_agevet_headers(ctx.guild)
                await self._get_agevet_url(ctx.guild)
                api_connected = True
            except Exception:
                pass

        # Get role info
        role = ctx.guild.get_role(role_id) if role_id else None

        embed = discord.Embed(
            title="Age Verification Configuration Status",
            color=await ctx.embed_color(),
            timestamp=discord.utils.utcnow()
        )

        embed.add_field(
            name="🔧 System Status",
            value="✅ Enabled" if enabled else "❌ Disabled",
            inline=True
        )

        embed.add_field(
            name="🌐 API Configuration",
            value="✅ Configured" if api_configured else "❌ Not configured",
            inline=True
        )

        embed.add_field(
            name="🔗 API Connection",
            value="✅ Connected" if api_connected else "❌ Failed",
            inline=True
        )

        embed.add_field(
            name="🎭 AgeVet Role",
            value=role.mention if role else "❌ Not set",
            inline=True
        )

        embed.add_field(
            name="🗄️ Database",
            value="✅ Connected" if self.db_manager.is_connected(ctx.guild.id) else "❌ Not connected",
            inline=True
        )

        if api_url:
            embed.add_field(
                name="🔗 API URL",
                value=f"`{api_url}`",
                inline=False
            )

        await ctx.send(embed=embed)

    @agevet.command(name="check")
    @checks.admin_or_permissions(manage_guild=True)
    async def agevet_check(self, ctx: commands.Context, user: discord.Member):
        """Check the age verification status of a user."""
        if not ctx.guild:
            await ctx.send("❌ This command must be used in a guild.")
            return

        # Check if agevet system is enabled
        agevet_enabled = await self.config.guild(ctx.guild).agevet_enabled()
        if not agevet_enabled:
            await ctx.send("❌ Age verification system is not enabled.")
            return

        # Check if database is connected
        if not self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("❌ Database is not connected.")
            return

        async with ctx.typing():
            try:
                # Get user's ckey from database
                link = await self.db_manager.get_valid_link_by_discord_id(ctx.guild.id, user.id)
                if not link:
                    await ctx.send(f"❌ User {user.mention} is not verified with a ckey.")
                    return

                ckey = link.ckey

                # Check age vetting status
                try:
                    record = await self.get_agevet_record(ctx.guild, ckey)
                except Exception as e:
                    await ctx.send(f"❌ Error checking age verification status: {e}")
                    return

                # Check role status
                agevet_role_id = await self.config.guild(ctx.guild).agevet_role()
                has_role = False
                if agevet_role_id:
                    role = ctx.guild.get_role(agevet_role_id)
                    if role and role in user.roles:
                        has_role = True

                embed = discord.Embed(
                    title=f"Age Verification Status: {user.display_name}",
                    color=discord.Color.green() if record else discord.Color.red(),
                    timestamp=discord.utils.utcnow()
                )

                embed.add_field(name="User", value=f"{user.mention} ({user.display_name})", inline=True)
                embed.add_field(name="Ckey", value=f"`{ckey}`", inline=True)

                if record:
                    dob = datetime.datetime.fromisoformat(record['date_of_birth']).date()
                    created_at = datetime.datetime.fromisoformat(record['created_at'].replace('Z', '+00:00'))

                    embed.add_field(name="Date of Birth", value=dob.strftime("%B %d, %Y"), inline=True)
                    embed.add_field(name="Age", value=f"{self._calculate_age(dob)} years old", inline=True)
                    embed.add_field(name="Vetted Since", value=f"<t:{int(created_at.timestamp())}:R>", inline=True)
                else:
                    embed.add_field(name="Status", value="❌ Not age verified", inline=True)

                embed.add_field(
                    name="AgeVet Role",
                    value="✅ Has role" if has_role else "❌ No role" if agevet_role_id else "❌ Not configured",
                    inline=True
                )

                embed.set_thumbnail(url=user.display_avatar.url)

                await ctx.send(embed=embed)

            except Exception as e:
                self.log.error(f"Error checking age verification status for {user}: {e}")
                await ctx.send(f"❌ An error occurred while checking age verification status: {e}")

    @agevet.command(name="unvet")
    @checks.admin_or_permissions(manage_guild=True)
    async def agevet_unvet(self, ctx: commands.Context, user: discord.Member):
        """
        Remove age verification from a Discord user.

        This command will:
        1. Get the user's linked ckey from the database
        2. Delete their age verification record from the BackgroundCheck API
        3. Remove the agevet role if they have it

        Examples:
        `[p]ckeytools agevet unvet @user`
        """
        if not ctx.guild:
            await ctx.send("❌ This command must be used in a guild.")
            return

        # Check if agevet system is enabled
        agevet_enabled = await self.config.guild(ctx.guild).agevet_enabled()
        if not agevet_enabled:
            await ctx.send("❌ Age verification system is not enabled. Use `[p]ckeytools agevet config enable` to enable it.")
            return

        # Check if database is connected
        if not self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("❌ Database is not connected. Please configure the database connection first.")
            return

        # Check if API is configured
        try:
            await self._get_agevet_headers(ctx.guild)
            await self._get_agevet_url(ctx.guild)
        except ValueError as e:
            await ctx.send(f"❌ Age verification API not configured: {e}")
            return

        async with ctx.typing():
            try:
                # Get user's ckey from database
                link = await self.db_manager.get_valid_link_by_discord_id(ctx.guild.id, user.id)
                if not link:
                    await ctx.send(f"❌ User {user.mention} is not verified with a ckey. They must verify their Discord account first.")
                    return

                ckey = link.ckey
                self.log.info(f"Removing age verification from user {user} ({user.id}) with ckey {ckey}")

                # Check if user is age vetted
                existing_record = await self.get_agevet_record(ctx.guild, ckey)

                if not existing_record:
                    await ctx.send(f"❌ User {user.mention} is not age verified.")
                    return

                # Delete the age vetting record
                await self.delete_agevet_record(ctx.guild, ckey)
                self.log.info(f"Deleted age vetting record for {user} ({ckey})")

                # Remove agevet role
                role_removed = await self._remove_agevet_role(ctx.guild, user)

                # Send success message
                embed = discord.Embed(
                    title="Age Verification Removed",
                    color=discord.Color.orange(),
                    timestamp=discord.utils.utcnow()
                )
                embed.add_field(name="User", value=f"{user.mention} ({user.display_name})", inline=True)
                embed.add_field(name="Ckey", value=f"`{ckey}`", inline=True)
                embed.add_field(name="Record", value="✅ Deleted", inline=True)
                embed.add_field(name="Role", value="✅ Removed" if role_removed else "❌ Not configured", inline=True)

                embed.set_thumbnail(url=user.display_avatar.url)
                embed.set_footer(text=f"Age verification removed by {ctx.author.display_name}")

                await ctx.send(embed=embed)
                await ctx.tick()

            except ValueError as e:
                await ctx.send(f"❌ {e}")
            except Exception as e:
                self.log.error(f"Error during age unvetting of {user}: {e}")
                await ctx.send(f"❌ An error occurred during age verification removal: {e}")

    @agevet.command(name="loadcsv")
    @checks.admin_or_permissions(administrator=True)
    async def agevet_load_csv(self, ctx: commands.Context):
        """
        Load age verification data from a CSV file attachment.

        The CSV file should have the following format:
        - First column: ckey (SS13 ckey)
        - Second column: date_of_birth (in any supported date format)

        Example CSV content:
        ```
        ckey,date_of_birth
        john_doe,2000-01-01
        jane_smith,1995-05-15
        bob_wilson,1998-12-25
        ```

        This command will:
        1. Parse the CSV file
        2. Create age verification records in the BackgroundCheck API
        3. Find Discord users linked to those ckeys
        4. Assign the agevet role to verified users

        Use: `[p]ckeytools agevet loadcsv` (attach CSV file to the message)
        """
        if not ctx.guild:
            await ctx.send("❌ This command must be used in a guild.")
            return

        # Check if agevet system is enabled
        agevet_enabled = await self.config.guild(ctx.guild).agevet_enabled()
        if not agevet_enabled:
            await ctx.send("❌ Age verification system is not enabled. Use `[p]ckeytools agevet config enable` to enable it.")
            return

        # Check if database is connected
        if not self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("❌ Database is not connected. Please configure the database connection first.")
            return

        # Check if API is configured
        try:
            await self._get_agevet_headers(ctx.guild)
            await self._get_agevet_url(ctx.guild)
        except ValueError as e:
            await ctx.send(f"❌ Age verification API not configured: {e}")
            return

        # Check for CSV file attachment
        if not ctx.message.attachments:
            await ctx.send("❌ Please attach a CSV file to this command message.")
            return

        attachment = ctx.message.attachments[0]
        if not attachment.filename.lower().endswith('.csv'):
            await ctx.send("❌ The attached file must be a CSV file.")
            return

        async with ctx.typing():
            try:
                # Download and parse CSV file
                file_bytes = await attachment.read()
                file_text = file_bytes.decode('utf-8')

                # Parse CSV with proper handling of newlines and quotes
                try:
                    csv_reader = csv.DictReader(io.StringIO(file_text))
                    records = list(csv_reader)
                except csv.Error as e:
                    # If CSV parsing fails, try with different dialect settings
                    try:
                        # Try with different quoting and escapechar settings
                        csv_reader = csv.DictReader(
                            io.StringIO(file_text),
                            quoting=csv.QUOTE_ALL,
                            escapechar='\\'
                        )
                        records = list(csv_reader)
                    except csv.Error:
                        # Try with minimal settings
                        csv_reader = csv.DictReader(
                            io.StringIO(file_text),
                            quoting=csv.QUOTE_NONE,
                            escapechar='\\'
                        )
                        records = list(csv_reader)

                # If all parsing attempts failed, raise the original error
                if 'records' not in locals():
                    raise csv.Error(f"Failed to parse CSV file: {e}")

                if not records:
                    await ctx.send("❌ CSV file is empty or has no valid records.")
                    return

                # Validate CSV format
                if 'ckey' not in records[0] or 'date_of_birth' not in records[0]:
                    await ctx.send("❌ CSV file must have 'ckey' and 'date_of_birth' columns.")
                    return

                self.log.info(f"Loading {len(records)} age verification records from CSV file")

                # Process records
                processed = 0
                created = 0
                updated = 0
                errors = 0
                discord_users_updated = 0
                error_details = []

                for i, record in enumerate(records, 1):
                    try:
                        ckey = record['ckey'].strip()
                        dob_str = record['date_of_birth'].strip()

                        if not ckey or not dob_str:
                            error_details.append(f"Row {i}: Empty ckey or date_of_birth")
                            errors += 1
                            continue

                        # Normalize ckey
                        ckey = normalise_to_ckey(ckey)

                        # Parse date of birth
                        try:
                            dob = self._parse_date_of_birth(dob_str)
                        except ValueError as e:
                            error_details.append(f"Row {i} (ckey: {ckey}): Invalid date format - {e}")
                            errors += 1
                            continue

                        # Check if record already exists
                        existing_record = await self.get_agevet_record(ctx.guild, ckey)

                        if existing_record:
                            # Update existing record
                            await self.update_agevet_record(ctx.guild, ckey, dob)
                            updated += 1
                            self.log.debug(f"Updated age vetting record for ckey {ckey}")
                        else:
                            # Create new record
                            await self.create_agevet_record(ctx.guild, ckey, dob)
                            created += 1
                            self.log.debug(f"Created age vetting record for ckey {ckey}")

                        # Find Discord users with this ckey and assign role
                        try:
                            # Get all links for this ckey
                            all_links = await self.db_manager.get_all_links_by_ckey(ctx.guild.id, ckey)

                            for link in all_links:
                                if link.valid and link.discord_id:
                                    # Find the Discord user
                                    discord_user = ctx.guild.get_member(link.discord_id)
                                    if discord_user:
                                        # Assign agevet role
                                        role_assigned = await self._assign_agevet_role(ctx.guild, discord_user)
                                        if role_assigned:
                                            discord_users_updated += 1
                                            self.log.debug(f"Assigned agevet role to {discord_user} for ckey {ckey}")
                        except Exception as e:
                            self.log.warning(f"Error processing Discord users for ckey {ckey}: {e}")

                        processed += 1

                    except Exception as e:
                        error_details.append(f"Row {i} (ckey: {record.get('ckey', 'unknown')}): {str(e)}")
                        errors += 1
                        self.log.error(f"Error processing CSV row {i}: {e}")

                # Send results
                embed = discord.Embed(
                    title="CSV Age Verification Load Complete",
                    color=discord.Color.green() if errors == 0 else discord.Color.orange(),
                    timestamp=discord.utils.utcnow()
                )

                embed.add_field(name="📊 Total Records", value=str(len(records)), inline=True)
                embed.add_field(name="✅ Processed", value=str(processed), inline=True)
                embed.add_field(name="🆕 Created", value=str(created), inline=True)
                embed.add_field(name="🔄 Updated", value=str(updated), inline=True)
                embed.add_field(name="👥 Discord Users Updated", value=str(discord_users_updated), inline=True)
                embed.add_field(name="❌ Errors", value=str(errors), inline=True)

                if error_details:
                    # Truncate error details if too long
                    error_text = "\n".join(error_details[:10])  # Show first 10 errors
                    if len(error_details) > 10:
                        error_text += f"\n... and {len(error_details) - 10} more errors"

                    if len(error_text) > 1024:
                        error_text = error_text[:1020] + "..."

                    embed.add_field(name="Error Details", value=f"```\n{error_text}\n```", inline=False)

                embed.set_footer(text=f"CSV load completed by {ctx.author.display_name}")
                await ctx.send(embed=embed)

                # Log summary
                self.log.info(f"CSV age verification load completed: {processed} processed, {created} created, {updated} updated, {discord_users_updated} Discord users updated, {errors} errors")

            except csv.Error as e:
                self.log.error(f"CSV parsing error: {e}")
                await ctx.send(f"❌ CSV parsing error: {e}\n\n**Tips for fixing CSV files:**\n"
                             "• Ensure all fields with newlines are properly quoted\n"
                             "• Use consistent quote characters (single or double quotes)\n"
                             "• Avoid unescaped quotes within quoted fields\n"
                             "• Make sure the file is saved as UTF-8 encoding")
            except Exception as e:
                self.log.error(f"Error loading CSV age verification data: {e}")
                await ctx.send(f"❌ An error occurred while loading CSV data: {e}")

    def _calculate_age(self, birth_date: datetime.date) -> int:
        """Calculate age from birth date."""
        today = datetime.date.today()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

class VerificationButtonView(discord.ui.View):
    """View for the verification button."""

    def __init__(self, cog: CkeyTools):
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

class VerificationCodeView(discord.ui.View):
    def __init__(self, cog, user, guild):
        super().__init__(timeout=None)
        self.cog = cog
        self.user = user  # Can be None for persistent view registration
        self.guild = guild  # Can be None for persistent view registration

    @discord.ui.button(label="Enter Verification Code", style=discord.ButtonStyle.primary, custom_id="verify_code_button")
    async def verify_code_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Get the actual user and guild from interaction
        user = interaction.user
        guild = interaction.guild

        # Check if this is a verification ticket by looking for open ticket in config
        open_ticket = await self.cog.config.member(user).open_ticket()
        if not open_ticket or not interaction.channel or open_ticket != interaction.channel.id:
            await interaction.response.send_message("This doesn't appear to be your verification ticket.", ephemeral=True)
            return

        await interaction.response.send_modal(VerificationCodeModal(self.cog, user, guild, interaction.channel))

class VerificationCodeModal(discord.ui.Modal, title="Enter Verification Code"):
    code: ui.TextInput = ui.TextInput(label="Verification Code", style=discord.TextStyle.short, required=True, max_length=100)

    def __init__(self, cog, user, guild, ticket_channel):
        super().__init__()
        self.cog = cog
        self.user = user
        self.guild = guild
        self.ticket_channel = ticket_channel

    async def on_submit(self, interaction: discord.Interaction):
        # Check if verification system is enabled
        verification_enabled = await self.cog.config.guild(self.guild).verification_enabled()
        if not verification_enabled:
            await interaction.response.send_message(
                "❌ Verification system is currently disabled. Please contact an administrator.",
                ephemeral=True
            )
            return

        # Try to verify the code
        verified, ckey = await self.cog.verify_code(self.guild, self.user, self.code.value)
        if verified:
            await self.cog.finish_verification(self.guild, self.user, ckey, ticket_channel=self.ticket_channel)
            await interaction.response.send_message(f"Verification successful! Welcome, `{ckey}`.", ephemeral=True)
            self.stop()
        else:
            await interaction.response.send_message(
                "❌ Could not verify your code. Please try again or ping staff for help.", ephemeral=True
            )

class DeverifyConfirmView(discord.ui.View):
    """View for confirming deverification."""

    def __init__(self, cog: CkeyTools, command_author: discord.Member, target_user: discord.Member):
        super().__init__(timeout=60.0)
        self.cog = cog
        self.command_author = command_author
        self.target_user = target_user

    @discord.ui.button(label="Yes, Deverify", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_deverify(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Only allow the command author to use the button
        if interaction.user.id != self.command_author.id:
            await interaction.response.send_message("You are not authorized to use this button.", ephemeral=True)
            return

        # Disable all buttons
        for item in self.children:
            if hasattr(item, 'disabled'):
                item.disabled = True
        await interaction.response.edit_message(view=self)

        # Perform deverification
        success, message = await self.cog.perform_deverify(interaction.guild, self.target_user, self.command_author)

        if success:
            await interaction.response.send_message(f"✅ {message}")
        else:
            await interaction.response.send_message(f"❌ {message}")

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_deverify(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Only allow the command author to use the button
        if interaction.user.id != self.command_author.id:
            await interaction.response.send_message("You are not authorized to use this button.", ephemeral=True)
            return

        # Disable all buttons
        for item in self.children:
            if hasattr(item, 'disabled'):
                item.disabled = True

        await interaction.response.edit_message(content="❌ Deverification cancelled.", view=self)

    async def on_timeout(self):
        # Disable all buttons when the view times out
        for item in self.children:
            if hasattr(item, 'disabled'):
                item.disabled = True
