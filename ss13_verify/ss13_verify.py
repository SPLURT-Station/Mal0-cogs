import logging
import json
import discord
from discord.ext import commands
from redbot.core import commands, Config, checks
from redbot.core.bot import Red
from redbot.core.utils import chat_formatting
from redbot.core.utils.views import SimpleMenu
import hashlib
import datetime
from discord import ui
from .helpers import normalise_to_ckey
from .database import DatabaseManager
from .models import DiscordLink

class SS13Verify(commands.Cog):
    """
    SS13 Discord <-> ckey verification and linking system.
    Handles ticket-based verification, role assignment, and database linking.
    """
    __author__ = "Mosley"
    __version__ = "0.1.0"

    def __init__(self, bot: Red):
        self.bot = bot
        self.log = logging.getLogger("red.ss13_verify")
        self.config = Config.get_conf(self, identifier=908039527271104514, force_registration=True)
        self.db_manager = DatabaseManager()
        # Per-guild config
        default_guild = {
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
        }
        self.config.register_guild(**default_guild)
        # Per-user config (for future use, e.g. to track open tickets)
        default_member = {
            "open_ticket": None,  # Channel ID of open ticket, if any
        }
        self.config.register_member(**default_member)

    async def cog_load(self):
        # On cog load, try to connect to DB for all guilds with config
        for guild in self.bot.guilds:
            conf = await self.config.guild(guild).all()
            if all([conf["db_host"], conf["db_port"], conf["db_user"], conf["db_password"], conf["db_name"]]):
                await self.reconnect_database(guild)

    async def cog_unload(self):
        # Close all database connections when cog is unloaded
        await self.db_manager.disconnect_all()

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

    @commands.group()
    @checks.admin_or_permissions(manage_guild=True)
    async def ss13verify(self, ctx):
        """SS13 Verification system configuration."""
        pass

    @ss13verify.group()
    async def settings(self, ctx):
        """Configure SS13Verify behavior settings."""
        pass

    @settings.group()
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

        Example: `[p]ss13verify settings database host localhost`
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

        Example: `[p]ss13verify settings database port 3306`
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

        Example: `[p]ss13verify settings database user ss13_bot`
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

        Example: `[p]ss13verify settings database password your_secure_password`
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

        Example: `[p]ss13verify settings database name ss13_database`
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

        Example: `[p]ss13verify settings database prefix ss13_`
        Example (no prefix): `[p]ss13verify settings database prefix ""`
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

        Example: `[p]ss13verify settings database reconnect`
        """
        await self.reconnect_database(ctx.guild)
        if self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("✅ Database reconnected successfully.")
            await ctx.tick()
        else:
            await ctx.send("❌ Failed to reconnect to the database. Check your settings and try again.")

    @settings.group()
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

    @settings.group()
    async def panel(self, ctx):
        """Configure the verification panel."""
        pass

    @settings.group()
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
        `[p]ss13verify settings permissions defaultset --view_channel deny --send_messages allow`
        `[p]ss13verify settings permissions defaultset -view_channel=deny -send_messages=allow`
        `[p]ss13verify settings permissions defaultset --embed_links --attach_files deny`
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
        `[p]ss13verify settings permissions defaultremove view_channel`
        `[p]ss13verify settings permissions defaultremove view_channel send_messages attach_files`
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

        Example: `[p]ss13verify settings permissions staffadd @Moderator`
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

        Example: `[p]ss13verify settings permissions staffremove @Moderator`
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
        `[p]ss13verify settings permissions staffset --manage_messages allow --kick_members deny`
        `[p]ss13verify settings permissions staffset -manage_messages=allow -kick_members=deny`
        `[p]ss13verify settings permissions staffset --view_audit_log --manage_channels`
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
        `[p]ss13verify settings permissions openerupdate --attach_files allow --embed_links deny`
        `[p]ss13verify settings permissions openerupdate -attach_files=allow -embed_links=deny`
        `[p]ss13verify settings permissions openerupdate --add_reactions --use_external_emojis`
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

        Example: `[p]ss13verify settings permissions testparse --view_channel deny --send_messages=allow -embed_links`
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
            title="SS13Verify Permission System Help",
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
            await ctx.send("❌ You must set up the panel embed first using `[p]ss13verify panel setembed`")
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
        await ctx.tick()


    @settings.command(name="invalidateonleave")
    async def toggle_invalidate_on_leave(self, ctx, enabled: bool = None):
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

    @settings.command(name="verification")
    async def toggle_verification(self, ctx, enabled: bool = None):
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

    @settings.command(name="autoverification")
    async def toggle_autoverification(self, ctx, enabled: bool = None):
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

    @settings.command(name="autoverifyonjoin")
    async def toggle_autoverify_on_join(self, ctx, enabled: bool = None):
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

    @settings.command(name="invalidategone")
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

    @ss13verify.command()
    async def status(self, ctx):
        """Show the current SS13Verify configuration status."""
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
            title="SS13Verify Configuration Status",
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

    @ss13verify.command()
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
                embed.color = discord.Color.green()
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
                embed.color = discord.Color.red()
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

    @ss13verify.command()
    async def ckeys(self, ctx, user: discord.Member):
        """List all past ckeys this Discord user has verified with."""
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
                self.log.error(f"Error getting ckeys for user {user}: {e}")
                await message.edit(content="❌ Error retrieving ckey history.")

    @ss13verify.command()
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

    @ss13verify.command()
    async def deverify(self, ctx: commands.Context, user: discord.Member = None):
        """
        Deverify a user, removing their verification and preventing auto-verification.

        Users can deverify themselves, or admins can deverify others.
        The user will be kicked from the server and can rejoin to verify with a new ckey.
        """
        # Determine target user
        target_user = user if user else ctx.author

        # Permission check: users can only deverify themselves, admins can deverify anyone
        if target_user != ctx.author:
            if not (ctx.author.guild_permissions.kick_members or
                   ctx.author.guild_permissions.administrator or
                   any(role.permissions.kick_members or role.permissions.administrator for role in ctx.author.roles)):
                await ctx.send("❌ You don't have permission to deverify other users.")
                return

        # Check if database is connected
        if not self.db_manager.is_connected(ctx.guild.id):
            await ctx.send("❌ Database is not connected.")
            return

        # Send confirmation view
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

        now = datetime.datetime.utcnow()
        new_token = self.generate_auto_token(original_token, now)

        try:
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
                    try:
                        dm_channel = user.dm_channel or await user.create_dm()
                        await dm_channel.send(f"You are already verified as `{ckey}`. Your roles have been updated if needed.")
                    except Exception as e:
                        self.log.error(f"Failed to send DM to already verified user {user}: {e}")

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
                    if user_is_deverified:
                        failure_msg = "You have been manually deverified. Auto-verification is not available."
                    elif not autoverification_enabled:
                        failure_msg = "Auto-verification is currently disabled."
                    else:
                        failure_msg = "No previous link found for auto verification."
                    await msg.edit(content=failure_msg)
                    return False, None
        elif dm:
            try:
                dm_channel = user.dm_channel or await user.create_dm()
                typing_ctx = dm_channel.typing() if hasattr(dm_channel, 'typing') else None
                if typing_ctx:
                    await typing_ctx.__aenter__()
                await dm_channel.send("Attempting to auto verify...")
                if link:
                    ckey = link["ckey"]
                    original_token = link["one_time_token"]
                    new_token = await self.create_auto_link(guild, ckey, user.id, original_token)
                    # Don't send completion message here - let finish_verification handle it
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
                    await dm_channel.send(msg)
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
        # Send confirmation
        msg = f"Verification completed! Welcome, `{ckey}`."
        if ticket_channel:
            await ticket_channel.send(msg)
            try:
                await ticket_channel.delete(reason="Verification completed")
            except Exception:
                pass
            await self.config.member(user).open_ticket.clear()
        elif dm_channel:
            await dm_channel.send(msg)

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

                # Create permanent invite
                invite = None
                try:
                    # Try to get ticket channel first, then any text channel
                    ticket_channel_id = await self.config.guild(guild).ticket_channel()
                    invite_channel = guild.get_channel(ticket_channel_id) if ticket_channel_id else None
                    if not invite_channel:
                        invite_channel = next((ch for ch in guild.text_channels if ch.permissions_for(guild.me).create_instant_invite), None)

                    if invite_channel:
                        invite = await invite_channel.create_invite(
                            max_age=0,  # Never expires
                            max_uses=0,  # Unlimited uses
                            reason=f"Deverification invite for {target_user}"
                        )
                except Exception as e:
                    self.log.warning(f"Failed to create invite for {target_user}: {e}")

                dm_msg = f"You have been deverified from **{guild.name}**.\n\n"
                dm_msg += "This means your current verification has been removed and you can now verify with a different ckey.\n\n"
                dm_msg += f"You can rejoin the server to verify with a new account"
                if invite:
                    dm_msg += f": {invite.url}"
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
            ticket_channel = await guild.create_text_channel(
                name=channel_name,
                category=category,
                overwrites=overwrites,
                reason=f"Verification ticket for {user}"
            )
            await self.config.member(user).open_ticket.set(ticket_channel.id)
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
                try:
                    await member.add_roles(*roles, reason="SS13 auto-verification successful")
                except Exception as role_e:
                    self.log.error(f"Failed to assign roles to {member} during auto-verification: {role_e}")

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

class VerificationCodeView(discord.ui.View):
    def __init__(self, cog, user, guild):
        super().__init__(timeout=None)
        self.cog = cog
        self.user = user
        self.guild = guild

    @discord.ui.button(label="Enter Verification Code", style=discord.ButtonStyle.primary, custom_id="verify_code_button")
    async def verify_code_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Only allow the ticket owner to use the button
        if interaction.user.id != self.user.id:
            await interaction.response.send_message("You are not the ticket owner.", ephemeral=True)
            return
        await interaction.response.send_modal(VerificationCodeModal(self.cog, self.user, self.guild, interaction.channel))

class VerificationCodeModal(discord.ui.Modal, title="Enter Verification Code"):
    code = ui.TextInput(label="Verification Code", style=discord.TextStyle.short, required=True, max_length=100)

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
        else:
            await interaction.response.send_message(
                "❌ Could not verify your code. Please try again or ping staff for help.", ephemeral=True
            )

class DeverifyConfirmView(discord.ui.View):
    """View for confirming deverification."""

    def __init__(self, cog: SS13Verify, command_author: discord.Member, target_user: discord.Member):
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
            item.disabled = True

        await interaction.response.edit_message(content="❌ Deverification cancelled.", view=self)

    async def on_timeout(self):
        # Disable all buttons when the view times out
        for item in self.children:
            item.disabled = True
