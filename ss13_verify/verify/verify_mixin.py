"""
Verification functionality mixin for the SS13Verify/CkeyTools cog.
This mixin provides Discord verification related functionality (non-command methods).
"""

import logging
import json
import asyncio
import hashlib
import datetime
import re
from typing import Optional, List

import discord
from discord.ext import commands
from redbot.core import commands as red_commands, checks
from redbot.core.utils import chat_formatting

from ..helpers import normalise_to_ckey

log = logging.getLogger("red.ss13_verify.verify")


class VerifyMixin:
    """Mixin class providing Discord verification functionality."""

    # Permission helper methods
    def _get_all_discord_permissions(self) -> List[str]:
        """Get a list of all Discord permission names."""
        return [perm for perm, value in discord.Permissions().all()]

    def _parse_permission_value(self, value: str) -> Optional[bool]:
        """Parse a permission value string to boolean or None."""
        value = value.lower().strip()
        if value in ['allow', 'true', '1', 'yes', 'on']:
            return True
        elif value in ['deny', 'false', '0', 'no', 'off']:
            return False
        elif value in ['passthrough', 'none', 'default', 'null']:
            return None
        else:
            raise ValueError(f"Invalid permission value: {value}")

    def _permission_value_to_string(self, value: Optional[bool]) -> str:
        """Convert permission value to string representation."""
        if value is True:
            return 'allow'
        elif value is False:
            return 'deny'
        else:
            return 'passthrough'

    def _parse_permission_args(self, args_string: str) -> dict:
        """Parse command-line style permission arguments using regex."""
        if not args_string.strip():
            return {}

        # Regex patterns for different argument formats
        # Matches: --option value, --option "value", --option=value, --option="value", --option (no value)
        patterns = [
            r'--(\w+)=(["\'])([^"\']*)\2',  # --option="value" or --option='value'
            r'--(\w+)=(\S+)',               # --option=value
            r'--(\w+)\s+(["\'])([^"\']*)\2', # --option "value" or --option 'value'
            r'--(\w+)\s+(\S+)',             # --option value
            r'--(\w+)',                     # --option (no value, defaults to allow)
        ]

        parsed_args = {}
        processed_positions = set()

        for pattern in patterns:
            for match in re.finditer(pattern, args_string):
                start, end = match.span()
                # Skip if this position was already processed by a more specific pattern
                if any(pos in processed_positions for pos in range(start, end)):
                    continue

                # Mark positions as processed
                for pos in range(start, end):
                    processed_positions.add(pos)

                option = match.group(1)
                if len(match.groups()) >= 3 and match.group(3):  # Quoted value
                    value = match.group(3)
                elif len(match.groups()) >= 2 and match.group(2) and not match.group(2).startswith('"') and not match.group(2).startswith("'"):  # Unquoted value
                    value = match.group(2)
                else:  # No value provided, default to 'allow'
                    value = 'allow'

                try:
                    parsed_args[option] = self._parse_permission_value(value)
                except ValueError as e:
                    log.warning(f"Error parsing permission argument {option}={value}: {e}")
                    continue

        return parsed_args

    async def _build_ticket_overwrites(self, guild: discord.Guild, ticket_opener: discord.Member) -> dict:
        """Build permission overwrites for a ticket channel."""
        config = await self.config.guild(guild).all()
        overwrites = {}

        # Default permissions for @everyone
        default_perms = config.get("ticket_default_permissions", {})
        if default_perms:
            overwrites[guild.default_role] = discord.PermissionOverwrite(**default_perms)
        else:
            # Default: deny view channel for @everyone
            overwrites[guild.default_role] = discord.PermissionOverwrite(view_channel=False)

        # Staff role permissions
        staff_roles = config.get("ticket_staff_roles", [])
        staff_perms = config.get("ticket_staff_permissions", {})

        for role_id in staff_roles:
            role = guild.get_role(role_id)
            if role:
                if staff_perms:
                    overwrites[role] = discord.PermissionOverwrite(**staff_perms)
                else:
                    # Default staff permissions
                    overwrites[role] = discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                        read_message_history=True,
                        manage_messages=True
                    )

        # Ticket opener permissions
        opener_perms = config.get("ticket_opener_permissions", {})
        if opener_perms:
            overwrites[ticket_opener] = discord.PermissionOverwrite(**opener_perms)
        else:
            # Default opener permissions
            overwrites[ticket_opener] = discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True
            )

        return overwrites

    # Verification flow methods
    async def fetch_valid_discord_link(self, guild: discord.Guild, discord_id: int):
        """Fetch a valid Discord link for a user."""
        log.info(f"Checking if user {discord_id} is already verified in guild {guild.name}")

        if not self.db_manager.is_connected(guild.id):
            # Try to reconnect
            log.warning(f"Database not connected for guild {guild.name} when checking valid link for user {discord_id}")
            await self.reconnect_database(guild)

            if not self.db_manager.is_connected(guild.id):
                log.error(f"Failed to reconnect database for guild {guild.name}")
                return None

        try:
            return await self.db_manager.get_valid_link_by_discord_id(guild.id, discord_id)
        except Exception as e:
            log.error(f"Error fetching valid link for user {discord_id} in guild {guild.name}: {e}")
            return None

    async def fetch_latest_discord_link(self, guild: discord.Guild, discord_id: int):
        """Fetch the latest Discord link (valid or invalid) for a user."""
        if not self.db_manager.is_connected(guild.id):
            # Try to reconnect
            log.warning(f"Database not connected for guild {guild.name} when checking latest link for user {discord_id}")
            await self.reconnect_database(guild)

            if not self.db_manager.is_connected(guild.id):
                log.error(f"Failed to reconnect database for guild {guild.name}")
                return None

        try:
            return await self.db_manager.get_latest_link_by_discord_id(guild.id, discord_id)
        except Exception as e:
            log.error(f"Error fetching latest link for user {discord_id} in guild {guild.name}: {e}")
            return None

    async def is_user_verified(self, guild: discord.Guild, discord_id: int) -> bool:
        """Check if a user is currently verified."""
        # Check if user is in deverified list
        deverified_users = await self.config.guild(guild).deverified_users()
        if discord_id in deverified_users:
            return False

        link = await self.fetch_valid_discord_link(guild, discord_id)
        return link is not None

    async def ensure_user_roles(self, guild: discord.Guild, member: discord.Member):
        """Ensure a verified user has the correct roles."""
        verification_roles = await self.config.guild(guild).verification_roles()

        for role_id in verification_roles:
            role = guild.get_role(role_id)
            if role and role not in member.roles:
                try:
                    await member.add_roles(role, reason="User verification")
                    log.info(f"Added role {role.name} to verified user {member.display_name}")
                except discord.HTTPException as e:
                    log.error(f"Failed to add role {role.name} to user {member.display_name}: {e}")

    async def create_auto_link(self, guild: discord.Guild, discord_id: int, original_link):
        """Create an automatic verification link based on a previous link."""
        # Generate new token based on original token and current timestamp
        timestamp_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
        new_token = hashlib.sha256(f"{original_link.one_time_token}{timestamp_str}".encode()).hexdigest()

        # First invalidate all previous valid links for this ckey and discord_id
        await self.db_manager.invalidate_previous_links(guild.id, original_link.ckey, discord_id)

        # Create new valid link
        new_link = await self.db_manager.create_link(
            guild_id=guild.id,
            ckey=original_link.ckey,
            discord_id=discord_id,
            one_time_token=new_token,
            valid=True
        )

        return new_link

    async def try_auto_verification(self, guild: discord.Guild, user: discord.User,
                                  dm_channel=None, ticket_channel=None):
        """Try to automatically verify a user based on previous links."""
        # Check if auto-verification is enabled
        autoverify_enabled = await self.config.guild(guild).autoverification_enabled()
        if not autoverify_enabled:
            return False

        # Check if user is already verified
        if await self.is_user_verified(guild, user.id):
            log.info(f"User {user.display_name} verification status: True")
            # Ensure they have the correct roles
            member = guild.get_member(user.id)
            if member:
                await self.ensure_user_roles(guild, member)

                # Send success message for already verified users
                if dm_channel:
                    await self.send_verification_success_dm(guild, user, await self.fetch_valid_discord_link(guild, user.id))
                elif ticket_channel:
                    await ticket_channel.send(f"✅ **{user.display_name}**, you are already verified! Welcome back.")

            return True

        log.info(f"User {user.display_name} verification status: False")

        # Try to find a previous link for auto-verification
        latest_link = await self.fetch_latest_discord_link(guild, user.id)

        if not latest_link:
            # No previous link found
            if dm_channel:
                panel_channel_id = await self.config.guild(guild).ticket_channel()
                panel_channel = guild.get_channel(panel_channel_id) if panel_channel_id else None
                panel_link = f"https://discord.com/channels/{guild.id}/{panel_channel_id}" if panel_channel else "the verification panel"

                await dm_channel.send(
                    f"It seems you have no account linked. Please make sure to link your discord account to your ckey at {panel_link} in order to verify!"
                )
            return False

        # Send attempting message
        attempting_msg = None
        if dm_channel:
            attempting_msg = await dm_channel.send("Attempting to auto verify...")
        elif ticket_channel:
            attempting_msg = await ticket_channel.send("Attempting to auto verify...")

        try:
            # Create auto link
            new_link = await self.create_auto_link(guild, user.id, latest_link)

            # Get member and add roles
            member = guild.get_member(user.id)
            if member:
                await self.ensure_user_roles(guild, member)

            # Success!
            if dm_channel:
                # For DMs, delete the attempting message and send the success embed
                if attempting_msg:
                    await attempting_msg.delete()
                await self.send_verification_success_dm(guild, user, new_link)
            elif ticket_channel:
                await attempting_msg.edit(content=f"✅ Automatic verification completed! Welcome back, **{latest_link.ckey}**.")

            log.info(f"Auto-verification successful for user {user.display_name} with ckey {latest_link.ckey}")
            return True

        except Exception as e:
            log.error(f"Auto-verification failed for user {user.display_name}: {e}")

            # Update attempting message with failure
            failure_msg = "❌ Auto-verification failed. Please try manual verification."
            if attempting_msg:
                await attempting_msg.edit(content=failure_msg)

            return False

    async def handle_verification_request(self, guild: discord.Guild, user: discord.User,
                                        interaction: discord.Interaction):
        """Handle a verification request from the panel button."""
        # Check if verification is enabled
        verification_enabled = await self.config.guild(guild).verification_enabled()
        if not verification_enabled:
            await interaction.response.send_message(
                "❌ Verification system is currently disabled.", ephemeral=True
            )
            return

        # Check if user is already verified
        if await self.is_user_verified(guild, user.id):
            member = guild.get_member(user.id)
            if member:
                await self.ensure_user_roles(guild, member)

            await interaction.response.send_message(
                "✅ You are already verified! If you're missing roles, they should be restored now.",
                ephemeral=True
            )
            return

        # Check if user already has an open ticket
        open_ticket_id = await self.config.member(user).open_ticket()
        if open_ticket_id:
            ticket_channel = guild.get_channel(open_ticket_id)
            if ticket_channel:
                await interaction.response.send_message(
                    f"❌ You already have an open verification ticket: {ticket_channel.mention}",
                    ephemeral=True
                )
                return

        # Create verification ticket
        category_id = await self.config.guild(guild).ticket_category()
        category = guild.get_channel(category_id) if category_id else None

        if not category:
            await interaction.response.send_message(
                "❌ Verification category not configured. Please contact an administrator.",
                ephemeral=True
            )
            return

        # Build permission overwrites
        overwrites = await self._build_ticket_overwrites(guild, user)

        # Create ticket channel
        ticket_name = f"verify-{user.display_name.lower()}"
        try:
            ticket_channel = await category.create_text_channel(
                name=ticket_name,
                overwrites=overwrites,
                reason=f"Verification ticket for {user}"
            )

            # Store open ticket
            await self.config.member(user).open_ticket.set(ticket_channel.id)

            await interaction.response.send_message(
                f"✅ Verification ticket created: {ticket_channel.mention}",
                ephemeral=True
            )

            # Try auto-verification first
            auto_success = await self.try_auto_verification(guild, user, ticket_channel=ticket_channel)

            if auto_success:
                # Auto-verification succeeded, close ticket after delay
                await self.finish_verification(guild, user, ticket_channel)
            else:
                # Send ticket embed for manual verification
                ticket_embed_data = await self.config.guild(guild).ticket_embed()
                if ticket_embed_data:
                    embed = discord.Embed.from_dict(ticket_embed_data)
                    from .ui_components import VerificationCodeView
                    view = VerificationCodeView(self)
                    await ticket_channel.send(embed=embed, view=view)
                else:
                    await ticket_channel.send(
                        "❌ Ticket embed not configured. Please contact an administrator."
                    )

        except discord.HTTPException as e:
            log.error(f"Failed to create verification ticket for {user}: {e}")
            await interaction.response.send_message(
                "❌ Failed to create verification ticket. Please try again or contact an administrator.",
                ephemeral=True
            )

    async def finish_verification(self, guild: discord.Guild, user: discord.User,
                                ticket_channel: discord.TextChannel = None):
        """Finish the verification process and clean up."""
        # Clear open ticket
        await self.config.member(user).open_ticket.set(None)

        # Get the verified link for the success message
        verified_link = await self.fetch_valid_discord_link(guild, user.id)

        # Send success DM
        await self.send_verification_success_dm(guild, user, verified_link)

        # Close ticket if provided
        if ticket_channel:
            await ticket_channel.send(f"✅ **{user.display_name}**, your verification is complete! This ticket will close in a few seconds.")
            await asyncio.sleep(3)  # Give user time to read the message
            try:
                await ticket_channel.delete(reason=f"Verification completed for {user}")
            except discord.HTTPException as e:
                log.error(f"Failed to delete ticket channel {ticket_channel.id}: {e}")

    async def perform_deverify(self, guild: discord.Guild, user: discord.User):
        """Perform deverification for a user."""
        # Invalidate all valid links
        await self.db_manager.invalidate_links_by_discord_id(guild.id, user.id)

        # Add to deverified users list
        deverified_users = await self.config.guild(guild).deverified_users()
        if user.id not in deverified_users:
            deverified_users.append(user.id)
            await self.config.guild(guild).deverified_users.set(deverified_users)

        # Remove verification roles
        member = guild.get_member(user.id)
        if member:
            verification_roles = await self.config.guild(guild).verification_roles()
            for role_id in verification_roles:
                role = guild.get_role(role_id)
                if role and role in member.roles:
                    try:
                        await member.remove_roles(role, reason="User deverified")
                    except discord.HTTPException as e:
                        log.error(f"Failed to remove role {role.name} from {member}: {e}")

    async def verify_code(self, guild: discord.Guild, user: discord.User, code: str):
        """Verify a one-time code and link the user."""
        try:
            verified_link = await self.db_manager.verify_code(guild.id, code, user.id)

            if verified_link:
                # Add roles
                member = guild.get_member(user.id)
                if member:
                    await self.ensure_user_roles(guild, member)

                return verified_link
            else:
                return None

        except Exception as e:
            log.error(f"Error verifying code for user {user.id}: {e}")
            return None

    async def send_verification_success_dm(self, guild: discord.Guild, user: discord.User, link):
        """Send a comprehensive verification success message to the user's DMs."""
        if not link:
            log.warning(f"No link provided for verification success DM to user {user.id}")
            return

        try:
            # Create embed
            embed = discord.Embed(
                title="Discord and ckey linked!",
                description=f"Use `/ckeytools deverify` in **{guild.name}** if you want to link a different ckey.",
                color=discord.Color.green(),
                timestamp=datetime.datetime.now(datetime.timezone.utc)
            )

            # Set author with guild invite URL
            guild_invite = await self.get_or_create_guild_invite(guild)
            embed.set_author(
                name=guild.name,
                icon_url=guild.icon.url if guild.icon else None,
                url=guild_invite
            )

            # Add ckey field
            embed.add_field(
                name="Linked Ckey",
                value=f"**{link.ckey}**",
                inline=False
            )

            # Set thumbnail to user's avatar
            embed.set_thumbnail(url=user.display_avatar.url)

            # Set footer
            embed.set_footer(text=f"{guild.name} • Discord Verification")

            # Send DM
            try:
                await user.send(embed=embed)
                log.info(f"Sent verification success DM to user {user.display_name}")
            except discord.HTTPException as e:
                log.warning(f"Failed to send verification success DM to user {user.display_name}: {e}")

        except Exception as e:
            log.error(f"Error creating verification success DM for user {user.id}: {e}")

    async def get_or_create_guild_invite(self, guild: discord.Guild) -> str:
        """Get an existing permanent invite or create one for the guild."""
        try:
            # Look for existing permanent invites
            invites = await guild.invites()
            for invite in invites:
                if invite.max_age == 0:  # Permanent invite
                    return str(invite)

            # No permanent invite found, try to create one
            # Find a suitable channel (preferably general or first available)
            channel = None

            # Try to find a channel named 'general' or similar
            for ch in guild.text_channels:
                if ch.name.lower() in ['general', 'main', 'lobby', 'welcome']:
                    channel = ch
                    break

            # If no suitable channel found, use the first available text channel
            if not channel and guild.text_channels:
                channel = guild.text_channels[0]

            if channel:
                invite = await channel.create_invite(
                    max_age=0,  # Permanent
                    max_uses=0,  # Unlimited uses
                    reason="Automatic invite for verification system"
                )
                return str(invite)
            else:
                # No text channels available, return generic Discord link
                return f"https://discord.gg/{guild.id}"

        except discord.HTTPException as e:
            log.error(f"Failed to create invite for guild {guild.name}: {e}")
            return f"https://discord.gg/{guild.id}"
