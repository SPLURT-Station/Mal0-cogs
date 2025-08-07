"""
Autodonator functionality mixin for the SS13Verify/CkeyTools cog.
This mixin provides automated donator role management and TOML file generation (non-command methods).
"""

import logging
import os
import tomlkit
from typing import Optional, List, Dict
from discord.ext import tasks

import discord
from redbot.core import commands as red_commands, checks
from redbot.core.utils import chat_formatting

from ..helpers import normalise_to_ckey

log = logging.getLogger("red.ss13_verify.autodonator")


class AutodonatorMixin:
    """Mixin class providing autodonator functionality."""

    def __init__(self):
        super().__init__()
        # Start the donator update task
        self.donator_update.start()

    def cog_unload(self):
        """Clean up when cog is unloaded."""
        self.donator_update.cancel()

    # Tasks
    @tasks.loop(minutes=5)
    async def donator_update(self):
        """Update donator files every 5 minutes."""
        for guild in self.bot.guilds:
            enabled = await self.config.guild(guild).autodonator_enabled()
            if enabled:
                await self.rebuild_donator_file(guild)

    @donator_update.before_loop
    async def before_donator_update(self):
        """Wait for bot to be ready before starting the update loop."""
        await self.bot.wait_until_ready()

    # Autodonator helper methods
    async def get_ckeys_from_role(self, role: discord.Role) -> List[str]:
        """Get all ckeys from members that have a specific role."""
        ckeys = []
        for member in role.members:
            link = await self.get_link_from_member(member)
            if link:
                ckeys.append(link.ckey)
        return ckeys

    async def get_link_from_member(self, member: discord.Member):
        """Given a valid discord member, return the latest valid record linked to that user."""
        try:
            return await self.db_manager.get_valid_link_by_discord_id(member.guild.id, member.id)
        except Exception as e:
            log.error(f"Error getting link for member {member.id}: {e}")
            return None

    def _parse_tier_path(self, tier_path: str) -> List[str]:
        """Parse a tier path like 'donators/tier_1' into a list of keys."""
        return tier_path.split('/')

    def _build_nested_dict(self, keys: List[str], value) -> Dict:
        """Build a nested dictionary from a list of keys."""
        if len(keys) == 1:
            return {keys[0]: value}
        else:
            return {keys[0]: self._build_nested_dict(keys[1:], value)}

    def _merge_nested_dicts(self, dict1: Dict, dict2: Dict) -> Dict:
        """Recursively merge two nested dictionaries."""
        result = dict1.copy()
        for key, value in dict2.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_nested_dicts(result[key], value)
            else:
                result[key] = value
        return result

    async def rebuild_donator_file(self, guild: discord.Guild):
        """Rebuild the donator TOML file for a guild."""
        try:
            folder = await self.config.guild(guild).config_folder()
            if folder is None:
                folder = os.path.abspath(os.getcwd())

            file_path = os.path.abspath(os.path.join(folder, "donator.toml"))

            # Get all donator tier configurations
            donator_tiers = await self.config.guild(guild).donator_tiers()

            # Build the nested structure
            result_dict = {}

            for tier_path, role_ids in donator_tiers.items():
                # Get all ckeys for this tier
                tier_ckeys = []
                for role_id in role_ids:
                    role = guild.get_role(role_id)
                    if role:
                        role_ckeys = await self.get_ckeys_from_role(role)
                        tier_ckeys.extend(role_ckeys)

                # Remove duplicates while preserving order
                tier_ckeys = list(dict.fromkeys(tier_ckeys))

                # Parse the tier path and build nested dict
                keys = self._parse_tier_path(tier_path)
                nested_dict = self._build_nested_dict(keys, tier_ckeys)

                # Merge into result
                result_dict = self._merge_nested_dicts(result_dict, nested_dict)

            # Write to file
            with open(file_path, mode="w", encoding="utf-8") as donator_file:
                donator_file.write(tomlkit.dumps(result_dict))

            log.info(f"Updated donator file for guild {guild.name} at {file_path}")

        except Exception as e:
            log.error(f"Error rebuilding donator file for guild {guild.name}: {e}")

    # Helper methods for autodonator functionality

    # Listener for role changes
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Update donator files when member roles change."""
        if before.roles != after.roles:
            autodonator_enabled = await self.config.guild(after.guild).autodonator_enabled()
            if autodonator_enabled:
                # Check if any of the changed roles are donator roles
                donator_tiers = await self.config.guild(after.guild).donator_tiers()
                all_donator_role_ids = []
                for role_ids in donator_tiers.values():
                    all_donator_role_ids.extend(role_ids)

                role_changed = False
                for role in before.roles + after.roles:
                    if role.id in all_donator_role_ids:
                        role_changed = True
                        break

                if role_changed:
                    await self.rebuild_donator_file(after.guild)

    # Force stay functionality (from original ckeytools)
    async def on_member_remove(self, member: discord.Member):
        """Handle member removal for force stay functionality."""
        guild = member.guild
        if guild is None:
            return

        # Check if invalidate on leave is enabled (this is the force stay functionality)
        invalidate_on_leave = await self.config.guild(guild).invalidate_on_leave()
        if invalidate_on_leave:
            try:
                await self.db_manager.invalidate_links_by_discord_id(guild.id, member.id)
                log.info(f"Invalidated links for user {member.id} who left guild {guild.name}")
            except Exception as e:
                log.error(f"Error invalidating links for user {member.id} who left guild {guild.name}: {e}")

        # Update donator file if autodonator is enabled
        autodonator_enabled = await self.config.guild(guild).autodonator_enabled()
        if autodonator_enabled:
            await self.rebuild_donator_file(guild)
