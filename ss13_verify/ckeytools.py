"""
CkeyTools - A comprehensive SS13 Discord verification and management system.
This is the main cog file that integrates verification and autodonator modules using proper mixins.
"""

import logging
import asyncio
import datetime

import discord
from discord.ext import commands
from redbot.core import commands as red_commands, Config
from redbot.core.bot import Red

from .helpers import normalise_to_ckey
from .database import DatabaseManager
from .models import DiscordLink
from .common import DEFAULT_GUILD_CONFIG, DEFAULT_MEMBER_CONFIG, DEFAULT_ROLE_CONFIG
from .verify import VerifyMixin, VerificationCommandsMixin, VerificationButtonView, VerificationCodeView
from .autodonator import AutodonatorMixin, AutodonatorCommandsMixin
from .core_commands import CoreCommandsMixin

log = logging.getLogger("red.ss13_verify")


class CkeyTools(CoreCommandsMixin, VerifyMixin, AutodonatorMixin, VerificationCommandsMixin, AutodonatorCommandsMixin, red_commands.Cog):
    """
    Comprehensive SS13 Discord verification and management system.
    Handles ticket-based verification, auto-verification, role assignment, database linking, and autodonator features.
    """
    __author__ = "Mal0"
    __version__ = "2.0.0"

    def __init__(self, bot: Red):
        self.bot = bot
        self.log = logging.getLogger("red.ss13_verify")
        self.config = Config.get_conf(self, identifier=908039527271104514, force_registration=True)
        self.db_manager = DatabaseManager()

        # Register configurations using the common defaults
        self.config.register_guild(**DEFAULT_GUILD_CONFIG)
        self.config.register_member(**DEFAULT_MEMBER_CONFIG)
        self.config.register_role(**DEFAULT_ROLE_CONFIG)

        # Initialize mixins
        super().__init__()

    async def cog_load(self):
        """Initialize the cog when it's loaded."""
        # First, connect to databases for all guilds with config
        for guild in self.bot.guilds:
            conf = await self.config.guild(guild).all()
            if all([conf["db_host"], conf["db_port"], conf["db_user"], conf["db_password"], conf["db_name"]]):
                await self.reconnect_database(guild)

        # Then add persistent views back to the bot so buttons work after reload
        try:
            self.bot.add_view(VerificationButtonView(self))
            self.bot.add_view(VerificationCodeView(self))  # Persistent view for handling all verify_code_button interactions
            self.log.info("Successfully registered persistent views for CkeyTools")
        except Exception as e:
            self.log.error(f"Failed to register persistent views: {e}")

    async def cog_unload(self):
        """Clean up when cog is unloaded."""
        # Close all database connections
        await self.db_manager.disconnect_all()
        # Call parent unload methods
        super().cog_unload()

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

    # Event listeners
    @red_commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Handle member join for auto-verification."""
        autoverify_on_join = await self.config.guild(member.guild).autoverify_on_join_enabled()
        if autoverify_on_join:
            try:
                dm_channel = await member.create_dm()
                await self.try_auto_verification(member.guild, member, dm_channel=dm_channel)
            except discord.HTTPException as e:
                self.log.warning(f"Failed to send auto-verification DM to {member}: {e}")

    @red_commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Handle member removal - call both verify and autodonator mixins."""
        # Call autodonator mixin method first (handles both invalidation and donator file updates)
        await super().on_member_remove(member)

    @red_commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Handle member updates for autodonator functionality."""
        await super().on_member_update(before, after)
