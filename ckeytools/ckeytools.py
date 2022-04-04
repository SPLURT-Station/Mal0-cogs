#General imports
from typing import Optional

#discord imports
import discord

#tgcommon imports
from tgcommon.errors import TGUnrecoverableError
from tgcommon.models import DiscordLink

#Redbot imports
from redbot.core import commands, Config, checks

BaseCog = getattr(commands, "Cog", object)

class CkeyTools(BaseCog):
    """
    Extension cog for TGDB/TGVerify
    """
    
    __author__ = "Mosley"
    __version__ = "1.0.0"
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=908039527271104513, force_registration=True)

        default_guild = {
            "forcestay_enabled": False,
            "saved_ctx": None
        }
        
        self.config.register_guild(**default_guild)
    
    #Listeners
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member) -> None:
        guild = member.guild
        if guild is None:
            return
        ctx = self.config.guild(guild).saved_ctx()
        enabled = self.config.guild(guild).forcestay_enabled()
        tgdb = self.get_tgdb()

        if (not enabled) or (not tgdb):
            return
        
        await tgdb.clear_all_valid_discord_links_for_discord_id(ctx, member.id)

    #Commands
    @commands.group()
    @checks.admin_or_permissions(kick_members=True, ban_members=True)
    async def ckeytools(self, ctx: commands.Context):
        """
        Main command for the ckeytools cog
        """
        pass
    

    @ckeytools.command()
    @checks.is_admin_or_superior()
    async def forcestay(self, ctx: commands.Context, True_or_False: Optional[bool]):
        """
        Turn on/off to deverify players who leave the discord server.

        Saves the context in which it was turned on to perform automatic actions.
        """
        current = self.config.guild(ctx.guild).forcestay_enabled()
        if not True_or_False:
            current = not current
        else:
            current = True_or_False

        if current:
            await ctx.send("Players will now be required to stay in the discord server to play.")
            await self.config.guild(ctx.guild).saved_ctx.set(ctx)
        else:
            await ctx.send("Players will no longer be required to stay in the discord server to play.")
            await self.config.guild(ctx.guild).saved_ctx.set(None)
        self.config.guild(ctx.guild).forcestay_enabled.set(current)
    
    @ckeytools.command(name="devgone")
    async def mass_deverify_nonmembers(self, ctx: commands.Context):
        enabled = self.config.guild(ctx.guild).forcestay_enabled()
        if not enabled:
            return await ctx.send("The requirement to stay in the discord is currently not enabled.")
        
        tgdb = self.get_tgdb()
        prefix = self.get_tgdb_prefix(ctx.guild)

        async with ctx.typing():
            query = f"SELECT * FROM {prefix}discord_links WHERE discord_id IS NOT NULL AND valid = TRUE"
            parameters = []
            rawsults = await tgdb.query_database(ctx, query, parameters)
            results = [DiscordLink.from_db_record(raw) for raw in rawsults]
            for result in results:
                member_check = await ctx.guild.get_member(result.discord_id)
                if not member_check:
                    await tgdb.clear_all_valid_discord_links_for_discord_id(ctx, result.discord_id)
        

    #Functions to get cogs and info from the cogs
    async def get_tgdb_prefix(self, guild):
        """
        Gets the prefix for the database linked to tgdb
        """
        tgdb = self.get_tgdb()
        return await tgdb.config.guild(guild).mysql_prefix()

    def get_tgdb(self):
        """
        Gets the TGDB cog if it is installed
        """
        tgdb = self.bot.get_cog("TGDB")
        if not tgdb:
            raise TGUnrecoverableError(
                "TGDB must exist and be configured for ckeytools to work"
            )
        return tgdb
    
    def get_tgverify(self):
        """
        Gets the TGverify cog if it is installed
        """
        tgver = self.bot.get_cog("TGverify")
        if not tgver:
            raise TGUnrecoverableError(
                "TGVerify must exist and be configured for ckeytools to work"
            )
        return tgver
