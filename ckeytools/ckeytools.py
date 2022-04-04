#General imports
import logging
import aiomysql
from typing import Optional

#discord imports
import discord

#tgcommon imports
from tgcommon.errors import TGUnrecoverableError
from tgcommon.models import DiscordLink

#Redbot imports
from redbot.core import commands, Config, checks

log = logging.getLogger("red.oranges_tgdb")

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
            "forcestay_enabled": "off"
        }
        
        self.config.register_guild(**default_guild)
    
    #Listeners
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member) -> None:
        guild = member.guild
        if guild is None:
            return
        enabled = self.config.guild(guild).forcestay_enabled()
        prefix = self.get_tgdb_prefix(guild)

        if not (enabled == "on"):
            return
        
        query = f"UPDATE {prefix}discord_links SET valid = FALSE WHERE discord_id = %s AND valid = TRUE"
        parameters = [member.id]
        results = self.query_database(query, parameters)

    #Commands
    @commands.group()
    @checks.admin_or_permissions(kick_members=True, ban_members=True)
    async def ckeytools(self, ctx: commands.Context):
        """
        Main command for the ckeytools cog
        """
        pass
    

    @ckeytools.command()
    @checks.admin()
    async def forcestay(self, ctx: commands.Context, *, on_or_off: Optional[str]):
        """
        Turn on/off to deverify players who leave the discord server.

        Saves the context in which it was turned on to perform automatic actions.
        """
        current = self.config.guild(ctx.guild).forcestay_enabled()
        if on_or_off is None:
            return await ctx.send(f"This option is currently set to {current}")
        
        on_or_off = on_or_off.lower()
        
        if on_or_off == "on":
            await ctx.send("Players will now be required to stay in the discord server to play.")
        elif on_or_off == "off":
            await ctx.send("Players will no longer be required to stay in the discord server to play.")
        else:
            return await ctx.send(f"This option is currently set to {current}")
        self.config.guild(ctx.guild).forcestay_enabled.set(current)
    
    @ckeytools.command(name="devgone")
    async def mass_deverify_nonmembers(self, ctx: commands.Context):
        """
        Deverify all the linked ckeys not currently in the discord server.
        """
        enabled = self.config.guild(ctx.guild).forcestay_enabled()
        if not (enabled == "on"):
            return await ctx.send("The requirement to stay in the discord is currently not enabled.")
        
        tgdb = self.get_tgdb()
        prefix = self.get_tgdb_prefix(ctx.guild)

        deleted = 0

        async with ctx.typing():
            query = f"SELECT * FROM {prefix}discord_links WHERE discord_id IS NOT NULL AND valid = TRUE"
            parameters = []
            rawsults = await tgdb.query_database(ctx, query, parameters)
            results = [DiscordLink.from_db_record(raw) for raw in rawsults]
            for result in results:
                member_check = await ctx.guild.get_member(result.discord_id)
                if not member_check:
                    deleted += 1
                    await tgdb.clear_all_valid_discord_links_for_discord_id(ctx, result.discord_id)
        
        return await ctx.send(f"**{deleted}** users have been deverified.")

    #Functions to get cogs and info from the cogs
    async def get_tgdb_prefix(self, guild):
        """
        Gets the prefix for the database linked to tgdb
        """
        tgdb = self.get_tgdb()
        prefix = await tgdb.config.guild(guild).mysql_prefix()
        return prefix

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
    
    #Miscellaneous functions
    async def query_database(self, query, parameters):
        """
        Use TGDB's active pool to access the database
        """
        tgdb = self.get_tgdb()
        pool = tgdb.pool
        if not pool:
            raise TGUnrecoverableError(
                "The database was not connected. Please reconnect it using [p]tgdb reconnect"
            )
        log.debug(f"Executing query {query}, with parameters {parameters}")
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, parameters)
                rows = cur.fetchall()
                # WRITE TO STORAGE LOL
                await conn.commit()
                return rows.result()
