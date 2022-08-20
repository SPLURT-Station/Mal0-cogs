#General imports
import logging, aiomysql, os, tomlkit, datetime
from unicodedata import name
from array import array
from tokenize import String
from typing import Optional

#discord imports
import discord

#tgcommon imports
from tgcommon.errors import TGUnrecoverableError
from tgcommon.models import DiscordLink

#Redbot imports
from redbot.core import commands, Config, checks
from redbot.core.utils import chat_formatting

log = logging.getLogger("red.oranges_tgdb")

BaseCog = getattr(commands, "Cog", object)

class CkeyTools(BaseCog):
    """
    Extension cog for TGDB/TGVerify
    """
    
    __author__ = "Mosley"
    __version__ = "2.0.0"
    
    def __init__(self, bot):
        self.bot = bot
        self.config: Config = Config.get_conf(self, identifier=908039527271104513, force_registration=True)

        default_guild = {
            "forcestay_enabled": "off",
            "autodonator_enabled": "off",
            "config_folder": None,
            "donator_roles": []
        }
        
        default_role = {
            "donator_tier": "none"
        }
        
        self.config.register_guild(**default_guild)
        self.config.register_role(**default_role)
    
    #Listeners
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member) -> None:
        guild = member.guild
        if guild is None:
            return
        enabled = await self.config.guild(guild).forcestay_enabled()
        prefix = await self.get_tgdb_prefix(guild)

        if not (enabled == "on"):
            return
        
        query = f"UPDATE {prefix}discord_links SET valid = FALSE WHERE discord_id = %s AND valid = TRUE"
        parameters = [member.id]
        results = await self.query_database(query, parameters)
    
    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member) -> None:
        enabled = await self.config.guild(after.guild).autodonator_enabled()
        if not (enabled == "on"):
            return
        await self.rebuild_donator_file(after.guild)

    #ckeytools Commands
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
        current = await self.config.guild(ctx.guild).forcestay_enabled()
        if on_or_off is None:
            return await ctx.send(f"This option is currently set to {current}")
        
        on_or_off = on_or_off.lower()
        
        if on_or_off == "on":
            await ctx.send("Players will now be required to stay in the discord server to play.")
        elif on_or_off == "off":
            await ctx.send("Players will no longer be required to stay in the discord server to play.")
        else:
            return await ctx.send(f"This option is currently set to {current}")
        await self.config.guild(ctx.guild).forcestay_enabled.set(on_or_off)
    
    @ckeytools.command(name="devgone")
    async def mass_deverify_nonmembers(self, ctx: commands.Context):
        """
        Deverify all the linked ckeys not currently in the discord server.
        """
        enabled = await self.config.guild(ctx.guild).forcestay_enabled()
        if not (enabled == "on"):
            return await ctx.send("The requirement to stay in the discord is currently not enabled.")
        
        tgdb = self.get_tgdb()
        prefix = await self.get_tgdb_prefix(ctx.guild)

        deleted = 0

        async with ctx.typing():
            query = f"SELECT * FROM {prefix}discord_links WHERE discord_id IS NOT NULL AND valid = TRUE"
            parameters = []
            rawsults = await tgdb.query_database(ctx, query, parameters)
            results = [DiscordLink.from_db_record(raw) for raw in rawsults]
            for result in results:
                member_check = ctx.guild.get_member(result.discord_id)
                if not member_check:
                    deleted += 1
                    await tgdb.clear_all_valid_discord_links_for_discord_id(ctx, result.discord_id)
        
        return await ctx.send(f"**{deleted}** users have been deverified.")

    #autodonator Commands
    @commands.group()
    @checks.is_owner()
    async def autodonator(self, ctx: commands.Context):
        """
        Commands for the automatic management of donator roles and perks in-game
        
        Creates and edits a TOML file in the server static files. Made specifically for SPLURT's donator system. 
        Needs configuration on the game itself to properly work.
        """
        pass

    @autodonator.command(name="update")
    async def update_donators(self, ctx: commands.Context):
        """
        Manually create a new donator tier toml file
        """
        await self.rebuild_donator_file(ctx.guild)
        await ctx.tick()
    
    @autodonator.group()
    async def config(self, ctx: commands.Context):
        """
        Configuration commands for autodonator
        """
        pass
    
    @config.command(name="folder")
    async def set_folder(self, ctx: commands.Context, *, new_repo: str):
        """
        Set the folder of the instance to set donator info in
        
        Please select the __FULL PATH__ of the __FOLDER__ where you want to send the TOML file to
        """
        new_repo = os.path.abspath(new_repo)
        if not (os.path.exists(new_repo) and os.path.isdir(new_repo)):
            return await ctx.send("This path is not a valid folder!")
        
        await self.config.guild(ctx.guild).config_folder.set(new_repo)
        await ctx.tick()
    
    @config.command()
    async def toggle(self, ctx: commands.Context, *, on_or_off: Optional[str]):
        """
        Toggle automatic donator updates (will update whenever user roles are changed)
        """
        current = await self.config.guild(ctx.guild).autodonator_enabled()
        if on_or_off is None:
            return await ctx.send(f"This option is currently set to {current}")
        
        on_or_off = on_or_off.lower()
        
        if on_or_off == "on":
            await ctx.send("Donators will be updated whenever users are updated.")
        elif on_or_off == "off":
            await ctx.send("Donators will no longer be automatically updated.")
        else:
            return await ctx.send(f"This option is currently set to {current}")
        await self.config.guild(ctx.guild).autodonator_enabled.set(on_or_off)
    
    @config.command()
    async def tier(self, ctx: commands.Context, role: discord.Role, *, tier: Optional[str]):
        """
        Edit which roles add what tiers of benefits
        
        Currently available tiers:
            - none
            - first
            - second
            - third
        """
        current_roles: array = await self.config.guild(ctx.guild).donator_roles()
        
        #Safety checks
        if(ctx.guild.get_role(role.id) is None):
            return await ctx.send_help()
        
        if(tier is None):
            return await ctx.send(f"{role.name} currently awards the {await self.config.role(role).donator_tier()} donator tier")
        elif(not (tier.lower() in ["first", "second", "third", "none"])):
            return await ctx.send_help()
        elif(tier.lower() == "none"):
            current_roles.remove(role.id)
        elif(not (role.id in current_roles)):
            current_roles.append(role.id)
        
        await self.config.guild(ctx.guild).donator_roles.set(current_roles)
        await self.config.role(role).donator_tier.set(tier.lower())
        await ctx.send(f"The role {role.name} will now award the {tier.lower()} donator tier")
        await self.rebuild_donator_file(ctx.guild)
    
    @config.command(name="current")
    async def current_roles(self, ctx: commands.Context):
        """
        Shows the current role config for donator awards
        """
        roles = await self.config.guild(ctx.guild).donator_roles()
        roles = [ctx.guild.get_role(role) for role in roles]
        
        roledict = {}
        for role in roles:
            tier = await self.config.role(role).donator_tier()
            roletier = {role.name: tier}
            roledict.update(roletier)
        
        firstdict = {}
        seconddict = {}
        thirddict = {}
        for k, v in roledict.items():
            if v == "first":
                firstdict.update({k: v})
            elif v == "second":
                seconddict.update({k: v})
            elif v == "third":
                thirddict.update({k: v})
        
        embed = discord.Embed(
            title="Current donator roles:",
            color=await ctx.embed_color(),
            timestamp=datetime.datetime.utcnow()
        )
        firststring = "\n".join(["- {}: {}".format(k, v) for k, v in firstdict.items()])
        secondstring = "\n".join(["- {}: {}".format(k, v) for k, v in seconddict.items()])
        thirdstring = "\n".join(["- {}: {}".format(k, v) for k, v in thirddict.items()])
        
        embed.add_field(name="First tier", value=chat_formatting.box(firststring.strip(), "yaml"), inline=False)
        embed.add_field(name="Second tier", value=chat_formatting.box(secondstring.strip(), "yaml"), inline=False)
        embed.add_field(name="Third tier", value=chat_formatting.box(thirdstring.strip(), "yaml"), inline=False)
        
        await ctx.send("", embed=embed)
        
    
    async def rebuild_donator_file(self, guild: discord.Guild):
        folder = await self.config.guild(guild).config_folder()
        roles = await self.config.guild(guild).donator_roles()
        tgdb = self.get_tgdb()
        if(folder is None):
            folder = os.path.abspath(os.getcwd())
        folder = os.path.abspath(os.path.join(folder, "donator.toml"))
        
        roles = [guild.get_role(role) for role in roles]
        tier1 = []
        tier2 = []
        tier3 = []
        
        for role in roles:
            tier = await self.config.role(role).donator_tier()
            keys = await self.get_ckeys_from_role(role)
            if(tier == "first"):
                tier1 += keys
            if(tier == "second"):
                tier2 += keys
            if(tier == "third"):
                tier3 += keys
        
        with open(folder, mode="w") as donatorfile:
            new_text ="""
[donators]
tier_1 = [{tier1}]
tier_2 = [{tier2}]
tier_3 = [{tier3}]
""".format(tier1=", ".join(["\"{}\"".format(c) for c in tier1]), \
    tier2=", ".join(["\"{}\"".format(c) for c in tier2]), \
    tier3=", ".join(["\"{}\"".format(c) for c in tier3]))
            donatorfile.write(tomlkit.dumps(tomlkit.loads(new_text.strip() + "\n")))
    
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
    
    async def get_ckeys_from_role(self, role: discord.Role):
        """
        Get all ckeys from members that have a specific role
        """
        ckeys = []
        for member in role.members:
            link: DiscordLink = await self.link_from_member(member)
            if(not link is None):
                ckeys.append(link.ckey)
        return ckeys
    
    async def link_from_member(self, member: discord.Member):
        """
        Given a valid discord member, return the latest record linked to that user
        """
        prefix = await self.get_tgdb_prefix(member.guild)
        query = f"SELECT * FROM {prefix}discord_links WHERE discord_id = %s AND ckey IS NOT NULL ORDER BY timestamp DESC LIMIT 1"
        parameters = [member.id]
        results = await self.query_database(query, parameters)
        if len(results):
            return DiscordLink.from_db_record(results[0])

        return None
