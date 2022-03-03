#General imports
import os, random, validators
from datetime import date
from typing import Optional

#Discord imports
import discord

#Redbot imports
from redbot.core import commands, Config, checks
from redbot.core.utils import chat_formatting

#Folder imports
import reader

BaseCog = getattr(commands, "Cog", object)

class SChangelog(BaseCog):
    """
    Posts your current SS13 instance changelogs
    """

    __author__ = "Mosley"
    __version__ = "1.0.0"

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=908039527271104513, force_registration=True)
        self.timer = None

        default_guild = {
            "instancerepo": None,
            "gitlink": "https://github.com/SPLURT-Station/Mal0-cogs",
            "footer_lines": ["Changelogs"],
            "embed_color": discord.Colour.from_rgb(255, 79, 240),
            "mentionrole": None
        }

        self.config.register_guild(**default_guild)

    async def _send_cl_embed(self, ctx: commands.Context, channel: Optional[discord.TextChannel]):
        now = date.today()
        guild = ctx.guild()
        guildpic = guild.icon_url
        instance = await self.config.guild(guild).instancerepo()
        footers = await self.config.guild(guild).footer_lines()
        gitlink = await self.config.guild(guild).gitlink()
        eColor = await self.config.guild(guild).embed_color()
        role = await self.config.guild(guild).mentionrole()
        message = ""

        if not channel:
            channel = ctx.channel()
            embedTitle = "Currently active changelogs"
        else:
            embedTitle = now.strftime("%d/%m/%Y")
        
        if not instance:
            return await channel.send("There is no configured repo yet!")

        if role:
            message = f"{role.mention}"
        
        (numCh, changes) = reader.reader(instance)

        embed = discord.Embed(
            title=embedTitle,
            description=f"There are currently **{numCh}** active changelogs.",
            color=eColor
        )
        embed.set_author(name=f"{guild.name}'s Changelogs", url=gitlink, icon_url=guildpic)
        embed.set_footer(text=random.choice(footers), icon_url=self.bot.icon_url)
        embed.set_thumbnail(url=guildpic)
        for k, v in changes.items():
            author = k
            cont = ""
            for t, c in v.items():
                cont += "\n" + t + ": "
                for i in c:
                    cont += "\n  - " + i
            embed.add_field(name=author, value=chat_formatting.box(cont.strip(), "yaml"), inline=False)
        
        await channel.send(message, embed=embed)
        #lines = ["Creating THE furry cum dungeon, one PR at the time.", "\"Code it yourself.\"", "These people work very hard. Someone give them love.", "We love you :3", "\"Literally 1984.\""]

    @commands.guild_only()
    @commands.group(aliases=["scl"])
    async def schangelog(self, ctx):
        """
        SS13 changelog main commmand
        """
        if ctx.invoked_subcommand is None:
            await self._send_cl_embed(ctx)

    @schangelog.command()
    @checks.admin_or_permissions(administrator=True)
    async def channel(self, ctx: commands.Context, *, channel: discord.TextChannel):
        """
        Send the changelogs to a certain specific channel
        """
        await self._send_cl_embed(ctx, channel)

    @schangelog.group(invoke_without_command=True)
    @checks.admin_or_permissions(administrator=True)
    async def set(self, ctx: commands.Context):
        """
        Changelog Configuration
        """
        if ctx.invoked_subcommand is None:
            guild = ctx.guild()
            instance = await self.config.guild(guild).instancerepo()
            gitlink = await self.config.guild(guild).gitlink()
            eColor = await self.config.guild(guild).embed_color()
            role = await self.config.guild(guild).mentionrole()

            message = f"""
Current config:
  - repo: {instance}
  - link: {gitlink}
  - color: {eColor}
  - role: {role.name}
""".strip()

            await ctx.send(chat_formatting.box(message, "yaml"))
    
    @set.command(name="repo")
    @checks.is_owner()
    async def repository(self, ctx: commands.Context, *, new_repo: str):
        """
        Change the Changelog Repository
        """
        guild = ctx.guild()
        try:
            location = os.path.abspath(new_repo)
            if not os.path.exists(location):
                raise ValueError
            await self.config.guild(guild).instancerepo.set(location)
            await ctx.tick()
        except ValueError:
            await ctx.send("This location does not exist!")
        except:
            await ctx.send("There was an error while setting this configuration.")
        return
    
    @set.command(name="link")
    async def set_gitlink(self, ctx: commands.Context, *, newLink: str):
        """
        Change the link that clicking on the changelog's author will direct to
        """
        if not validators.url(newLink):
            await ctx.send("That's not a valid link!")
            return
        
        await self.config.guild(ctx.guild()).gitlink.set(newLink)
        await ctx.tick()
    
    @set.command(name="color")
    async def set_color(self, ctx: commands.Context, *, newColor: discord.Colour):
        """
        Change the color of the changelog embeds
        """
        await self.config.guild(ctx.guild()).embed_color.set(newColor)
        await ctx.tick()
    
    @set.command(name="role")
    async def set_mrole(self, ctx: commands.Context, *, newRole: discord.Role):
        """
        Change the role that will be pinged when using the channel command.
        
        Defaults to none
        """
        await self.config.guild(ctx.guild()).mentionrole.set(newRole)
        await ctx.tick()
    
    @set.group()
    async def footers(self, ctx: commands.Context):
        """
        Command to edit and manage footers of the changelogs
        """
        if ctx.invoked_subcommand is None:
            footers = await self.config.guild(ctx.guild()).footers()
            message = ""
            for i in range(len(footers)):
                message += f"{i+1}. {footers[i]}\n"
            await ctx.send(chat_formatting.box(message.strip()))
    
    @footers.command(name="add")
    async def add_footer(self, ctx: commands.Context, *, newF: str):
        """
        Add a footer to the list of footers that can appear in the changelogs
        """
        current = await self.config.guild(ctx.guild()).footers()
        current.append(newF)
        await self.config.guild(ctx.guild()).footers.set(current)
        await ctx.tick()
    
    @footers.command(name="delete")
    async def remove_footer(self, ctx: commands.Context, *, delF: int):
        """
        Remove a footer from the footer list
        """
        toDelete = delF - 1
        current = await self.config.guild(ctx.guild()).footers()
        if (len(current) <= 1):
            return await ctx.send("There must be at least 1 active footer.")
        try:
            current.pop(toDelete)
        except IndexError:
            await ctx.send("Footer not found.")
            return
        await self.config.guild(ctx.guild()).footers.set(current)
        await ctx.tick()
