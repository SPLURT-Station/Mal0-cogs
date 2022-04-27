#General imports
import os, random, validators, requests
from datetime import date, datetime
from typing import Optional
from shutil import rmtree
from urllib.error import HTTPError

#Discord imports
import discord

#Redbot imports
from redbot.core import commands, Config, checks
from redbot.core.utils import chat_formatting

#Folder imports
from .reader import readCl, RepoError

BaseCog = getattr(commands, "Cog", object)

class SChangelog(BaseCog):
    """
    Posts your current SS13 instance changelogs
    """

    __author__ = "Mosley"
    __version__ = "1.2.0"

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=908039527271104513, force_registration=True)

        default_guild = {
            "instancerepo": None,
            "gitlink": "https://github.com/SPLURT-Station/Mal0-cogs",
            "footer_lines": ["Changelogs"],
            "last_footer": None,
            "embed_color": (255, 79, 240),
            "mentionrole": None
        }

        self.config.register_guild(**default_guild)

    async def _send_cl_embed(self, ctx: commands.Context, channel: Optional[discord.TextChannel], day: Optional[str]):
        now = date.today()
        guild = ctx.guild
        guildpic = guild.icon_url
        instance = await self.config.guild(guild).instancerepo()
        footers = await self.config.guild(guild).footer_lines()
        gitlink = await self.config.guild(guild).gitlink()
        eColor = await self.config.guild(guild).embed_color()
        role = await self.config.guild(guild).mentionrole()
        role = discord.utils.get(guild.roles, id=role)
        numCh = 0
        nullCl = ""
        message = ""
        fallback = False

        if not channel:
            channel = ctx.channel
            role = None
        try:
            daydate = datetime.strptime(day, "%Y-%m-%d")
        except ValueError:
            return await channel.send("That's not a valid date, dummy")
        
        if day == now.strftime("%Y-%m-%d"):
            embedTitle = "Currently active changelogs"
        else:
            try:
                embedTitle = daydate.strftime("%d/%m/%Y")
            except:
                embedTitle = "Error"

        try:
            await self._download_cl_from_repo(ctx, daydate)
            instance = os.path.join(os.getcwd(), "temp")
        except:
            await ctx.send("Error while fetching from github link! Using fallback local repo.")
            fallback = True
        
        if not instance and fallback:
            return await channel.send("There is no configured repo yet!")

        if role:
            message = f"{role.mention}"

        try:
            (changes, numCh) = readCl(instance, day)
        except AttributeError:
            nullCl = "\nSeems like nothing happened on this day"
        except RepoError as e:
            return await channel.send(str(e))
        except Exception as e:
            raise e
        
        try:
            rmtree(os.path.join(os.getcwd(), "temp"))
        except:
            pass

        if numCh < 1:
            message = ""

        footer = random.choice(footers)
        while (len(footers) > 1) and footer == await self.config.guild(guild).last_footer():
            footer = random.choice(footers)
        await self.config.guild(guild).last_footer.set(footer)

        embed = discord.Embed(
            title=embedTitle,
            description=f"There were **{numCh}** active changelogs." + nullCl,
            color=discord.Colour.from_rgb(*eColor),
            timestamp=datetime.utcnow()
        )
        embed.set_author(name=f"{guild.name}'s Changelogs", url=gitlink, icon_url=guildpic)
        embed.set_footer(text=footer, icon_url=ctx.me.avatar_url)
        embed.set_thumbnail(url=guildpic)

        if len(nullCl):
            return await channel.send(message, embed=embed, allowed_mentions=discord.AllowedMentions(everyone=True, users=True, roles=True, replied_user=True))

        for k, v in changes.items():
            author = k
            cont = ""
            for t, c in v.items():
                cont += "\n" + t + ": "
                for i in c:
                    if len(cont + "\n  - " + i) > (1014):
                        embed.add_field(name=author, value=chat_formatting.box(cont.strip(), "yaml"), inline=False)
                        cont = "\n" + t + ": "
                        author = "\u200b"
                    cont += "\n  - " + i
            embed.add_field(name=author, value=chat_formatting.box(cont.strip(), "yaml"), inline=False)
        
        await channel.send(message, embed=embed, allowed_mentions=discord.AllowedMentions(everyone=True, users=True, roles=True, replied_user=True))
    
    async def _download_cl_from_repo(self, ctx: commands.Context, day: datetime):
        gitlink = await self.config.guild(ctx.guild).gitlink()
        rawlink = gitlink.replace("github.com", "raw.githubusercontent.com")
        rawlink += "/master/html/changelogs/archive/{yearmonth}.yml".format(yearmonth=day.strftime("%Y-%m"))
        archivedir = os.path.join(os.getcwd(), "temp/Repository/html/changelogs/archive")
        filedir = os.path.join(archivedir, day.strftime("%Y-%m") + ".yml")
        os.makedirs(archivedir, exist_ok=True)
        changelog = requests.get(rawlink)
        if changelog.text == "404: Not Found":
            raise HTTPError(rawlink, 404, "Not Found")
        with open(filedir, "w", encoding="utf-8") as monthfile:
            monthfile.write(changelog.text)

    @commands.guild_only()
    @commands.group(invoke_without_command=True, aliases=["scl"])
    async def schangelog(self, ctx, *, today: Optional[str]):
        """
        SS13 changelogs
        
        Use this to post the active changelogs in the current channel.

        - Today: Date of the changelog you want to get. in YYYY-mm-d format. (defaults to today)
        """
        if ctx.invoked_subcommand is None:
            if not today:
                today = date.today().strftime("%Y-%m-%d")
            await self._send_cl_embed(ctx, channel=None, day=today)

    @schangelog.command()
    @checks.admin_or_permissions(administrator=True)
    async def channel(self, ctx: commands.Context, channel: discord.TextChannel, *, today: Optional[str]):
        """
        Send the changelogs to a certain specific channel

        This command is supposed to be used in tandem with a command scheduler cog like https://github.com/bobloy/Fox-V3 's fifo in order to create an automatic changelogs channel.
        make sure that you set the auto changelogs to a time before they get compiled in the repo or this command will be useless!
        """
        if not today:
            today = date.today().strftime("%Y-%m-%d")
        await self._send_cl_embed(ctx, channel=channel, day=today)

    @schangelog.group(invoke_without_command=True)
    @checks.admin_or_permissions(administrator=True)
    async def set(self, ctx: commands.Context):
        """
        Changelog Configuration
        """
        if ctx.invoked_subcommand is None:
            guild = ctx.guild
            instance = await self.config.guild(guild).instancerepo()
            gitlink = await self.config.guild(guild).gitlink()
            eColor = await self.config.guild(guild).embed_color()
            role = await self.config.guild(guild).mentionrole()
            role = discord.utils.get(guild.roles, id=role)
            
            message = f"""
Current config:
  - repo: {instance}
  - link: {gitlink}
  - color: {discord.Colour.from_rgb(*eColor)}
  - role: {role}
""".strip()

            await ctx.send(chat_formatting.box(message, "yaml"))
    
    @set.command(name="repo")
    @checks.is_owner()
    async def repository(self, ctx: commands.Context, *, new_repo: Optional[str]):
        """
        Change the fallback changelog repository.

        Use this to save the path to your local server instance if you're running it in the same machine. 
        This is ONLY supposed to be used as a fallback if it fails to fetch from the link.
        """
        guild = ctx.guild
        if not new_repo:
            await self.config.guild(guild).instancerepo.clear()
            await ctx.send("`repo` has been reset to its default value")
            return

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
    async def set_gitlink(self, ctx: commands.Context, *, newLink: Optional[str]):
        """
        Change the link where the changelogs come from.
        
        Changelog files will be downloaded from here. Clicking on the embed's author will take you to this link as well.
        """

        if not newLink:
            await self.config.guild(ctx.guild).gitlink.clear()
            await ctx.send("`link` has been reset to its default value")
            return

        if not validators.url(newLink):
            await ctx.send("That's not a valid link!")
            return
        
        await self.config.guild(ctx.guild).gitlink.set(newLink)
        await ctx.tick()
    
    @set.command(name="color")
    async def set_color(self, ctx: commands.Context, *, newColor: Optional[discord.Colour]):
        """
        Change the color of the changelog embeds
        """

        if not newColor:
            await self.config.guild(ctx.guild).embed_color.clear()
            await ctx.send("`color` has been reset to its default value")
            return

        await self.config.guild(ctx.guild).embed_color.set(newColor.to_rgb())
        await ctx.tick()
    
    @set.command(name="role")
    async def set_mrole(self, ctx: commands.Context, *, newRole: Optional[discord.Role]):
        """
        Change the role that will be pinged when using the channel command.
        
        Defaults to none
        """
        if not newRole:
            await self.config.guild(ctx.guild).mentionrole.clear()
            await ctx.send("`role` has been reset to its default value")
            return
        
        await self.config.guild(ctx.guild).mentionrole.set(newRole.id)
        await ctx.tick()
    
    @set.command(name="reset")
    async def reset_config(self, ctx: commands.Context):
        """
        Reset all the data for the current guild

        This will clear everything, be careful!
        """
        await self.config.guild(ctx.guild).clear()
        await ctx.tick()
    
    @set.group(invoke_without_command=True)
    async def footers(self, ctx: commands.Context):
        """
        Command to edit and manage footers of the changelogs
        """
        if ctx.invoked_subcommand is None:
            footers = await self.config.guild(ctx.guild).footer_lines()
            message = ""
            for i in range(len(footers)):
                message += f"{i+1}. {footers[i]}\n"
            await ctx.send(chat_formatting.box(message.strip()))
    
    @footers.command(name="add")
    async def add_footer(self, ctx: commands.Context, *, newF: str):
        """
        Add a footer to the list of footers that can appear in the changelogs
        """
        current = await self.config.guild(ctx.guild).footer_lines()
        current.append(newF)
        await self.config.guild(ctx.guild).footer_lines.set(current)
        await ctx.tick()
    
    @footers.command(name="delete")
    async def remove_footer(self, ctx: commands.Context, *, delF: int):
        """
        Remove a footer from the footer list
        """
        toDelete = delF - 1
        current = await self.config.guild(ctx.guild).footer_lines()
        if (len(current) <= 1):
            return await ctx.send("There must be at least 1 active footer.")
        try:
            current.pop(toDelete)
        except IndexError:
            await ctx.send("Footer not found.")
            return
        await self.config.guild(ctx.guild).footer_lines.set(current)
        await ctx.tick()
