#general imports
import io
import os
from pydub import AudioSegment
from datetime import timedelta

#discord imports
import discord

#Redbot imports
from redbot.core import commands, Config
from redbot.core.utils import antispam
from redbot.core.bot import Red

class AutoJukebox(commands.Cog):
    """
    Lets player suggest songs to automatically add to the jukebox in-game
    """
    __author__ = "Mosley"
    __version__ = "1.0.0"
    
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=908039527271104513, force_registration=True)
        self.config.register_guild(
            toggle = False,
            suggest_id = None,
            mods_id = None,
            next_id = 1,
            max_size = 0,
            max_length = 0,
            save_path = None
        )
        self.config.init_custom("JUKEBOX_SUGGESTION", 2)
        self.config.register_custom(
                        "JUKEBOX_SUGGESTION",
            author = [],
            msg_id = 0,
            finished = False,
            approved = False,
            rejected = False,
            song = None,
            length = 0,
            bpm = 0,
            )
        self.antispam = {}
    
    @commands.command(name="jukesuggest")
    @commands.guild_only()
    async def jukebox_suggest(self, ctx: commands.Context, bpm: int):
        """
        Suggest a song to be added to the jukebox
        
        - bpm: the beats per minute (bpm) of the song
        """
        suggest_id = await self.config.guild(ctx.guild).suggest_id()
        mods_id = await self.config.guild(ctx.guild).mods_id()
        current_id = await self.config.guild(ctx.guild).next_id()
        enabled = await self.config.guild(ctx.guild).toggle()
        max_song_size = (1024**3) * await self.config.guild(ctx.guild).max_size()
        max_song_length = await self.config.guild(ctx.guild).max_length()
        
        if not suggest_id or not mods_id or not enabled:
            return await ctx.message.reply("Uh oh, jukebox suggestions aren't enabled.")
        
        suggest_channel = discord.utils.get(ctx.guild.text_channels, id=suggest_id)
        mods_channel = discord.utils.get(ctx.guild.text_channels, id=mods_id)
        
        if not suggest_channel or not mods_channel:
            return await ctx.message.reply("Uh oh, jukebox suggestion channels not found.")
        
        if not ctx.channel == suggest_channel:
            await ctx.message.delete()
            return await ctx.send(f"You're not in {suggest_channel.mention}!", delete_after=5)
        
        antispam_key = (ctx.guild.id, ctx.author.id)
        if antispam_key not in self.antispam:
            self.antispam[antispam_key] = antispam.AntiSpam([(timedelta(minutes=1), 6)])
        if self.antispam[antispam_key].spammy:
            return await ctx.send("Uh oh, you're doing this way too frequently.")
        
        if not ctx.message.attachments:
            await ctx.message.delete()
            return await ctx.send(f"You must send an .ogg file.", delete_after=5)
        
        attachment = ctx.message.attachments[0]
        if not attachment.content_type in ('audio/ogg'):
            await ctx.message.delete()
            return await ctx.send(f"You must send an .ogg file.", delete_after=5)
        
        if attachment.size > max_song_size:
            return await ctx.message.reply(f"Your file is too thicc! the max filesize is {max_song_size}mb.", delete_after=5)
        
        async with ctx.typing():
            ogg_bytes = io.BytesIO(await attachment.read())
            ogg_audio = AudioSegment.from_file(ogg_bytes, format="ogg")
            
            if len(ogg_audio) > max_song_length*60000:
                return await ctx.message.reply(f"Your file is too hung! the max song length is {max_song_length} minutes.", delete_after=5)
            
            await suggest_channel.send(f"Jukebox suggestion #{current_id}", files=[attachment.to_file()])
            await ctx.tick()
        
        async with self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, current_id).author() as author:
            author.append(ctx.author.id)
            author.append(ctx.author.name)
            author.append(ctx.author.discriminator)
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, current_id).msg_id.set(ctx.message.id)
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, current_id).song.set(attachment.url)
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, current_id).length.set(len(ogg_audio))
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, current_id).bpm.set(bpm)
        await self.config.guild(ctx.guild).next_id.set(current_id + 1)
    
    @commands.command(name="jukeapprove")
    @commands.guild_only()
    @checks.admin_or_permissions(mention_everyone=True) # Idk what other permissions admins have that mods don't
    async def jukebox_approve(self, ctx: commands.Context, suggestion: int):
        """
        Approve a jukebox suggestion and add it to the jukebox files
        
        - suggestion: the number of the suggestion to approve
        """
        suggest_id = await self.config.guild(ctx.guild).suggest_id()
        enabled = await self.config.guild(ctx.guild).toggle()
        jukebox_folder = await self.config.guild(ctx.guild).save_path()
        
        if not enabled:
            return await ctx.send("Jukebox suggestions aren't enabled.")
        if not suggest_id:
            return await ctx.send("There's no suggestions channel.")
        if not jukebox_folder:
            return await ctx.send("The jukebox path hasn't been configured.")
        
        suggest_channel = discord.utils.get(ctx.guild.text_channels, id=suggest_id)
        jukebox_folder = os.path.abspath(jukebox_folder)
        
        msg_id = await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).msg_id()
        if msg_id != 0:
            if await self.config.custom("SUGGESTION", ctx.guild.id, suggestion).finished():
                return await ctx.send("This suggestion has been finished already.")
        
        oldmsg: discord.Message
        try:
            oldmsg = await suggest_channel.fetch_message(id=msg_id)
        except discord.NotFound:
            return await ctx.send("Uh oh, message with this ID doesn't exist.")
        if not oldmsg:
            return await ctx.send("Uh oh, message with this ID doesn't exist.")
        
        attachment = oldmsg.attachments[0]
        song_length = await self.config.custom("SUGGESTION", ctx.guild.id, suggestion).length()
        song_bpm = await self.config.custom("SUGGESTION", ctx.guild.id, suggestion).bpm()
        song_id = len([name for name in os.listdir(jukebox_folder) if os.path.isfile(name)])
        await attachment.save(os.path.join(jukebox_folder, f"{os.path.splitext(os.path.basename(attachment.filename))[0]}+{song_length/100}+{song_bpm}+{song_id}"))
        
        op_data = await self.config.custom("SUGGESTION", ctx.guild.id, suggestion).author()
        op = await self.bot.fetch_user(op_data[0])
        await op.send("Your song suggestion, " +  attachment.filename + " has been accepted!")
        
        await self.config.custom("SUGGESTION", ctx.guild.id, suggestion).finished.set(True)
        await self.config.custom("SUGGESTION", ctx.guild.id, suggestion).approved.set(True)
        
        await oldmsg.add_reaction("musical_note")
        await ctx.tick()
        
    @commands.command(name="jukereject")
    @commands.guild_only()
    @checks.admin_or_permissions(mention_everyone=True) # Idk what other permissions admins have that mods don't
    async def jukebox_reject(self, ctx: commands.Context, suggestion: int):
        """
        Reject a jukebox song
        
        - suggestion: the number of the jukebox suggestion to reject
        """
        suggest_id = await self.config.guild(ctx.guild).suggest_id()
        enabled = await self.config.guild(ctx.guild).toggle()

        if not enabled:
            return await ctx.send("Jukebox suggestions aren't enabled.")
        if not suggest_id:
            return await ctx.send("There's no suggestions channel.")
        
        suggest_channel = discord.utils.get(ctx.guild.text_channels, id=suggest_id)
        msg_id = await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).msg_id()
        if msg_id != 0:
            if await self.config.custom("SUGGESTION", ctx.guild.id, suggestion).finished():
                return await ctx.send("This suggestion has been finished already.")
            
        oldmsg: discord.Message
        try:
            oldmsg = await suggest_channel.fetch_message(id=msg_id)
        except discord.NotFound:
            return await ctx.send("Uh oh, message with this ID doesn't exist.")
        if not oldmsg:
            return await ctx.send("Uh oh, message with this ID doesn't exist.")
        
        attachment = oldmsg.attachments[0]
        
        op_data = await self.config.custom("SUGGESTION", ctx.guild.id, suggestion).author()
        op = await self.bot.fetch_user(op_data[0])
        await op.send("Your song suggestion, " + attachment.filename + "has been rejected.")
        
        await self.config.custom("SUGGESTION", ctx.guild.id, suggestion).finished.set(True)
        await self.config.custom("SUGGESTION", ctx.guild.id, suggestion).approved.set(True)
        
        await oldmsg.add_reaction("x")
        await ctx.tick()
        
    @commands.group()
    @commands.guild_only()
    @checks.admin_or_permissions(mention_everyone=True)
    async def setjukesuggest(self, ctx: commands.Context):
        """
        Jukebox suggestions settings
        """
        pass
    
    @setjukesuggest.command()
    async def toggle(self, ctx: commands.Context):
        """
        Toggle jukebox suggestions
        """
        current_toggle = await self.config.guild(ctx.guild).toggle()
        await self.config.guild(ctx.guild).toggle.set(not current_toggle)
        await ctx.tick()
    
    @setjukesuggest.command()
    async def suggestchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """
        Set the channel for jukebox suggestions
        """
        await self.config.guild(ctx.guild).suggest_id.set(channel.id)
        await ctx.tick()
    
    @setjukesuggest.command()
    async def modschannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """
        Set the channel for jukebox mod actions
        """
        await self.config.guild(ctx.guild).mods_id.set(channel.id)
        await ctx.tick()
    
    @setjukesuggest.command()
    @checks.is_owner()
    async def savepath(self, ctx: commands.Context, path: str):
        """
        Set the path for jukebox files
        """
        await self.config.guild(ctx.guild).save_path.set(path)
        await ctx.tick()
    
    @setjukesuggest.command()
    async def maxlength(self, ctx: commands.Context, length: int):
        """
        Set the max length for jukebox suggestions (in minutes)
        """
        await self.config.guild(ctx.guild).max_length.set(length)
        await ctx.tick()
    
    @setjukesuggest.command()
    async def maxsize(self, ctx: commands.Context, size: int):
        """
        Set the max size for jukebox suggestions (in MB)
        """
        await self.config.guild(ctx.guild).max_size.set(size)
        await ctx.tick()
