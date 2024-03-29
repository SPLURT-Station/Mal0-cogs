#general imports
import io
import os
from pydub import AudioSegment
from datetime import timedelta

#discord imports
import discord

#Redbot imports
from redbot.core import commands, Config, checks
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
            save_path = None,
            ffmpeg_path = None
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
        Remember to attach an .ogg file to your suggestion. Keep in mind
        the name you give your file is how it'll appear in game!
        
        - bpm: the beats per minute (bpm) of the song
        """
        suggest_id = await self.config.guild(ctx.guild).suggest_id()
        mods_id = await self.config.guild(ctx.guild).mods_id()
        current_id = await self.config.guild(ctx.guild).next_id()
        enabled = await self.config.guild(ctx.guild).toggle()
        max_song_size = (1024**2) * await self.config.guild(ctx.guild).max_size()
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
            return await ctx.message.reply(f"Your file is too thicc! the max filesize is {round(max_song_size / 1024**2, 2)}mb.")
        
        async with ctx.typing():
            path_name = os.path.abspath(os.path.join(os.getcwd(), attachment.filename))
            await attachment.save(path_name)
            
            ogg_audio = AudioSegment.from_ogg(path_name)
            os.remove(path_name)
            
            if len(ogg_audio) > max_song_length*60000:
                return await ctx.message.reply(f"Your file is too hung! the max song length is {max_song_length} minutes.")
            
            suggest_msg = await mods_channel.send(f"Jukebox suggestion #{current_id} by {ctx.author.mention}", files=[await attachment.to_file()])
            await ctx.tick()
        
        async with self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, current_id).author() as author:
            author.append(ctx.author.id)
            author.append(ctx.author.name)
            author.append(ctx.author.discriminator)
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, current_id).msg_id.set(suggest_msg.id)
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, current_id).song.set(attachment.url)
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, current_id).length.set(len(ogg_audio))
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, current_id).bpm.set(bpm)
        await self.config.guild(ctx.guild).next_id.set(current_id + 1)
        
        self.antispam[antispam_key].stamp()
        
    async def approve_song(self, ctx: commands.Context, suggestion: int):
        """
        Approve a jukebox suggestion and add it to the jukebox files
        
        - suggestion: the number of the suggestion to approve
        """
        
        mods_id = await self.config.guild(ctx.guild).mods_id()
        enabled = await self.config.guild(ctx.guild).toggle()
        jukebox_folder = await self.config.guild(ctx.guild).save_path()
        ffmpeg_folder = await self.config.guild(ctx.guild).ffmpeg_path()
        
        if not enabled:
            return await ctx.send("Jukebox suggestions aren't enabled.")
        if not mods_id:
            return await ctx.send("There's no suggestions channel.")
        if not jukebox_folder or not ffmpeg_folder:
            return await ctx.send("The jukebox path hasn't been configured.")
        
        mods_channel = discord.utils.get(ctx.guild.text_channels, id=mods_id)
        jukebox_folder = os.path.abspath(jukebox_folder)
        
        msg_id = await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).msg_id()
        if msg_id != 0:
            if await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).finished():
                return await ctx.send("This suggestion has been finished already.")
        
        oldmsg: discord.Message
        try:
            oldmsg = await mods_channel.fetch_message(msg_id)
        except discord.NotFound:
            return await ctx.send(f"Uh oh, message with this ID {msg_id} doesn't exist.")
        if not oldmsg:
            return await ctx.send(f"Uh oh, message with ID {msg_id} doesn't exist.")
        
        attachment = oldmsg.attachments[0]
        song_length = await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).length()
        song_bpm = await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).bpm()
        song_id = len([name for name in os.listdir(jukebox_folder)]) + 1
        await attachment.save(os.path.join(jukebox_folder, f"{os.path.splitext(os.path.basename(attachment.filename.replace('_', ' ').replace('+', ' ')))[0]}+{song_length/100}+{song_bpm}+{song_id}.ogg"))
        
        op_data = await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).author()
        op = await self.bot.fetch_user(op_data[0])
        try:
            await op.send("Your song suggestion, `" +  attachment.filename + "` has been accepted!")
        except:
            await ctx.send("Could not notify " + op.mention + " of their song approval")
        
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).finished.set(True)
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).approved.set(True)
        await oldmsg.add_reaction(self.bot.get_emoji(933392769647534100))
    
    @commands.group(invoke_without_command=True, name="jukeapprove")
    @commands.guild_only()
    @checks.admin_or_permissions(mention_everyone=True, manage_messages=True)
    async def jukebox_approve(self, ctx: commands.Context, suggestion: int):
        if ctx.invoked_subcommand is None:
            async with ctx.typing():
                await self.approve_song(ctx, suggestion)
            await ctx.tick()
        
    @jukebox_approve.command(name="mass")
    async def jukebox_approve_mass(self, ctx: commands.Context, a: int, b: int):
        """
        Mass approve jukebox suggestions
        
        - a: the first suggestion to approve
        - b: the last suggestion to approve
        """
        async with ctx.typing():
            for i in range(a, b+1):
                await self.approve_song(ctx, i)
        await ctx.tick()
            
    async def reject_song(self, ctx: commands.Context, suggestion: int):
        """
        Reject a jukebox song
        
        - suggestion: the number of the jukebox suggestion to reject
        """
        
        mods_id = await self.config.guild(ctx.guild).mods_id()
        enabled = await self.config.guild(ctx.guild).toggle()

        if not enabled:
            return await ctx.send("Jukebox suggestions aren't enabled.")
        if not mods_id:
            return await ctx.send("There's no suggestions channel.")
        
        mods_channel = discord.utils.get(ctx.guild.text_channels, id=mods_id)
        msg_id = await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).msg_id()
        if msg_id != 0:
            if await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).finished():
                return await ctx.send("This suggestion has been finished already.")
            
        oldmsg: discord.Message
        try:
            oldmsg = await mods_channel.fetch_message(msg_id)
        except discord.NotFound:
            return await ctx.send("Uh oh, message with this ID doesn't exist.")
        if not oldmsg:
            return await ctx.send("Uh oh, message with this ID doesn't exist.")
        
        attachment = oldmsg.attachments[0]
        
        op_data = await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).author()
        op = await self.bot.fetch_user(op_data[0])
        try:
            await op.send("Your song suggestion, `" + attachment.filename + "` has been rejected.")
        except:
            await ctx.send("Could not notify " + op.mention + " of their song denial")
        
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).finished.set(True)
        await self.config.custom("JUKEBOX_SUGGESTION", ctx.guild.id, suggestion).approved.set(True)
        await ctx.tick()
        await oldmsg.add_reaction(self.bot.get_emoji(933392807727607818))
        
    @commands.group(invoke_without_command=True, name="jukereject")
    @commands.guild_only()
    @checks.admin_or_permissions(mention_everyone=True, manage_messages=True)
    async def jukebox_reject(self, ctx: commands.Context, suggestion: int):
        if ctx.invoked_subcommand is None:
            async with ctx.typing():
                await self.reject_song(ctx, suggestion)
            await ctx.tick()
        
    @jukebox_reject.command(name="mass")
    async def jukebox_reject_mass(self, ctx: commands.Context, a: int, b: int):
        """
        Mass reject jukebox suggestions
        
        - a: the first suggestion to reject
        - b: the last suggestion to reject
        """
        async with ctx.typing():
            for i in range(a, b+1):
                await self.reject_song(ctx, i)
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
        await ctx.message.reply(f"Jukebox suggestions are now {'enabled' if not current_toggle else 'disabled'}")
    
    @setjukesuggest.command()
    async def suggestchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """
        Set the channel for jukebox suggestions
        """
        await self.config.guild(ctx.guild).suggest_id.set(channel.id)
        await ctx.tick()
        await ctx.message.reply(f"Jukebox suggestions will now be posted in {channel.mention}")
    
    @setjukesuggest.command()
    async def modschannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """
        Set the channel for jukebox mod actions
        """
        await self.config.guild(ctx.guild).mods_id.set(channel.id)
        await ctx.tick()
        await ctx.message.reply(f"Jukebox mod actions will now be sent to {channel.mention}")
    
    @setjukesuggest.command()
    @checks.is_owner()
    async def savepath(self, ctx: commands.Context, *, path: str):
        """
        Set the path for jukebox files
        """
        await self.config.guild(ctx.guild).save_path.set(path)
        await ctx.tick()
        await ctx.message.reply(f"Jukebox files will now be saved to {path}")
    
    @setjukesuggest.command()
    async def maxlength(self, ctx: commands.Context, length: float):
        """
        Set the max length for jukebox suggestions (in minutes)
        """
        await self.config.guild(ctx.guild).max_length.set(length)
        await ctx.tick()
        await ctx.message.reply(f"Jukebox suggestions will now be limited to {length} minutes")
    
    @setjukesuggest.command()
    async def maxsize(self, ctx: commands.Context, size: float):
        """
        Set the max size for jukebox suggestions (in MB)
        """
        await self.config.guild(ctx.guild).max_size.set(size)
        await ctx.tick()
        await ctx.message.reply(f"Jukebox suggestions will now be limited to {size} MB")

    @setjukesuggest.command()
    @checks.is_owner()
    async def ffmpeg(self, ctx: commands.Context, *, path: str):
        """
        Set the path for ffmpeg
        """
        await self.config.guild(ctx.guild).ffmpeg_path.set(os.path.abspath(path))
        AudioSegment.ffmpeg = os.path.abspath(path)
        await ctx.tick()
        await ctx.message.reply(f"ffmpeg is now set to {path}")
