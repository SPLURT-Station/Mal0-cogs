from .tgs_commands import TGSCommands

async def setup(bot):
    await bot.add_cog(TGSCommands(bot)) 
