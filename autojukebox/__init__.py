from .autojukebox import AutoJukebox

async def setup(bot):
    await bot.add_cog(AutoJukebox(bot))
