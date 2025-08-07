from .ckeytools import CkeyTools

async def setup(bot):
    await bot.add_cog(CkeyTools(bot))
