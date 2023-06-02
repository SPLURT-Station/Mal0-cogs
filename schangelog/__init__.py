from ._schangelog import SChangelog

async def setup(bot):
    await bot.add_cog(SChangelog(bot))
