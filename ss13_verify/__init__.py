from .ss13_verify import SS13Verify

async def setup(bot):
    await bot.add_cog(SS13Verify(bot))
