from .suggestbounties import SuggestBounties

async def setup(bot):
    await bot.add_cog(SuggestBounties(bot)) 
