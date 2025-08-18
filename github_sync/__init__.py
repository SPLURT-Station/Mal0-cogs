async def setup(bot):
    from .github_sync import GitHubSync
    await bot.add_cog(GitHubSync(bot))


