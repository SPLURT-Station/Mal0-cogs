"""
Autodonator commands mixin for the CkeyTools cog.
This module contains all autodonator related commands.
"""

import logging
import os
import datetime
from typing import Optional, List, Dict

import discord
from redbot.core import commands as red_commands, checks
from redbot.core.utils import chat_formatting

log = logging.getLogger("red.ss13_verify.autodonator.commands")


class AutodonatorCommandsMixin:
    """Mixin class providing autodonator commands."""

    # Autodonator commands (will be added to ckeytools group)
    @red_commands.group()
    async def ckeytools(self, ctx):
        """CkeyTools - SS13 Discord verification and management system."""
        pass

    @ckeytools.group(name="autodonator")
    @checks.admin_or_permissions(administrator=True)
    async def ckeytools_autodonator(self, ctx):
        """
        Automated donator role management and TOML file generation.
        
        This system creates and maintains TOML files with flexible tier naming
        for use with SS13 server donator systems.
        """
        pass

    @ckeytools_autodonator.command()
    async def toggle(self, ctx, enabled: bool = None):
        """Toggle automatic donator file updates."""
        if enabled is None:
            current = await self.config.guild(ctx.guild).autodonator_enabled()
            await ctx.send(f"Autodonator system is currently **{'enabled' if current else 'disabled'}**.")
            return
        
        await self.config.guild(ctx.guild).autodonator_enabled.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"Autodonator system is now **{status}**.")
        await ctx.tick()

    @ckeytools_autodonator.command()
    async def folder(self, ctx, *, folder_path: str):
        """
        Set the folder path where donator TOML files will be saved.
        
        Please provide the full path to the folder where you want the donator.toml file to be created.
        """
        folder_path = os.path.abspath(folder_path)
        if not (os.path.exists(folder_path) and os.path.isdir(folder_path)):
            await ctx.send("❌ The specified path is not a valid folder!")
            return
        
        await self.config.guild(ctx.guild).config_folder.set(folder_path)
        await ctx.send(f"✅ Donator file folder set to: `{folder_path}`")
        await ctx.tick()

    @ckeytools_autodonator.command()
    async def addtier(self, ctx, tier_path: str, role: discord.Role):
        """
        Add a role to a donator tier.
        
        The tier_path uses forward slashes to create nested structures.
        Examples:
        - `donators/tier_1`
        - `supporters/bronze`
        - `vip/premium/gold`
        """
        donator_tiers = await self.config.guild(ctx.guild).donator_tiers()
        
        if tier_path not in donator_tiers:
            donator_tiers[tier_path] = []
        
        if role.id not in donator_tiers[tier_path]:
            donator_tiers[tier_path].append(role.id)
            await self.config.guild(ctx.guild).donator_tiers.set(donator_tiers)
            await ctx.send(f"✅ Added {role.mention} to tier `{tier_path}`.")
            
            # Update donator file if enabled
            if await self.config.guild(ctx.guild).autodonator_enabled():
                await self.rebuild_donator_file(ctx.guild)
        else:
            await ctx.send(f"❌ {role.mention} is already in tier `{tier_path}`.")
        
        await ctx.tick()

    @ckeytools_autodonator.command()
    async def removetier(self, ctx, tier_path: str, role: discord.Role):
        """Remove a role from a donator tier."""
        donator_tiers = await self.config.guild(ctx.guild).donator_tiers()
        
        if tier_path not in donator_tiers:
            await ctx.send(f"❌ Tier `{tier_path}` does not exist.")
            return
        
        if role.id in donator_tiers[tier_path]:
            donator_tiers[tier_path].remove(role.id)
            
            # Clean up empty tiers
            if not donator_tiers[tier_path]:
                del donator_tiers[tier_path]
            
            await self.config.guild(ctx.guild).donator_tiers.set(donator_tiers)
            await ctx.send(f"✅ Removed {role.mention} from tier `{tier_path}`.")
            
            # Update donator file if enabled
            if await self.config.guild(ctx.guild).autodonator_enabled():
                await self.rebuild_donator_file(ctx.guild)
        else:
            await ctx.send(f"❌ {role.mention} is not in tier `{tier_path}`.")
        
        await ctx.tick()

    @ckeytools_autodonator.command()
    async def listtiers(self, ctx):
        """List all configured donator tiers and their roles."""
        donator_tiers = await self.config.guild(ctx.guild).donator_tiers()
        
        if not donator_tiers:
            await ctx.send("❌ No donator tiers configured.")
            return
        
        embed = discord.Embed(
            title="Donator Tiers Configuration",
            color=await ctx.embed_color(),
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        for tier_path, role_ids in donator_tiers.items():
            roles = [ctx.guild.get_role(role_id) for role_id in role_ids]
            role_mentions = []
            for i, role in enumerate(roles):
                if role:
                    role_mentions.append(role.mention)
                else:
                    role_mentions.append(f"<@&{role_ids[i]}> (deleted)")
            
            embed.add_field(
                name=f"`{tier_path}`",
                value="\n".join(role_mentions) if role_mentions else "No roles",
                inline=False
            )
        
        await ctx.send(embed=embed)

    @ckeytools_autodonator.command()
    async def update(self, ctx):
        """Manually update the donator TOML file."""
        if not await self.config.guild(ctx.guild).autodonator_enabled():
            await ctx.send("❌ Autodonator system is disabled. Enable it first with `[p]ckeytools autodonator toggle true`.")
            return
        
        await self.rebuild_donator_file(ctx.guild)
        await ctx.send("✅ Donator file updated successfully!")
        await ctx.tick()

    @ckeytools_autodonator.command()
    async def preview(self, ctx):
        """Preview the current donator TOML file content."""
        folder = await self.config.guild(ctx.guild).config_folder()
        if folder is None:
            folder = os.path.abspath(os.getcwd())
        
        file_path = os.path.abspath(os.path.join(folder, "donator.toml"))
        
        if not os.path.exists(file_path):
            await ctx.send("❌ Donator file does not exist. Run `[p]ckeytools autodonator update` first.")
            return
        
        try:
            with open(file_path, "r", encoding="utf-8") as donator_file:
                content = donator_file.read()
                
            if len(content) > 1900:  # Discord message limit consideration
                content = content[:1900] + "\n... (truncated)"
            
            await ctx.send(f"**Current donator.toml content:**\n{chat_formatting.box(content, 'toml')}")
        except Exception as e:
            await ctx.send(f"❌ Error reading donator file: {e}")

    @ckeytools_autodonator.command()
    async def cleartiers(self, ctx):
        """Clear all donator tier configurations."""
        view = ClearTiersConfirmView()
        await ctx.send("⚠️ **Warning:** This will remove all donator tier configurations. Are you sure?", view=view)
        
        # Wait for response
        await view.wait()
        
        if view.confirmed:
            await self.config.guild(ctx.guild).donator_tiers.set({})
            await ctx.send("✅ All donator tier configurations have been cleared.")
            await ctx.tick()
        else:
            await ctx.send("❌ Operation cancelled.")


class ClearTiersConfirmView(discord.ui.View):
    """Confirmation view for clearing donator tiers."""
    
    def __init__(self):
        super().__init__(timeout=60)
        self.confirmed = False

    @discord.ui.button(label="Yes, Clear All", style=discord.ButtonStyle.danger, emoji="⚠️")
    async def confirm_clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handle clear confirmation."""
        self.confirmed = True
        self.stop()
        await interaction.response.edit_message(content="✅ Confirmed. Clearing all donator tiers...", view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handle clear cancellation."""
        self.confirmed = False
        self.stop()
        await interaction.response.edit_message(content="❌ Operation cancelled.", view=None)

    async def on_timeout(self):
        """Handle view timeout."""
        for item in self.children:
            item.disabled = True
