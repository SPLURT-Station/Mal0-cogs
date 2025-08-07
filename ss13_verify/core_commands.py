"""
Core commands for the CkeyTools cog.
This module contains commands that are part of the main cog functionality.
"""

import logging
import datetime

import discord
from redbot.core import commands as red_commands, checks

log = logging.getLogger("red.ss13_verify.core")


class CoreCommandsMixin:
    """Mixin class providing core CkeyTools commands."""

    # Main command group
    @red_commands.group()
    async def ckeytools(self, ctx):
        """CkeyTools - SS13 Discord verification and management system."""
        pass

    # Status command
    @ckeytools.command()
    async def status(self, ctx):
        """Show the current status of CkeyTools."""
        config = await self.config.guild(ctx.guild).all()
        
        # Database status
        db_connected = self.db_manager.is_connected(ctx.guild.id)
        db_status = "✅ Connected" if db_connected else "❌ Disconnected"
        
        # Channel configurations
        panel_channel = ctx.guild.get_channel(config["ticket_channel"]) if config["ticket_channel"] else None
        ticket_category = ctx.guild.get_channel(config["ticket_category"]) if config["ticket_category"] else None
        
        # Role configurations
        verification_roles = [ctx.guild.get_role(r) for r in config["verification_roles"] if ctx.guild.get_role(r)]
        
        embed = discord.Embed(
            title="CkeyTools Status",
            color=discord.Color.green() if db_connected else discord.Color.red(),
            timestamp=datetime.datetime.now(datetime.timezone.utc)
        )
        
        # Database info
        embed.add_field(
            name="🗄️ Database",
            value=f"{db_status}\nHost: `{config['db_host']}:{config['db_port']}`\nDatabase: `{config['db_name']}`",
            inline=False
        )
        
        # Channel info
        embed.add_field(
            name="📋 Channels",
            value=f"Panel: {panel_channel.mention if panel_channel else '❌ Not set'}\nTicket Category: {ticket_category.name if ticket_category else '❌ Not set'}",
            inline=True
        )
        
        # Role info
        embed.add_field(
            name="👥 Roles",
            value=f"Verification Roles: {len(verification_roles)}\n" + (", ".join([r.mention for r in verification_roles[:3]]) + ("..." if len(verification_roles) > 3 else "") if verification_roles else "❌ None"),
            inline=True
        )
        
        # System toggles
        embed.add_field(
            name="⚙️ System Settings",
            value=f"Verification: {'✅' if config['verification_enabled'] else '❌'}\n"
                  f"Auto-verification: {'✅' if config['autoverification_enabled'] else '❌'}\n"
                  f"Auto-verify on join: {'✅' if config['autoverify_on_join_enabled'] else '❌'}\n"
                  f"Invalidate on leave: {'✅' if config['invalidate_on_leave'] else '❌'}\n"
                  f"Autodonator: {'✅' if config['autodonator_enabled'] else '❌'}",
            inline=False
        )
        
        await ctx.send(embed=embed)
