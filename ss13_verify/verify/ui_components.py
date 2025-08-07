"""
Discord UI components for the verification system.
"""

import logging
import discord
from discord.ext import commands

log = logging.getLogger("red.ss13_verify.verify.ui")


class VerificationButtonView(discord.ui.View):
    """Persistent view for the verification panel button."""

    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Start Verification",
        style=discord.ButtonStyle.primary,
        custom_id="verify_button",
        emoji="✅"
    )
    async def verify_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handle verification button clicks."""
        await self.cog.handle_verification_request(interaction.guild, interaction.user, interaction)


class VerificationCodeView(discord.ui.View):
    """Persistent view for the verification code input in tickets."""

    def __init__(self, cog):
        super().__init__(timeout=None)  # Persistent view
        self.cog = cog

    @discord.ui.button(
        label="Enter Verification Code",
        style=discord.ButtonStyle.secondary,
        custom_id="verify_code_button",
        emoji="🔑"
    )
    async def verify_code_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handle verification code button clicks."""
        # Get the channel and determine the ticket owner
        channel = interaction.channel
        
        # Try to find the ticket owner from the channel permissions
        ticket_owner = None
        for overwrite in channel.overwrites:
            if isinstance(overwrite, discord.Member) and overwrite != interaction.guild.me:
                # This is likely the ticket owner
                ticket_owner = overwrite
                break
        
        # Validate that the user clicking is the ticket owner or has permissions
        if ticket_owner and interaction.user.id != ticket_owner.id:
            # Check if user has manage_messages permission (staff)
            if not interaction.user.guild_permissions.manage_messages:
                await interaction.response.send_message(
                    "❌ Only the ticket owner or staff can use this button.",
                    ephemeral=True
                )
                return

        # Show modal for code input
        modal = VerificationCodeModal(self.cog, interaction.user, interaction.guild, channel)
        await interaction.response.send_modal(modal)

    async def on_timeout(self):
        """Handle view timeout."""
        # Disable all buttons
        for item in self.children:
            item.disabled = True


class VerificationCodeModal(discord.ui.Modal):
    """Modal for entering verification codes."""

    def __init__(self, cog, user: discord.User, guild: discord.Guild, ticket_channel=None):
        super().__init__(title="Discord Verification")
        self.cog = cog
        self.user = user
        self.guild = guild
        self.ticket_channel = ticket_channel

    verification_code = discord.ui.TextInput(
        label="Verification Code",
        placeholder="Enter your verification code from the game...",
        required=True,
        max_length=100
    )

    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission."""
        code = self.verification_code.value.strip()

        if not code:
            await interaction.response.send_message(
                "❌ Please enter a verification code.",
                ephemeral=True
            )
            return

        # Attempt verification
        try:
            verified_link = await self.cog.verify_code(self.guild, self.user, code)

            if verified_link:
                # Success!
                await interaction.response.send_message(
                    f"✅ **Verification successful!** Welcome, **{verified_link.ckey}**! 🎉"
                )

                # Finish verification process
                await self.cog.finish_verification(self.guild, self.user, verified_link.ckey, self.ticket_channel or interaction.channel)

            else:
                # Failed verification
                await interaction.response.send_message(
                    "❌ **Verification failed.** The code you entered is invalid or has already been used. "
                    "Please try again or contact staff for assistance.",
                    ephemeral=True
                )

        except Exception as e:
            log.error(f"Error during verification for user {self.user.id}: {e}")
            await interaction.response.send_message(
                "❌ **An error occurred during verification.** Please try again or contact staff.",
                ephemeral=True
            )


class DeverifyConfirmView(discord.ui.View):
    """Confirmation view for deverification."""

    def __init__(self, cog, target_user: discord.User, invoker: discord.User):
        super().__init__(timeout=60)  # 1 minute timeout
        self.cog = cog
        self.target_user = target_user
        self.invoker = invoker

    @discord.ui.button(label="Yes, Deverify", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_deverify(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handle deverification confirmation."""
        if interaction.user.id != self.invoker.id:
            await interaction.response.send_message(
                "❌ Only the person who initiated this command can confirm it.",
                ephemeral=True
            )
            return

        try:
            # Perform deverification
            await self.cog.perform_deverify(interaction.guild, self.target_user)

            # Send DM to target user with invite
            try:
                guild_invite = await self.cog.get_or_create_guild_invite(interaction.guild)
                dm_embed = discord.Embed(
                    title="Discord Verification Removed",
                    description=f"Your Discord verification has been removed from **{interaction.guild.name}**.",
                    color=discord.Color.orange()
                )
                dm_embed.add_field(
                    name="Rejoin to Reverify",
                    value=f"You can rejoin the server using this link to verify again: {guild_invite}",
                    inline=False
                )
                await self.target_user.send(embed=dm_embed)
            except discord.HTTPException:
                log.warning(f"Failed to send deverify DM to user {self.target_user.id}")

            # Kick the user
            member = interaction.guild.get_member(self.target_user.id)
            if member:
                try:
                    await member.kick(reason=f"Deverified by {self.invoker}")
                except discord.HTTPException as e:
                    log.warning(f"Failed to kick deverified user {self.target_user.id}: {e}")

            # Update interaction
            await interaction.response.edit_message(
                content=f"✅ **{self.target_user.display_name}** has been deverified and kicked from the server.",
                view=None
            )

        except Exception as e:
            log.error(f"Error during deverification of user {self.target_user.id}: {e}")
            await interaction.response.edit_message(
                content="❌ An error occurred during deverification. Please try again.",
                view=None
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_deverify(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handle deverification cancellation."""
        if interaction.user.id != self.invoker.id:
            await interaction.response.send_message(
                "❌ Only the person who initiated this command can cancel it.",
                ephemeral=True
            )
            return

        await interaction.response.edit_message(
            content="❌ Deverification cancelled.",
            view=None
        )

    async def on_timeout(self):
        """Handle view timeout."""
        # Disable all buttons
        for item in self.children:
            item.disabled = True
