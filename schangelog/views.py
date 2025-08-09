import discord
from datetime import date, datetime, timedelta
from typing import Optional, TYPE_CHECKING
from redbot.core import commands

if TYPE_CHECKING:
    from ._schangelog import SChangelog

class CalendarModal(discord.ui.Modal, title="Go to date"):
    date_input: discord.ui.TextInput

    def __init__(self, view: "ChangelogMenuView"):
        super().__init__(timeout=60)
        self.view_ref = view
        self.date_input = discord.ui.TextInput(label="Date (YYYY-mm-dd or 'today')", style=discord.TextStyle.short, required=True, max_length=20)
        self.add_item(self.date_input)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        view = self.view_ref
        if interaction.user.id != view.invoker_id:
            await interaction.response.send_message("You're not the user who invoked this menu.", ephemeral=True)
            return
        text = str(self.date_input.value).strip().lower()
        if text == "today":
            new_day = date.today()
        else:
            try:
                new_day = datetime.strptime(text, "%Y-%m-%d").date()
            except Exception:
                await interaction.response.send_message("Invalid date.", ephemeral=True)
                return
        await view.update_to_day(interaction, new_day)


class ChangelogMenuView(discord.ui.View):
    def __init__(self, cog: "SChangelog", ctx: commands.Context, start_day: date):
        super().__init__(timeout=60)
        self.cog = cog
        self.ctx = ctx
        self.current_day = start_day
        self.invoker_id = ctx.author.id
        self.message: Optional[discord.Message] = None

    async def on_timeout(self) -> None:
        try:
            if self.message:
                await self.message.edit(view=None)
        except Exception:
            pass

    async def update_to_day(self, interaction: discord.Interaction, new_day: date) -> None:
        # refresh message for new day
        self.current_day = new_day
        await interaction.response.defer()
        data = await self.cog._fetch_day_aggregated(self.ctx, new_day)
        embed = await self.cog._build_day_menu_embed(self.ctx, new_day, data)
        try:
            if self.message:
                await self.message.edit(embed=embed, view=self)
            else:
                await interaction.edit_original_response(embed=embed, view=self)
        except Exception:
            pass
        # View timeout is automatically refreshed on interactions in discord.py v2

    async def _ensure_invoker(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            if interaction.response.is_done():
                await interaction.followup.send("You're not the user who invoked this menu.", ephemeral=True)
            else:
                await interaction.response.send_message("You're not the user who invoked this menu.", ephemeral=True)
            return False
        return True

    @discord.ui.button(emoji="\u25C0\uFE0F", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._ensure_invoker(interaction):
            return
        await self.update_to_day(interaction, self.current_day - timedelta(days=1))

    @discord.ui.button(emoji="\U0001F4C5", style=discord.ButtonStyle.secondary)
    async def calendar_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message("You're not the user who invoked this menu.", ephemeral=True)
            return
        await interaction.response.send_modal(CalendarModal(self))

    @discord.ui.button(emoji="\u25B6\uFE0F", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._ensure_invoker(interaction):
            return
        await self.update_to_day(interaction, self.current_day + timedelta(days=1))

    @discord.ui.button(emoji="\u2716\uFE0F", style=discord.ButtonStyle.danger)
    async def delete_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._ensure_invoker(interaction):
            return
        try:
            await interaction.response.defer()
            if self.message:
                await self.message.delete()
        except Exception:
            pass
        finally:
            self.stop()
