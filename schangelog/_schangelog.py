#General imports
import random
import requests  # type: ignore[import]
import yaml  # type: ignore[import]
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

#Folder imports
from .views import ChangelogMenuView

#Discord imports
import discord

#Redbot imports
from redbot.core import commands, Config, checks
from redbot.core.utils import chat_formatting

class SChangelog(commands.Cog):
    """
    Posts your current SS13 instance changelogs
    """

    __author__ = "Mosley, rework by Mal0"
    __version__ = "2.0.0"

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=908039527271104513, force_registration=True)

        default_guild = {
            # List of sources: each is {owner, repo, branch, path}
            # path should point to the folder that contains monthly %Y-%m.yml files
            "sources": [],
            # Cached aggregated data: { "YYYY-MM": { "YYYY-MM-DD": { author: { tag: [entries] } } } }
            "cached": {},
            # Number of days before a reference day to consider for updates/diffs
            "cache_days": 7,
            # Visual customization
            "footer_lines": ["Changelogs"],
            "last_footer": None,
            "embed_color": (255, 79, 240),
            "mentionrole": None,
        }

        self.config.register_guild(**default_guild)

    # ==========================
    # Internal data helpers
    # ==========================

    @staticmethod
    def _source_to_raw_base(source: Dict[str, str]) -> str:
        owner = source.get("owner", "").strip()
        repo = source.get("repo", "").strip()
        branch = source.get("branch", "main").strip() or "main"
        path = source.get("path", "").strip().strip("/")
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{path}"

    @staticmethod
    def _normalize_day_key(k: object) -> Optional[str]:
        # YAML may parse unquoted dates as datetime.date
        if isinstance(k, date):
            return k.strftime("%Y-%m-%d")
        try:
            # strings or other types coerced to date
            s = str(k)
            return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
        except Exception:
            return None

    @staticmethod
    def _ensure_nested_dict(d: dict, *keys: str) -> dict:
        cur = d
        for key in keys:
            if key not in cur or not isinstance(cur[key], dict):
                cur[key] = {}
            cur = cur[key]
        return cur

    @staticmethod
    def _merge_author_changes(target: dict, source: dict) -> None:
        # target and source are { tag: [entries] }
        for tag, entries in source.items():
            if tag not in target:
                target[tag] = []
            for entry in entries:
                if entry not in target[tag]:
                    target[tag].append(entry)

    @staticmethod
    def _parse_month_yaml(yaml_text: str) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
        # Returns { day: { author: { tag: [entry, ...] } } }
        try:
            loaded = yaml.load(yaml_text, Loader=yaml.SafeLoader) or {}
        except Exception:
            return {}

        result: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
        for raw_day, authors in (loaded.items() if isinstance(loaded, dict) else []):
            day_key = SChangelog._normalize_day_key(raw_day)
            if day_key is None:
                continue
            if not isinstance(authors, dict):
                continue
            day_bucket = result.setdefault(day_key, {})
            for author, tag_items in authors.items():
                if not isinstance(tag_items, list):
                    continue
                author_bucket: Dict[str, List[str]] = day_bucket.setdefault(str(author), {})
                for item in tag_items:
                    if not isinstance(item, dict) or not item:
                        continue
                    # single-key dict: {tag: text}
                    tag, text = list(item.items())[0]
                    tag = str(tag)
                    text_value = str(text)
                    author_bucket.setdefault(tag, [])
                    if text_value not in author_bucket[tag]:
                        author_bucket[tag].append(text_value)
        return result

    async def _fetch_month_from_sources(self, ctx: commands.Context, year_month: str) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
        # Aggregate month across all sources
        sources = await self.config.guild(ctx.guild).sources()
        aggregated: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
        for source in sources:
            raw_base = self._source_to_raw_base(source)
            url = f"{raw_base}/{year_month}.yml"
            try:
                resp = requests.get(url, timeout=20)
                if resp.status_code != 200 or resp.text.strip() == "404: Not Found":
                    continue
                month_doc = self._parse_month_yaml(resp.text)
                # merge into aggregated
                for day, authors in month_doc.items():
                    day_bucket = aggregated.setdefault(day, {})
                    for author, tags in authors.items():
                        author_bucket = day_bucket.setdefault(author, {})
                        self._merge_author_changes(author_bucket, tags)
            except Exception:
                # Ignore this source for this month
                continue
        return aggregated

    @staticmethod
    def _month_range(start_date: date, end_date: date) -> List[str]:
        # inclusive range of YYYY-MM strings
        months = []
        cur = date(start_date.year, start_date.month, 1)
        end = date(end_date.year, end_date.month, 1)
        while cur <= end:
            months.append(cur.strftime("%Y-%m"))
            # next month
            if cur.month == 12:
                cur = date(cur.year + 1, 1, 1)
            else:
                cur = date(cur.year, cur.month + 1, 1)
        return months

    @staticmethod
    def _compute_window_for_ref_day(reference_day: date, days_before: int) -> Tuple[date, date]:
        # window includes the reference day and the specified number of days before
        start = reference_day - timedelta(days=max(0, int(days_before)))
        end = reference_day
        return start, end

    @staticmethod
    def _diff_day(remote: Dict[str, Dict[str, List[str]]], local: Dict[str, Dict[str, List[str]]]) -> Dict[str, Dict[str, List[str]]]:
        # Return entries present in remote but not in local
        diff: Dict[str, Dict[str, List[str]]] = {}
        for author, tags in remote.items():
            for tag, entries in tags.items():
                for entry in entries:
                    if author not in local or tag not in local.get(author, {}) or entry not in local.get(author, {}).get(tag, []):
                        diff.setdefault(author, {}).setdefault(tag, []).append(entry)
        return diff

    async def _fetch_window_from_sources(self, ctx: commands.Context, start_date: date, end_date: date) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
        # Aggregate all days in window across months
        aggregated: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
        for ym in self._month_range(start_date, end_date):
            remote_month = await self._fetch_month_from_sources(ctx, ym)
            for day, authors in remote_month.items():
                try:
                    d = datetime.strptime(day, "%Y-%m-%d").date()
                except Exception:
                    continue
                if d < start_date or d > end_date:
                    continue
                day_bucket = aggregated.setdefault(day, {})
                for author, tags in authors.items():
                    author_bucket = day_bucket.setdefault(author, {})
                    self._merge_author_changes(author_bucket, tags)
        return aggregated

    async def _build_cache_for_range(self, ctx: commands.Context, start_date: date, end_date: date) -> Tuple[int, List[str]]:
        # Build a fresh cache solely for the given window
        window_data = await self._fetch_window_from_sources(ctx, start_date, end_date)
        new_cache: Dict[str, Dict[str, Dict[str, Dict[str, List[str]]]]] = {}
        for day, authors in window_data.items():
            ym = day[:7]
            month_bucket = new_cache.setdefault(ym, {})
            month_bucket[day] = authors
        await self.config.guild(ctx.guild).cached.set(new_cache)
        return len(window_data), list({day[:7] for day in window_data.keys()})

    def _build_fields_from_aggregated(self, aggregated_by_author: Dict[str, Dict[str, List[str]]]) -> List[Tuple[str, str]]:
        # Convert aggregated author->tag->[entries] mapping into a list of (field_name, field_value)
        fields: List[Tuple[str, str]] = []
        for author, tags in aggregated_by_author.items():
            shown_name = author
            content = ""
            for tag, entries in tags.items():
                content += f"\n{tag}: "
                for entry in entries:
                    if len(content + "\n  - " + entry) > 1014:
                        fields.append((shown_name, chat_formatting.box(content.strip(), "yaml")))
                        content = f"\n{tag}: "
                        shown_name = "\u200b"
                    content += "\n  - " + entry
            if content.strip():
                fields.append((shown_name, chat_formatting.box(content.strip(), "yaml")))
        return fields

    def _new_base_embed(self, ctx: commands.Context, guild: discord.Guild, title: str, description: str, eColor: Tuple[int, int, int], footer: str, author_url: Optional[str]) -> discord.Embed:
        guildpic = guild.icon
        embed = discord.Embed(title=title, description=description, color=discord.Colour.from_rgb(*eColor), timestamp=discord.utils.utcnow())
        embed.set_author(name=f"{guild.name}'s Changelogs", url=author_url, icon_url=guildpic)
        embed.set_footer(text=footer, icon_url=ctx.me.avatar)
        embed.set_thumbnail(url=guildpic)
        return embed

    def _paginate_embeds(self, ctx: commands.Context, guild: discord.Guild, base_title: str, base_description: str, eColor: Tuple[int, int, int], footer: str, author_url: Optional[str], fields: List[Tuple[str, str]]) -> List[discord.Embed]:
        # Build a list of embeds, each under Discord's 6000 character limit and 25 fields
        pages: List[discord.Embed] = []
        current = self._new_base_embed(ctx, guild, base_title, base_description, eColor, footer, author_url)
        field_count = 0
        for name, value in fields:
            # If adding this field would exceed limits, start a new page
            if field_count >= 25 or (len(current) + len(str(name)) + len(str(value)) > 5900):
                pages.append(current)
                # continued title for subsequent pages
                current = self._new_base_embed(ctx, guild, f"{base_title} (cont.)", base_description, eColor, footer, author_url)
                field_count = 0
            current.add_field(name=name, value=value, inline=False)
            field_count += 1
        # Always append the last embed, even if it has zero fields (header-only)
        pages.append(current)
        return pages

    async def _send_diff_embed_for_window(self, ctx: commands.Context, ping_enabled: bool, ref_day: str):
        now = date.today()
        guild = ctx.guild
        assert guild is not None
        guildpic = guild.icon
        footers = await self.config.guild(guild).footer_lines()
        eColor = await self.config.guild(guild).embed_color()
        role_id = await self.config.guild(guild).mentionrole()
        role = discord.utils.get(guild.roles, id=role_id) if ping_enabled else None
        channel = ctx.channel

        ref_day_str = ref_day.strip().lower()
        if ref_day_str == "today":
            daydate = now
        else:
            try:
                daydate = datetime.strptime(ref_day, "%Y-%m-%d").date()
            except ValueError:
                return await channel.send("That's not a valid date, dummy")

        cache_days = await self.config.guild(guild).cache_days()
        start_date, end_date = self._compute_window_for_ref_day(daydate, int(cache_days))

        embedTitle = "Currently active changelogs" if end_date == now else f"Changelogs {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}"

        # Fetch remote window
        remote_window = await self._fetch_window_from_sources(ctx, start_date, end_date)

        # Build diff against local cache across the window and aggregate by author
        cached = await self.config.guild(guild).cached()
        if not isinstance(cached, dict):
            cached = {}
        aggregated_by_author: Dict[str, Dict[str, List[str]]] = {}
        for day in sorted(remote_window.keys()):
            ym = day[:7]
            local_day = cached.get(ym, {}).get(day, {})
            remote_day = remote_window.get(day, {})
            day_diff = self._diff_day(remote_day, local_day)
            if not day_diff:
                continue
            for author, tags in day_diff.items():
                author_bucket = aggregated_by_author.setdefault(author, {})
                self._merge_author_changes(author_bucket, tags)
        num_authors = len(aggregated_by_author)

        footer = random.choice(footers)
        while (len(footers) > 1) and footer == await self.config.guild(guild).last_footer():
            footer = random.choice(footers)
        await self.config.guild(guild).last_footer.set(footer)

        author_url = None
        sources = await self.config.guild(guild).sources()
        if sources:
            s0 = sources[0]
            author_url = f"https://github.com/{s0.get('owner','')}/{s0.get('repo','')}"

        description = f"There were **{num_authors}** active changelogs."

        if num_authors == 0:
            empty_embed = self._new_base_embed(ctx, guild, embedTitle, description, eColor, footer, author_url)
            return await channel.send("", embed=empty_embed, allowed_mentions=discord.AllowedMentions(everyone=True, users=True, roles=True, replied_user=True))

        # Build fields and paginate across multiple embeds if necessary
        fields = self._build_fields_from_aggregated(aggregated_by_author)
        pages = self._paginate_embeds(ctx, guild, embedTitle, description, eColor, footer, author_url, fields)

        # Send pages one-by-one, mentioning role only on the first message
        for idx, page in enumerate(pages):
            await channel.send(role.mention if (role and idx == 0) else "", embed=page, allowed_mentions=discord.AllowedMentions(everyone=True, users=True, roles=True, replied_user=True))

    async def _update_cache_range(self, ctx: commands.Context, start_date: date, end_date: date) -> Tuple[int, List[str]]:
        # Returns (num_days_updated, months_processed)
        months = self._month_range(start_date, end_date)
        cached = await self.config.guild(ctx.guild).cached()
        if not isinstance(cached, dict):
            cached = {}
        touched_days = 0
        for ym in months:
            remote_month = await self._fetch_month_from_sources(ctx, ym)
            month_bucket = cached.setdefault(ym, {})
            # Only merge days within the requested range
            for day, authors in remote_month.items():
                try:
                    d = datetime.strptime(day, "%Y-%m-%d").date()
                except Exception:
                    continue
                if d < start_date or d > end_date:
                    continue
                day_bucket = month_bucket.setdefault(day, {})
                for author, tags in authors.items():
                    author_bucket = day_bucket.setdefault(author, {})
                    self._merge_author_changes(author_bucket, tags)
                touched_days += 1
        await self.config.guild(ctx.guild).cached.set(cached)
        return touched_days, months

    @commands.guild_only()
    @commands.group(invoke_without_command=True, aliases=["scl"])
    async def schangelog(self, ctx, ping: Optional[bool] = False, *, target_day: Optional[str] = None):
        """
        SS13 changelogs (web vs local diff across window)

        - ping: mention the configured role if true
        - target_day: reference day in YYYY-mm-dd or 'today' (defaults to today)
        """
        if ctx.invoked_subcommand is None:
            if not target_day:
                target_day = date.today().strftime("%Y-%m-%d")
            await self._send_diff_embed_for_window(ctx, ping_enabled=bool(ping), ref_day=target_day)

    @schangelog.group(invoke_without_command=True)
    @checks.admin_or_permissions(administrator=True)
    async def set(self, ctx: commands.Context):
        """
        Changelog Configuration
        """
        if ctx.invoked_subcommand is None:
            guild = ctx.guild
            assert guild is not None
            sources = await self.config.guild(guild).sources()
            eColor = await self.config.guild(guild).embed_color()
            role_id = await self.config.guild(guild).mentionrole()
            role = discord.utils.get(guild.roles, id=role_id)

            lines = [
                "Current config:",
                f"  - sources: {len(sources)}",
            ]
            for idx, s in enumerate(sources, start=1):
                lines.append(f"    {idx}. {s.get('owner','')}/{s.get('repo','')}@{s.get('branch','main')} — {s.get('path','')}")
            lines.extend([
                f"  - color: {discord.Colour.from_rgb(*eColor)}",
                f"  - role: {role}",
            ])

            await ctx.send(chat_formatting.box("\n".join(lines), "yaml"))

    @set.group(name="sources", invoke_without_command=True)
    @checks.admin_or_permissions(administrator=True)
    async def set_sources(self, ctx: commands.Context):
        """
        Manage GitHub sources (folders containing monthly %Y-%m.yml files)
        """
        if ctx.invoked_subcommand is None:
            sources = await self.config.guild(ctx.guild).sources()
            if not sources:
                await ctx.send("No sources configured yet. Use `schangelog set sources add`.")
                return
            lines = []
            for idx, s in enumerate(sources, start=1):
                lines.append(f"{idx}. {s.get('owner','')}/{s.get('repo','')}@{s.get('branch','main')} — {s.get('path','')}")
            await ctx.send(chat_formatting.box("\n".join(lines)))

    @set_sources.command(name="add")
    @checks.admin_or_permissions(administrator=True)
    async def set_sources_add(self, ctx: commands.Context, owner: str, repo: str, path: str, branch: Optional[str] = "main"):
        """
        Add a source folder.

        Example: schangelog set sources add owner repo path/to/changelogs main
        """
        sources = await self.config.guild(ctx.guild).sources()
        sources.append({"owner": owner, "repo": repo, "branch": branch or "main", "path": path})
        await self.config.guild(ctx.guild).sources.set(sources)
        await ctx.tick()

    @set.command(name="cachedays")
    @checks.admin_or_permissions(administrator=True)
    async def set_cache_days(self, ctx: commands.Context, days: Optional[int] = None):
        """
        Set how many days prior to the reference day are considered for updates/diffs.

        Example: schangelog set cachedays 10
        """
        if days is None:
            cur = await self.config.guild(ctx.guild).cache_days()
            await ctx.send(f"Current cache_days: {cur}")
            return
        if days < 0 or days > 120:
            await ctx.send("Please provide a value between 0 and 120.")
            return
        await self.config.guild(ctx.guild).cache_days.set(int(days))
        await ctx.tick()

    @set.group(invoke_without_command=True)
    async def footers(self, ctx: commands.Context):
        """
        Command to edit and manage footers of the changelogs
        """
        if ctx.invoked_subcommand is None:
            footers = await self.config.guild(ctx.guild).footer_lines()
            message = ""
            for i in range(len(footers)):
                message += f"{i+1}. {footers[i]}\n"
            await ctx.send(chat_formatting.box(message.strip()))

    @footers.command(name="add")
    async def add_footer(self, ctx: commands.Context, *, newF: str):
        """
        Add a footer to the list of footers that can appear in the changelogs
        """
        current = await self.config.guild(ctx.guild).footer_lines()
        current.append(newF)
        await self.config.guild(ctx.guild).footer_lines.set(current)
        await ctx.tick()

    @footers.command(name="delete")
    async def remove_footer(self, ctx: commands.Context, *, delF: int):
        """
        Remove a footer from the footer list
        """
        toDelete = delF - 1
        current = await self.config.guild(ctx.guild).footer_lines()
        if (len(current) <= 1):
            return await ctx.send("There must be at least 1 active footer.")
        try:
            current.pop(toDelete)
        except IndexError:
            await ctx.send("Footer not found.")
            return
        await self.config.guild(ctx.guild).footer_lines.set(current)
        await ctx.tick()

    @set_sources.command(name="remove")
    @checks.admin_or_permissions(administrator=True)
    async def set_sources_remove(self, ctx: commands.Context, index: int):
        """
        Remove a source by its index (from the list command)
        """
        sources = await self.config.guild(ctx.guild).sources()
        idx = index - 1
        try:
            sources.pop(idx)
        except Exception:
            await ctx.send("Invalid index")
            return
        await self.config.guild(ctx.guild).sources.set(sources)
        await ctx.tick()

    @set_sources.command(name="clear")
    @checks.admin_or_permissions(administrator=True)
    async def set_sources_clear(self, ctx: commands.Context):
        await self.config.guild(ctx.guild).sources.set([])
        await ctx.tick()

    @set.command(name="color")
    async def set_color(self, ctx: commands.Context, *, newColor: Optional[discord.Colour]):
        """
        Change the color of the changelog embeds
        """

        if not newColor:
            await self.config.guild(ctx.guild).embed_color.clear()
            await ctx.send("`color` has been reset to its default value")
            return

        await self.config.guild(ctx.guild).embed_color.set(newColor.to_rgb())
        await ctx.tick()

    @set.command(name="role")
    async def set_mrole(self, ctx: commands.Context, *, newRole: Optional[discord.Role]):
        """
        Change the role that will be pinged when using the channel command.

        Defaults to none
        """
        if not newRole:
            await self.config.guild(ctx.guild).mentionrole.clear()
            await ctx.send("`role` has been reset to its default value")
            return

        await self.config.guild(ctx.guild).mentionrole.set(newRole.id)
        await ctx.tick()

    @set.command(name="reset")
    async def reset_config(self, ctx: commands.Context):
        """
        Reset all the data for the current guild

        This will clear everything, be careful!
        """
        await self.config.guild(ctx.guild).clear()
        await ctx.tick()

    # ==========================
    # Cache and update commands (under `set`)
    # ==========================

    @set.command(name="update")
    @checks.admin_or_permissions(administrator=True)
    async def schangelog_update(self, ctx: commands.Context, refday: Optional[str] = None):
        """
        Update local cache to be exactly the window from reference day back to configured cache_days.

        refday: YYYY-mm-dd or 'today' (defaults to today)
        """
        today = date.today()
        if not refday or refday.strip().lower() == "today":
            reference_day = today
        else:
            try:
                reference_day = datetime.strptime(refday, "%Y-%m-%d").date()
            except Exception:
                await ctx.send("Invalid reference day")
                return

        cache_days = await self.config.guild(ctx.guild).cache_days()
        start_date, end_date = self._compute_window_for_ref_day(reference_day, int(cache_days))

        updated_days, months = await self._build_cache_for_range(ctx, start_date, end_date)
        await ctx.send(f"Cache rebuilt for window {start_date} to {end_date}. Days cached: {updated_days} across {len(months)} month(s)")

    @set.command(name="clearcache")
    @checks.admin_or_permissions(administrator=True)
    async def schangelog_clearcache(self, ctx: commands.Context):
        """
        Clears the stored local cache.
        """
        await self.config.guild(ctx.guild).cached.set({})
        await ctx.tick()

    # ==========================
    # Menu (interactive) command
    # ==========================

    async def _fetch_day_aggregated(self, ctx: commands.Context, daydate: date) -> Dict[str, Dict[str, List[str]]]:
        start_date, end_date = daydate, daydate
        window = await self._fetch_window_from_sources(ctx, start_date, end_date)
        # window keys are day strings; if none, return {}
        data = window.get(daydate.strftime("%Y-%m-%d"), {})
        # already aggregated by author
        return data

    async def _build_day_menu_embed(self, ctx: commands.Context, daydate: date, data_by_author: Dict[str, Dict[str, List[str]]]) -> discord.Embed:
        guild = ctx.guild
        if guild is None:
            # Fallback: minimal embed
            return discord.Embed(title="Changelogs Menu", description="No guild context.")
        assert guild is not None
        guildpic = guild.icon
        eColor = await self.config.guild(guild).embed_color()
        footers = await self.config.guild(guild).footer_lines()
        footer = random.choice(footers)
        while (len(footers) > 1) and footer == await self.config.guild(guild).last_footer():
            footer = random.choice(footers)
        await self.config.guild(guild).last_footer.set(footer)

        num_authors = len(data_by_author)
        description = f"{daydate.strftime('%Y-%m-%d')} — There were **{num_authors}** active changelogs."

        embed = discord.Embed(title="Changelogs Menu", description=description, color=discord.Colour.from_rgb(*eColor), timestamp=discord.utils.utcnow())
        # Link author to the first configured repo, if available
        author_url = None
        sources = await self.config.guild(guild).sources()
        if sources:
            s0 = sources[0]
            author_url = f"https://github.com/{s0.get('owner','')}/{s0.get('repo','')}"
        embed.set_author(name=f"{guild.name}'s Changelogs", url=author_url, icon_url=guildpic)
        embed.set_footer(text=footer, icon_url=ctx.me.avatar)
        embed.set_thumbnail(url=guildpic)

        if num_authors == 0:
            embed.add_field(name="No entries", value=chat_formatting.box("No changelogs found for this day.", "yaml"), inline=False)
            return embed

        # Add fields while respecting embed total size. If it would overflow, truncate with a summary field.
        remaining_fields = self._build_fields_from_aggregated(data_by_author)
        added = 0
        for name, value in remaining_fields:
            if added >= 25 or (len(embed) + len(str(name)) + len(str(value)) > 5900):
                hidden = len(remaining_fields) - added
                if hidden > 0:
                    embed.add_field(name="More…", value=f"{hidden} additional field(s) not shown due to embed limits.", inline=False)
                break
            embed.add_field(name=name, value=value, inline=False)
            added += 1
        return embed

    @schangelog.command(name="menu")
    async def schangelog_menu(self, ctx: commands.Context, day: Optional[str] = None):
        """
        Open an interactive menu to browse daily changelogs aggregated from sources.

        day: YYYY-mm-dd or 'today' (defaults to today)
        """
        ref = (day or "").strip().lower()
        if not ref or ref == "today":
            current_day = date.today()
        else:
            try:
                assert day is not None
                current_day = datetime.strptime(day, "%Y-%m-%d").date()
            except Exception:
                await ctx.send("That's not a valid date, dummy")
                return

        data_by_author = await self._fetch_day_aggregated(ctx, current_day)
        embed = await self._build_day_menu_embed(ctx, current_day, data_by_author)

        view = ChangelogMenuView(cog=self, ctx=ctx, start_day=current_day)
        msg = await ctx.send(embed=embed, view=view)
        view.message = msg
