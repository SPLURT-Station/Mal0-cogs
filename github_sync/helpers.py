from typing import Dict, List, Optional, Tuple
import discord


def map_discord_tags_to_github_labels(
    discord_tags: List[discord.ForumTag], applied: List[discord.ForumTag]
) -> List[str]:
    names = {tag.id: tag.name for tag in discord_tags}
    result: List[str] = []
    for t in applied:
        name = names.get(t.id)
        if name:
            result.append(name)
    return result


def map_github_labels_to_discord_tags(
    discord_tags: List[discord.ForumTag], labels: List[str]
) -> List[discord.ForumTag]:
    name_to_tag: Dict[str, discord.ForumTag] = {tag.name: tag for tag in discord_tags}
    return [name_to_tag[label] for label in labels if label in name_to_tag]


def build_discord_message_prefix(
    author_name: str, author_url: Optional[str] = None
) -> str:
    if author_url:
        return f"**[{author_name}]({author_url})**\n\n"
    return f"**{author_name}**\n\n"


