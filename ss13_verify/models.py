"""
SQLAlchemy models for SS13Verify cog.
"""
from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class DiscordLink(Base):
    """
    Discord link model representing the discord_links table.

    This model represents the relationship between a Discord user and their SS13 ckey,
    including verification tokens and validity status.
    """
    __tablename__ = 'discord_links'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ckey = Column(String(32), nullable=False, index=True)
    discord_id = Column(BigInteger, nullable=True, index=True)
    timestamp = Column(DateTime, nullable=False, default=func.current_timestamp(), index=True)
    one_time_token = Column(String(100), nullable=False, index=True)
    valid = Column(Boolean, nullable=False, default=False, index=True)

    def __repr__(self):
        return f"<DiscordLink(id={self.id}, ckey='{self.ckey}', discord_id={self.discord_id}, valid={self.valid})>"

    def to_dict(self):
        """Convert the model to a dictionary for compatibility with existing code."""
        return {
            'id': self.id,
            'ckey': self.ckey,
            'discord_id': self.discord_id,
            'timestamp': self.timestamp,
            'one_time_token': self.one_time_token,
            'valid': self.valid
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Create a DiscordLink instance from a dictionary."""
        return cls(
            id=data.get('id'),
            ckey=data.get('ckey'),
            discord_id=data.get('discord_id'),
            timestamp=data.get('timestamp'),
            one_time_token=data.get('one_time_token'),
            valid=data.get('valid', False)
        )


def get_table_name_with_prefix(prefix: str) -> str:
    """
    Get the table name with the specified prefix.

    Args:
        prefix: The database table prefix

    Returns:
        The full table name with prefix
    """
    if prefix and not prefix.endswith('_'):
        prefix += '_'
    return f"{prefix}discord_links"


def create_dynamic_model(prefix: str):
    """
    Create a dynamic DiscordLink model with a custom table name based on prefix.

    Args:
        prefix: The database table prefix

    Returns:
        A DiscordLink class with the correct table name
    """
    table_name = get_table_name_with_prefix(prefix)

    class PrefixedDiscordLink(DiscordLink):
        __tablename__ = table_name

        # Override the table name
        __table_args__ = {'extend_existing': True}

    return PrefixedDiscordLink
