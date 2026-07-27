"""
Database manager for CkeyTools cog using SQLAlchemy.
"""
import logging
from datetime import date
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from urllib.parse import quote_plus

from sqlalchemy import select, update, and_, or_
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.exc import IntegrityError

from .models import DiscordLink, AgeVet, Base, AgeVetBase, create_dynamic_model, get_table_name_with_prefix

log = logging.getLogger("red.ckeytools.database")


class DatabaseManager:
    """
    Manages database connections and operations for CkeyTools using SQLAlchemy.
    """

    def __init__(self):
        self.engines: Dict[int, Any] = {}  # Guild ID -> AsyncEngine
        self.session_makers: Dict[int, Any] = {}  # Guild ID -> AsyncSessionMaker
        self.models: Dict[int, Any] = {}  # Guild ID -> DiscordLink model class

    async def connect_guild(self, guild_id: int, host: str, port: int, user: str,
                          password: str, database: str, prefix: str = ""):
        """
        Create a database connection for a specific guild.

        Args:
            guild_id: Discord guild ID
            host: Database host
            port: Database port
            user: Database username
            password: Database password
            database: Database name
            prefix: Table prefix
        """
        try:
            # Close existing connection if it exists
            await self.disconnect_guild(guild_id)

            # Create connection URL
            url = f"mysql+aiomysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"

            # Create async engine
            engine = create_async_engine(
                url,
                echo=False,  # Set to True for SQL debugging
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600
            )

            # Create session maker
            session_maker = async_sessionmaker(
                bind=engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            # Create dynamic model with prefix
            model_class = create_dynamic_model(prefix)

            # Store connections
            self.engines[guild_id] = engine
            self.session_makers[guild_id] = session_maker
            self.models[guild_id] = model_class

            # Create tables if they don't exist
            async with engine.begin() as conn:
                # Update the model's metadata with the correct table name
                model_class.__table__.name = get_table_name_with_prefix(prefix)
                await conn.run_sync(Base.metadata.create_all, checkfirst=True)

            log.info(f"Connected to database for guild {guild_id} with prefix '{prefix}'")
            return True

        except Exception as e:
            log.error(f"Failed to connect to database for guild {guild_id}: {e}")
            # Clean up partial connections
            await self.disconnect_guild(guild_id)
            return False

    async def disconnect_guild(self, guild_id: int):
        """
        Disconnect the database for a specific guild.

        Args:
            guild_id: Discord guild ID
        """
        try:
            if guild_id in self.engines:
                engine = self.engines[guild_id]
                if engine:
                    await engine.dispose()
                del self.engines[guild_id]

            if guild_id in self.session_makers:
                del self.session_makers[guild_id]

            if guild_id in self.models:
                del self.models[guild_id]

            log.info(f"Disconnected database for guild {guild_id}")

        except Exception as e:
            log.error(f"Error disconnecting database for guild {guild_id}: {e}")

    async def disconnect_all(self):
        """Disconnect all guild databases."""
        for guild_id in list(self.engines.keys()):
            await self.disconnect_guild(guild_id)

    def is_connected(self, guild_id: int) -> bool:
        """Check if a guild has an active database connection."""
        return (guild_id in self.engines and
                guild_id in self.session_makers and
                guild_id in self.models)

    @asynccontextmanager
    async def get_session(self, guild_id: int):
        """
        Get a database session for a guild.

        Args:
            guild_id: Discord guild ID

        Yields:
            AsyncSession: Database session

        Raises:
            RuntimeError: If guild is not connected to database
        """
        if not self.is_connected(guild_id):
            raise RuntimeError(f"Guild {guild_id} is not connected to database")

        session_maker = self.session_makers[guild_id]
        async with session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    def get_model(self, guild_id: int):
        """
        Get the DiscordLink model class for a guild.

        Args:
            guild_id: Discord guild ID

        Returns:
            DiscordLink model class

        Raises:
            RuntimeError: If guild is not connected to database
        """
        if not self.is_connected(guild_id):
            raise RuntimeError(f"Guild {guild_id} is not connected to database")

        return self.models[guild_id]

    # Convenience methods for common operations

    async def get_valid_link_by_discord_id(self, guild_id: int, discord_id: int) -> Optional[DiscordLink]:
        """Get the latest valid discord link for a user."""
        log.info(f"Fetching valid link for discord_id {discord_id} in guild {guild_id}")

        model = self.get_model(guild_id)
        async with self.get_session(guild_id) as session:
            result = await session.execute(
                select(model)
                .where(and_(model.discord_id == discord_id, model.valid == True))
                .order_by(model.timestamp.desc())
                .limit(1)
            )
            link = result.scalar_one_or_none()

            if link:
                log.info(f"Found valid link for discord_id {discord_id}: {link}")
            else:
                log.info(f"No valid link found for discord_id {discord_id}")

            return link

    async def get_latest_link_by_discord_id(self, guild_id: int, discord_id: int) -> Optional[DiscordLink]:
        """Get the latest discord link (valid or invalid) for a user."""
        log.info(f"Fetching latest link for discord_id {discord_id} in guild {guild_id}")

        model = self.get_model(guild_id)
        async with self.get_session(guild_id) as session:
            result = await session.execute(
                select(model)
                .where(model.discord_id == discord_id)
                .order_by(model.timestamp.desc())
                .limit(1)
            )
            return result.scalar_one_or_none()

    async def get_all_links_by_discord_id(self, guild_id: int, discord_id: int) -> List[DiscordLink]:
        """Get all discord links for a user."""
        model = self.get_model(guild_id)
        async with self.get_session(guild_id) as session:
            result = await session.execute(
                select(model)
                .where(model.discord_id == discord_id)
                .order_by(model.timestamp.desc())
            )
            return result.scalars().all()

    async def get_all_links_by_ckey(self, guild_id: int, ckey: str) -> List[DiscordLink]:
        """Get all discord links for a ckey."""
        model = self.get_model(guild_id)
        async with self.get_session(guild_id) as session:
            result = await session.execute(
                select(model)
                .where(model.ckey == ckey)
                .order_by(model.timestamp.desc())
            )
            return result.scalars().all()

    async def create_link(self, guild_id: int, ckey: str, discord_id: int,
                         one_time_token: str, valid: bool = False) -> DiscordLink:
        """Create a new discord link."""
        log.info(f"Creating link for ckey '{ckey}' and discord_id {discord_id} in guild {guild_id}")

        model = self.get_model(guild_id)
        async with self.get_session(guild_id) as session:
            link = model(
                ckey=ckey,
                discord_id=discord_id,
                one_time_token=one_time_token,
                valid=valid
                # timestamp and id are handled automatically by the database
            )
            session.add(link)
            await session.flush()  # Get the ID
            await session.refresh(link)

            log.info(f"Created link with ID {link.id}")
            return link

    async def invalidate_links_by_discord_id(self, guild_id: int, discord_id: int) -> int:
        """Invalidate all valid links for a discord user."""
        log.info(f"Invalidating links for discord_id {discord_id} in guild {guild_id}")

        model = self.get_model(guild_id)
        async with self.get_session(guild_id) as session:
            result = await session.execute(
                update(model)
                .where(and_(model.discord_id == discord_id, model.valid == True))
                .values(valid=False)
            )
            count = result.rowcount

            log.info(f"Invalidated {count} links for discord_id {discord_id}")
            return count

    async def invalidate_links_by_ckey(self, guild_id: int, ckey: str) -> int:
        """Invalidate all valid links for a ckey."""
        log.info(f"Invalidating links for ckey '{ckey}' in guild {guild_id}")

        model = self.get_model(guild_id)
        async with self.get_session(guild_id) as session:
            result = await session.execute(
                update(model)
                .where(and_(model.ckey == ckey, model.valid == True))
                .values(valid=False)
            )
            count = result.rowcount

            log.info(f"Invalidated {count} links for ckey '{ckey}'")
            return count

    async def invalidate_previous_links(self, guild_id: int, ckey: str, discord_id: int) -> int:
        """Invalidate all previous valid links for both the ckey and discord_id before creating a new verified link."""
        log.info(f"Invalidating previous links for ckey '{ckey}' and discord_id {discord_id} in guild {guild_id}")

        model = self.get_model(guild_id)
        async with self.get_session(guild_id) as session:
            # Invalidate all valid links for this ckey OR this discord_id
            result = await session.execute(
                update(model)
                .where(and_(
                    or_(model.ckey == ckey, model.discord_id == discord_id),
                    model.valid == True
                ))
                .values(valid=False)
            )
            count = result.rowcount

            log.info(f"Invalidated {count} previous links for ckey '{ckey}' and discord_id {discord_id}")
            return count

    async def verify_code(self, guild_id: int, code: str, discord_id: int) -> Optional[DiscordLink]:
        """Verify a one-time code and mark the link as valid."""
        log.info(f"Verifying code for discord_id {discord_id} in guild {guild_id}")

        model = self.get_model(guild_id)
        async with self.get_session(guild_id) as session:
            # Find the link with the matching code
            result = await session.execute(
                select(model)
                .where(and_(
                    model.one_time_token == code,
                    model.discord_id.is_(None)  # Unlinked token
                ))
                .limit(1)
            )
            link = result.scalar_one_or_none()

            if link:
                # First, invalidate all previous valid links for this ckey and discord_id
                await session.execute(
                    update(model)
                    .where(and_(
                        or_(model.ckey == link.ckey, model.discord_id == discord_id),
                        model.valid == True
                    ))
                    .values(valid=False)
                )

                # Then update the link to be valid and set discord_id
                link.valid = True
                link.discord_id = discord_id
                await session.flush()
                await session.refresh(link)

                log.info(f"Verified code for ckey '{link.ckey}' and discord_id {discord_id}")
                return link
            else:
                log.info(f"No matching code found for discord_id {discord_id}")
                return None

    async def get_all_valid_links(self, guild_id: int) -> List[DiscordLink]:
        """Get all valid discord links."""
        model = self.get_model(guild_id)
        async with self.get_session(guild_id) as session:
            result = await session.execute(
                select(model)
                .where(and_(model.discord_id.isnot(None), model.valid == True))
                .order_by(model.timestamp.desc())
            )
            return result.scalars().all()


class AgeVetDatabaseManager:
    """
    Manages MariaDB/MySQL connections for BackgroundCheck AgeVet data.

    Uses the same host/port/user/password as the main cog DB, but a separate
    database name. Table schema matches Django's ``bc_vetting_agevet``.
    """

    def __init__(self):
        self.engines: Dict[int, Any] = {}
        self.session_makers: Dict[int, Any] = {}
        self.databases: Dict[int, str] = {}

    async def connect_guild(
        self,
        guild_id: int,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ) -> bool:
        """
        Connect a guild to the AgeVet MariaDB/MySQL database.

        Args:
            guild_id: Discord guild ID
            host: Database host (shared with main cog DB)
            port: Database port
            user: Database username
            password: Database password
            database: AgeVet database name (separate from SS13 discord_links DB)
        """
        try:
            await self.disconnect_guild(guild_id)

            url = (
                f"mysql+aiomysql://{quote_plus(user)}:{quote_plus(password)}"
                f"@{host}:{port}/{database}?charset=utf8mb4"
            )

            engine = create_async_engine(
                url,
                echo=False,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
            session_maker = async_sessionmaker(
                bind=engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            # Ensure the AgeVet table exists (works standalone or alongside Django migrations).
            async with engine.begin() as conn:
                await conn.run_sync(AgeVetBase.metadata.create_all, checkfirst=True)

            self.engines[guild_id] = engine
            self.session_makers[guild_id] = session_maker
            self.databases[guild_id] = database

            log.info(f"Connected to AgeVet database '{database}' for guild {guild_id}")
            return True

        except Exception as e:
            log.error(f"Failed to connect AgeVet database for guild {guild_id}: {e}")
            await self.disconnect_guild(guild_id)
            return False

    async def disconnect_guild(self, guild_id: int):
        """Disconnect the AgeVet database for a guild."""
        try:
            if guild_id in self.engines:
                engine = self.engines[guild_id]
                if engine:
                    await engine.dispose()
                del self.engines[guild_id]

            self.session_makers.pop(guild_id, None)
            self.databases.pop(guild_id, None)

            log.info(f"Disconnected AgeVet database for guild {guild_id}")
        except Exception as e:
            log.error(f"Error disconnecting AgeVet database for guild {guild_id}: {e}")

    async def disconnect_all(self):
        """Disconnect all AgeVet databases."""
        for guild_id in list(self.engines.keys()):
            await self.disconnect_guild(guild_id)

    def is_connected(self, guild_id: int) -> bool:
        """Check if a guild has an active AgeVet database connection."""
        return guild_id in self.engines and guild_id in self.session_makers

    @asynccontextmanager
    async def get_session(self, guild_id: int):
        """Yield an AsyncSession for the guild's AgeVet database."""
        if not self.is_connected(guild_id):
            raise RuntimeError(f"Guild {guild_id} is not connected to AgeVet database")

        session_maker = self.session_makers[guild_id]
        async with session_maker() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def get_record(self, guild_id: int, ckey: str) -> Optional[Dict[str, Any]]:
        """Get an AgeVet record by ckey, or None if not found."""
        async with self.get_session(guild_id) as session:
            result = await session.execute(
                select(AgeVet).where(AgeVet.ckey == ckey).limit(1)
            )
            record = result.scalar_one_or_none()
            return record.to_dict() if record else None

    async def get_all_records(self, guild_id: int) -> List[Dict[str, Any]]:
        """Get all AgeVet records."""
        async with self.get_session(guild_id) as session:
            result = await session.execute(select(AgeVet).order_by(AgeVet.created_at.desc()))
            return [record.to_dict() for record in result.scalars().all()]

    async def create_record(self, guild_id: int, ckey: str, date_of_birth: date) -> Dict[str, Any]:
        """Create a new AgeVet record."""
        async with self.get_session(guild_id) as session:
            existing = await session.get(AgeVet, ckey)
            if existing:
                raise ValueError(f"AgeVet record for ckey '{ckey}' already exists")

            record = AgeVet(ckey=ckey, date_of_birth=date_of_birth)
            session.add(record)
            try:
                await session.flush()
                await session.refresh(record)
            except IntegrityError as e:
                raise ValueError(f"AgeVet record for ckey '{ckey}' already exists") from e

            log.info(f"Created AgeVet record for ckey '{ckey}'")
            return record.to_dict()

    async def update_record(self, guild_id: int, ckey: str, date_of_birth: date) -> Dict[str, Any]:
        """Update an existing AgeVet record's date of birth."""
        async with self.get_session(guild_id) as session:
            record = await session.get(AgeVet, ckey)
            if not record:
                raise ValueError(f"AgeVet record for ckey '{ckey}' not found")

            record.date_of_birth = date_of_birth
            await session.flush()
            await session.refresh(record)

            log.info(f"Updated AgeVet record for ckey '{ckey}'")
            return record.to_dict()

    async def delete_record(self, guild_id: int, ckey: str) -> bool:
        """Delete an AgeVet record. Returns True if deleted."""
        async with self.get_session(guild_id) as session:
            record = await session.get(AgeVet, ckey)
            if not record:
                raise ValueError(f"AgeVet record for ckey '{ckey}' not found")

            await session.delete(record)
            log.info(f"Deleted AgeVet record for ckey '{ckey}'")
            return True
