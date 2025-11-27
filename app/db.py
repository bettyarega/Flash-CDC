# app/db.py
import os
from typing import AsyncGenerator
from sqlmodel import SQLModel
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import text

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://flash_app:pass@localhost:5432/flash",
)

DB_SCHEMA = os.getenv("DB_SCHEMA", "flash")
RUN_DDL = os.getenv("RUN_DDL", "0") == "1"  # set RUN_DDL=1 once to create tables

# --- Pool/settings knobs (exported) ---
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))
DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))  # seconds
DB_APP_NAME = os.getenv("DB_APP_NAME", "flash-api")  # appears in pg_stat_activity.application_name

# Async engine with a QueuePool under the hood
# Set search_path at connection level so all queries use the correct schema
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_MAX_OVERFLOW,
    pool_recycle=DB_POOL_RECYCLE,
    pool_pre_ping=True,
    # psycopg(v3): set application_name and search_path using libpq options
    connect_args={
        "options": f"-c application_name={DB_APP_NAME} -c search_path={DB_SCHEMA},public"
    },
)

async_session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    # Ensure models are registered before create_all()
    from . import models  # noqa: F401

    async with engine.begin() as conn:
        # Create schema if missing (safe even if it exists)
        await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}'))
        # Make our schema first on the search_path for *this connection*
        await conn.execute(text(f"SET search_path TO {DB_SCHEMA}, public"))

        if RUN_DDL:
            # Set schema on tables that don't have it explicitly set in __table_args__
            # This is needed because we can't mix schema dict with constraints
            for table in SQLModel.metadata.tables.values():
                if table.schema is None:
                    table.schema = DB_SCHEMA
            # Create all tables known to SQLModel.metadata
            await conn.run_sync(SQLModel.metadata.create_all)

async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        # Ensure every session hits the intended schema first
        await session.execute(text(f"SET search_path TO {DB_SCHEMA}, public"))
        yield session