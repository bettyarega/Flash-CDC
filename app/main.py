from app.logging_conf import setup_logging
setup_logging()

import os
from sqlmodel import select

from fastapi import FastAPI
from sqlalchemy import text
from app.db import (
    init_db,
    async_session_factory,
    DB_SCHEMA,
    engine,               # NEW: to inspect pool + run pg_stat_activity
    DB_POOL_SIZE,         # NEW: to report configured values
    DB_MAX_OVERFLOW,      # NEW: "
    DB_APP_NAME,          # NEW: name used in pg_stat_activity.application_name
)
from app.routers import clients, listeners
from app.routers import auth as auth_router
from app.services.listener_manager import manager
from app.models import User, RoleEnum
from app.security import hash_password
import logging
from sqlalchemy.ext.asyncio import AsyncSession

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.include_router(auth_router.router)
app.include_router(clients.router, prefix="/clients")
app.include_router(listeners.router)

# allow your future frontend to call the API in dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "*"],  # relax in dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def on_startup():
    await init_db()
    async with async_session_factory() as session:
        await seed_admin(session)
        started = await manager.autostart_active(session)
        logging.getLogger("listener-manager").info("auto-started %d active listener(s)", started)

@app.get("/health")
async def health():
    return {"ok": True}

async def seed_admin(session: AsyncSession):
    ADM_EMAIL = os.getenv("ADMIN_EMAIL", "admin@example.com")
    ADM_PW = os.getenv("ADMIN_PASSWORD", "admin123")
    existing = (await session.execute(select(User).limit(1))).scalar_one_or_none()
    if existing:
        return
    user = User(
        email=ADM_EMAIL,
        password_hash=hash_password(ADM_PW),
        role=RoleEnum.admin,
        is_active=True,
    )
    session.add(user)
    await session.commit()

# ---------------------------
# Debug endpoints (pool / db)
# ---------------------------

@app.get("/debug/pool")
async def debug_pool():
    """
    Returns current SQLAlchemy pool internals AND live DB activity counts from pg_stat_activity
    filtered to this app's application_name.
    """
    # Pool internals (per-process)
    pool = engine.sync_engine.pool
    pool_stats = {
        # "size_current": number of connections the pool currently has open (checked-in + checked-out)
        "size_current": pool.size(),
        # "checked_in": connections sitting idle in the pool (your "idle" pool connections)
        "checked_in": pool.checkedin(),
        # "checked_out": connections currently in use by requests/tasks
        "checked_out": pool.checkedout(),
        # "overflow_current": number of extra connections beyond pool_size that are open right now
        "overflow_current": pool.overflow(),
        "status": pool.status(),
        "configured": {
            "pool_size": DB_POOL_SIZE,
            "max_overflow": DB_MAX_OVERFLOW,
            "max_concurrency_per_process": DB_POOL_SIZE + DB_MAX_OVERFLOW,
        },
    }

    # DB activity: how many connections the server sees for this app
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    """
                    SELECT COALESCE(state,'(unknown)') AS state, count(*)::int AS n
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND application_name = :app
                    GROUP BY 1
                    """
                ),
                {"app": DB_APP_NAME},
            )
        ).all()

    by_state = {r.state: r.n for r in rows}
    total = sum(by_state.values())
    # Common states we want to surface explicitly:
    active = by_state.get("active", 0)
    idle = by_state.get("idle", 0)
    idle_in_tx = by_state.get("idle in transaction", 0)

    return {
        "application_name": DB_APP_NAME,
        "pool": pool_stats,
        "db_activity": {
            "by_state": by_state,
            "active": active,
            "idle": idle,
            "idle_in_transaction": idle_in_tx,
            "total": total,
        },
        # "notes": [
        #     "pool_size is NOT a hard cap on DB connections; it's the baseline of persistent connections.",
        #     "Peak concurrency per process can reach pool_size + max_overflow. Multiply by the number of worker processes.",
        # ],
    }

@app.get("/debug/dbsleep")
async def debug_dbsleep(ms: int = 5000):
    """
    Hold a connection busy server-side to simulate load.
    e.g. /debug/dbsleep?ms=10000
    """
    ms = max(1, min(ms, 60000))
    async with engine.connect() as conn:
        await conn.execute(text("SELECT pg_sleep(:sec)"), {"sec": ms / 1000.0})
        # simple extra round trip so the connection doesn't go straight back to idle
        await conn.execute(text("SELECT 1"))
    return {"slept_ms": ms}
