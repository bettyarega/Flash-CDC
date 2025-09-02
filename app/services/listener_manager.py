# app/services/listener_manager.py
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, Optional

from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import async_session_factory, DB_SCHEMA
from ..models import Client
from .sf_pubsub import run_salesforce_pubsub  # worker entrypoint

log = logging.getLogger("listener-manager")


@dataclass
class ListenerState:
    client_id: int
    status: str
    started_at: Optional[datetime] = None
    last_beat: Optional[datetime] = None
    last_error: Optional[str] = None
    fail_count: int = 0


class Listener:
    def __init__(self, client_id: int):
        self.client_id = client_id
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self.state = ListenerState(client_id=client_id, status="stopped")

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        if self.is_running():
            return
        self._stop_event = asyncio.Event()
        self.state = ListenerState(
            client_id=self.client_id,
            status="starting",
            started_at=datetime.now(timezone.utc),
        )
        self._task = asyncio.create_task(self._runner(), name=f"listener-{self.client_id}")
        log.info("[manager] start: client %s task created", self.client_id)

    async def stop(self, timeout: float = 10.0) -> None:
        if not self._task:
            self.state.status = "stopped"
            return
        if self._task.done():
            self.state.status = "stopped"
            return
        self.state.status = "stopping"
        self._stop_event.set()
        log.info("[manager] stop: cancelling client %s …", self.client_id)
        try:
            await asyncio.wait_for(self._task, timeout=timeout)
        except asyncio.TimeoutError:
            self._task.cancel()
        finally:
            self.state.status = "stopped"
            log.info("[manager] runner: client %s stopped", self.client_id)

    async def status(self) -> dict:
        # reflect task errors
        if self._task and self._task.done() and self.state.status not in ("stopped", "stopping"):
            exc = self._task.exception()
            if exc:
                self.state.status = "error"
                self.state.last_error = str(exc)
        # convenience: include a boolean
        d = asdict(self.state)
        d["running"] = self.is_running() or self.state.status in ("starting", "running")
        return d

    async def _runner(self):
        self.state.status = "starting"
        backoff = 1
        max_backoff = 60

        while not self._stop_event.is_set():
            try:
                client = await self._load_client()
                if not client:
                    raise RuntimeError(f"Client {self.client_id} not found")
                if not client.is_active:
                    raise RuntimeError(f"Client {self.client_id} is not active")

                # Reset error/fail state
                self.state.last_error = None
                self.state.fail_count = 0
                backoff = 1

                # heartbeat hook to mark last_beat
                def _log(level: int, msg: str):
                    self.state.last_beat = datetime.now(timezone.utc)
                    logging.getLogger("sf-listener").log(level, f"[client:{client.id}:{client.client_name}] {msg}")

                self.state.status = "running"
                await run_salesforce_pubsub(client, self._stop_event, _log)
                break  # graceful stop

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.state.status = "error"
                self.state.last_error = str(e)
                self.state.fail_count += 1
                delay = min(backoff, max_backoff)
                backoff = min(backoff * 2, max_backoff)
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    continue

        self.state.status = "stopped"

    async def _load_client(self) -> Optional[Client]:
        # Use a fresh session to read latest config before (re)starting
        async with async_session_factory() as session:
            await session.execute(text(f"SET search_path TO {DB_SCHEMA}, public"))
            result = await session.execute(select(Client).where(Client.id == self.client_id))
            return result.scalar_one_or_none()


class ListenerManager:
    def __init__(self):
        self._listeners: Dict[int, Listener] = {}
        self._lock = asyncio.Lock()

    async def start(self, db: AsyncSession, client_id: int) -> dict:
        # db kept for signature symmetry; not used here (Listener loads fresh)
        async with self._lock:
            listener = self._listeners.get(client_id)
            if not listener:
                listener = Listener(client_id=client_id)
                self._listeners[client_id] = listener
            if not listener.is_running():
                listener.start()
            return await listener.status()

    async def stop(self, client_id: int) -> dict:
        async with self._lock:
            listener = self._listeners.get(client_id)
            if not listener:
                return {"client_id": client_id, "status": "stopped", "running": False}
        await listener.stop()
        return await listener.status()

    async def restart(self, db: AsyncSession, client_id: int) -> dict:
        # stop (if any), then start again
        await self.stop(client_id)
        return await self.start(db, client_id)

    async def status(self, client_id: int) -> dict:
        async with self._lock:
            listener = self._listeners.get(client_id)
            if not listener:
                # not tracked -> report stopped
                return {"client_id": client_id, "status": "stopped", "running": False}
            return await listener.status()

    async def status_all(self) -> dict:
        # NEW: what /listeners expects
        async with self._lock:
            items = []
            for cid, lst in self._listeners.items():
                items.append(await lst.status())
        return {"items": items}

    # Keep backward-compat with earlier code that called manager.list()
    async def list(self) -> dict:
        return await self.status_all()

    async def autostart_active(self, db: AsyncSession) -> int:
        """
        Start listeners for all active clients in DB.
        """
        await db.execute(text(f"SET search_path TO {DB_SCHEMA}, public"))
        rows = (await db.execute(select(Client).where(Client.is_active == True))).scalars().all()  # noqa: E712
        n = 0
        for c in rows:
            await self.start(db, c.id)
            n += 1
        return n


manager = ListenerManager()
