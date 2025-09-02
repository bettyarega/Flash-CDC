# app/routers/listeners.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel.ext.asyncio.session import AsyncSession

from app.db import get_session
from app.services.listener_manager import manager

router = APIRouter(prefix="/listeners", tags=["listeners"])


# ---------- NEW: list & single status the UI expects ----------

@router.get("")
async def list_listeners():
    """
    Frontend polls this path. Always return {"items": [...]}
    """
    res = await manager.status_all()
    # Normalize to {"items": [...]}
    if isinstance(res, dict) and "items" in res:
        return res
    if isinstance(res, list):
        return {"items": res}
    # last resort: wrap whatever came back
    return {"items": res.get("items", [])}  # type: ignore[union-attr]


@router.get("/{client_id}")
async def get_listener(client_id: int):
    """
    Optional helper: fetch a single listener's status at /listeners/{id}.
    """
    return await manager.status(client_id)


# ---------- Your existing controls (kept) ----------

@router.post("/{client_id}/start")
async def start_listener(client_id: int, db: AsyncSession = Depends(get_session)):
    res = await manager.start(db, client_id)
    if isinstance(res, dict) and "error" in res:
        raise HTTPException(status_code=404, detail=res["error"])
    return res

@router.post("/{client_id}/stop")
async def stop_listener(client_id: int):
    return await manager.stop(client_id)

@router.post("/{client_id}/restart")
async def restart_listener(client_id: int, db: AsyncSession = Depends(get_session)):
    res = await manager.restart(db, client_id)
    if isinstance(res, dict) and "error" in res:
        raise HTTPException(status_code=404, detail=res["error"])
    return res


# ---------- Your status/diag utilities (kept) ----------

@router.get("/{client_id}/status")
async def status_listener(client_id: int):
    return await manager.status(client_id)

@router.get("/status")
async def status_all():
    return await manager.status_all()

@router.post("/start-active")
async def start_active(db: AsyncSession = Depends(get_session)):
    n = await manager.autostart_active(db)
    return {"started": n}

@router.get("/{client_id}/diag")
async def diag_listener(client_id: int):
    st = await manager.status(client_id)
    # If your manager.status returns a dict with "status"/"running", keep this check:
    if not st.get("running") and st.get("status") not in ("running", "starting"):  # type: ignore[union-attr]
        raise HTTPException(status_code=400, detail="listener not running")
    return st
