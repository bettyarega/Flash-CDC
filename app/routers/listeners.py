# app/routers/listeners.py
from __future__ import annotations

import json
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel.ext.asyncio.session import AsyncSession

from app.db import get_session
from app.services.listener_manager import manager

router = APIRouter(prefix="/listeners", tags=["listeners"])


def _parse_replay_args(
    *,
    mode: Optional[str],
    since_minutes: Optional[int],
    replay_id_b64: Optional[str],
    replay_json: Optional[str],
) -> Optional[Dict[str, Any]]:
    """
    Normalize replay options into: {"mode": "...", "since_minutes": int, "replay_id_b64": "..."}
    Returns None if nothing provided (so backend will use its default).
    """
    if replay_json:
        try:
            data = json.loads(replay_json)
            if isinstance(data, dict):
                return {
                    k: v for k, v in data.items()
                    if k in ("mode", "since_minutes", "replay_id_b64") and v is not None
                } or None
        except Exception:
            pass

    if mode or since_minutes is not None or replay_id_b64:
        out: Dict[str, Any] = {}
        if mode:
            out["mode"] = mode
        if since_minutes is not None:
            out["since_minutes"] = since_minutes
        if replay_id_b64:
            out["replay_id_b64"] = replay_id_b64
        return out

    return None


@router.get("")
async def list_listeners():
    res = await manager.status_all()
    if isinstance(res, dict) and "items" in res:
        return res
    if isinstance(res, list):
        return {"items": res}
    return {"items": res.get("items", [])}  # type: ignore[union-attr]


@router.get("/{client_id}")
async def get_listener(client_id: int):
    return await manager.status(client_id)


@router.post("/{client_id}/start")
async def start_listener(
    client_id: int,
    db: AsyncSession = Depends(get_session),
    mode: Optional[str] = Query(None),
    since_minutes: Optional[int] = Query(None, ge=1),
    replay_id_b64: Optional[str] = Query(None),
    replay: Optional[str] = Query(None, description="(legacy) JSON object with replay options"),
):
    replay_opts = _parse_replay_args(
        mode=mode, since_minutes=since_minutes, replay_id_b64=replay_id_b64, replay_json=replay
    )
    res = await manager.start(db, client_id, replay=replay_opts)
    if isinstance(res, dict) and "error" in res:
        raise HTTPException(status_code=404, detail=res["error"])
    return res


@router.post("/{client_id}/stop")
async def stop_listener(client_id: int):
    return await manager.stop(client_id)


@router.post("/{client_id}/restart")
async def restart_listener(
    client_id: int,
    db: AsyncSession = Depends(get_session),
    mode: Optional[str] = Query(None),
    since_minutes: Optional[int] = Query(None, ge=1),
    replay_id_b64: Optional[str] = Query(None),
    replay: Optional[str] = Query(None, description="(legacy) JSON object with replay options"),
):
    replay_opts = _parse_replay_args(
        mode=mode, since_minutes=since_minutes, replay_id_b64=replay_id_b64, replay_json=replay
    )
    res = await manager.restart(db, client_id, replay=replay_opts)
    if isinstance(res, dict) and "error" in res:
        raise HTTPException(status_code=404, detail=res["error"])
    return res


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
    if not st.get("running") and st.get("status") not in ("running", "starting"):  # type: ignore[union-attr]
        raise HTTPException(status_code=400, detail="listener not running")
    return st
