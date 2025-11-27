from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select, func
import logging
from pydantic import BaseModel
import os

from app.services.sf_pubsub import OAuthConfig, test_salesforce_connection
from app.services.listener_manager import manager
from ..db import get_session
from ..models import (
    Client,
    ClientCreate,
    ClientUpdate,
    ClientReadSafe,
    ClientReadWithSecrets,
    to_safe,
)
from app.security import require_roles, RoleEnum

DEFAULT_PUBSUB_HOST = os.getenv("SF_PUBSUB_HOST", "api.pubsub.salesforce.com:7443")


router = APIRouter()
log = logging.getLogger("listener-manager")


# --- Helpers ---

async def fetch_client_or_404(session: AsyncSession, client_id: int) -> Client:
    client = await session.get(Client, client_id)
    if not client:
        raise HTTPException(404, "Client not found")
    return client

def maybe_secrets(include_secrets: bool):
    # Only used for OpenAPI typing hints (we build the actual shape at runtime)
    return ClientReadWithSecrets if include_secrets else ClientReadSafe


class TestConnectionPayload(BaseModel):
    # minimal fields needed to auth + optional topic check
    login_url: str
    oauth_grant_type: str
    oauth_client_id: str
    oauth_client_secret: str
    oauth_username: Optional[str] = None
    oauth_password: Optional[str] = None

    # optional extras
    topic_name: Optional[str] = None
    pubsub_host: Optional[str] = None
    tenant_id: Optional[str] = None
    check_topic: bool = True  # if false, skip GetTopic


# --- Routes ---

@router.post("/test-connection", dependencies=[Depends(require_roles(RoleEnum.admin, RoleEnum.user))])
async def test_connection(payload: TestConnectionPayload):
    oauth = OAuthConfig(
        login_url=payload.login_url,
        client_id=payload.oauth_client_id,
        client_secret=payload.oauth_client_secret,
        username=payload.oauth_username,
        password=payload.oauth_password,
        auth_grant_type=payload.oauth_grant_type,
    )

    host = payload.pubsub_host or DEFAULT_PUBSUB_HOST
    res = await test_salesforce_connection(
        oauth,
        topic_name=(payload.topic_name if payload.check_topic else None),
        pubsub_host=host,  
        tenant_id=payload.tenant_id,
    )
    return res


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=ClientReadWithSecrets,  # doc hint (we still control runtime shape)
    dependencies=[Depends(require_roles(RoleEnum.admin, RoleEnum.user))],
)
async def create_client(
    payload: ClientCreate,
    include_secrets: bool = Query(True, description="If true (default), return full row with secrets."),
    session: AsyncSession = Depends(get_session),
):
    # Check for duplicate client_name
    existing_name = (await session.execute(select(Client).where(Client.client_name == payload.client_name))).scalar_one_or_none()
    if existing_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Client with name '{payload.client_name}' already exists. Please use a different name."
        )
    
    # Check for duplicate oauth_client_id + topic_name combination
    existing_combo = (
        await session.execute(
            select(Client).where(
                Client.oauth_client_id == payload.oauth_client_id,
                Client.topic_name == payload.topic_name
            )
        )
    ).scalar_one_or_none()
    if existing_combo:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Client already exists for OAuth Client ID '{payload.oauth_client_id}' and topic '{payload.topic_name}'. "
                   f"Each Salesforce Connected App can only have one listener per topic."
        )
    
    # Create row
    client = Client.model_validate(payload)

    # Force from env/default
    client.pubsub_host = os.getenv("SF_PUBSUB_HOST", DEFAULT_PUBSUB_HOST) 
    session.add(client)
    await session.commit()
    await session.refresh(client)

    # AUTOSTART: if active, start its listener after commit so id exists
    if client.is_active:
        try:
            # pass the DB session into the manager.start signature
            await manager.start(session, client.id)
        except Exception as e:
            # Don’t fail the API response; the DB write succeeded
            log.error("post-create listener start failed for %s: %r", client.id, e)

    return client if include_secrets else to_safe(client)


@router.get(
    "/status",
    response_model=dict,
    dependencies=[Depends(require_roles(RoleEnum.admin, RoleEnum.user))],
)
async def get_clients_status(session: AsyncSession = Depends(get_session)):
    """
    Get read-only status of all clients with their listener status.
    Accessible by both admin and user roles for monitoring purposes.
    """
    # Get all clients (without secrets for security)
    stmt = select(Client).order_by(Client.id)
    results = await session.execute(stmt)
    clients = results.scalars().all()
    
    # Get all listener statuses
    listener_statuses = await manager.status_all()
    listener_map = {}
    if isinstance(listener_statuses, dict) and "items" in listener_statuses:
        for status_item in listener_statuses["items"]:
            listener_map[status_item["client_id"]] = status_item
    elif isinstance(listener_statuses, list):
        for status_item in listener_statuses:
            listener_map[status_item["client_id"]] = status_item
    
    # Note: events_received is only available from SFListener instance, which is not directly accessible
    # from ListenerManager. It will default to 0 if not available.
    
    # Combine client info with listener status
    status_items = []
    for client in clients:
        listener_status = listener_map.get(client.id, {
            "client_id": client.id,
            "status": "stopped",
            "running": False,
        })
        
        status_items.append({
            "id": client.id,
            "client_name": client.client_name,
            "topic_name": client.topic_name,
            "is_active": client.is_active,
            "webhook_url": client.webhook_url,
            "listener_status": listener_status.get("status", "stopped"),
            "listener_running": listener_status.get("running", False),
            "last_error": listener_status.get("last_error"),
            "started_at": listener_status.get("started_at"),
            "last_beat": listener_status.get("last_beat"),
            "events_received": listener_status.get("events_received", 0),
            "fail_count": listener_status.get("fail_count", 0),
        })
    
    return {"items": status_items}


@router.get(
    "/",
    response_model=dict,  # {"items":[...], "total":..., "limit":..., "offset":...}
    dependencies=[Depends(require_roles(RoleEnum.admin, RoleEnum.user))],
)
async def list_clients(
    q: Optional[str] = Query(None, description="Filter by client_name (icontains)"),
    is_active: Optional[bool] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    include_secrets: bool = Query(True, description="If true (default), return full rows with secrets."),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(Client)
    count_stmt = select(func.count(Client.id))

    if q:
        like = f"%{q}%"
        stmt = stmt.where(Client.client_name.ilike(like))
        count_stmt = count_stmt.where(Client.client_name.ilike(like))
    if is_active is not None:
        stmt = stmt.where(Client.is_active == is_active)
        count_stmt = count_stmt.where(Client.is_active == is_active)

    stmt = stmt.order_by(Client.id).limit(limit).offset(offset)

    total = (await session.execute(count_stmt)).scalar_one()
    results = (await session.execute(stmt)).scalars().all()

    # Build items with or without secrets
    if include_secrets:
        items = results  # full ORM objects include secret fields
    else:
        items = [to_safe(c) for c in results]

    return {"items": items, "total": total, "limit": limit, "offset": offset}


@router.get(
    "/{client_id}",
    response_model=maybe_secrets(True),  # doc hint only; runtime decides via include_secrets
    dependencies=[Depends(require_roles(RoleEnum.admin, RoleEnum.user))],
)
async def get_client(
    client_id: int,
    include_secrets: bool = Query(True, description="If true (default), return full row with secrets."),
    session: AsyncSession = Depends(get_session),
):
    client = await fetch_client_or_404(session, client_id)
    return client if include_secrets else to_safe(client)


@router.patch(
    "/{client_id}",
    response_model=maybe_secrets(True),  # doc hint only; runtime decides via include_secrets
    dependencies=[Depends(require_roles(RoleEnum.admin, RoleEnum.user))],
)
async def update_client(
    client_id: int,
    payload: ClientUpdate,
    include_secrets: bool = Query(True, description="If true (default), return full row with secrets."),
    session: AsyncSession = Depends(get_session),
):
    client = await fetch_client_or_404(session, client_id)

    # Apply partial updates
    data = payload.model_dump(exclude_unset=True)

    # Never allow pubsub_host updates from API; we control via env
    if "pubsub_host" in data:
        data.pop("pubsub_host")

    # Check for duplicate client_name if it's being updated
    if "client_name" in data:
        existing_name = (
            await session.execute(
                select(Client).where(
                    Client.client_name == data["client_name"],
                    Client.id != client_id  # Exclude current client
                )
            )
        ).scalar_one_or_none()
        if existing_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Client with name '{data['client_name']}' already exists. Please use a different name."
            )
    
    # Check for duplicate oauth_client_id + topic_name combination if either is being updated
    oauth_client_id = data.get("oauth_client_id", client.oauth_client_id)
    topic_name = data.get("topic_name", client.topic_name)
    if "oauth_client_id" in data or "topic_name" in data:
        existing_combo = (
            await session.execute(
                select(Client).where(
                    Client.oauth_client_id == oauth_client_id,
                    Client.topic_name == topic_name,
                    Client.id != client_id  # Exclude current client
                )
            )
        ).scalar_one_or_none()
        if existing_combo:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Client already exists for OAuth Client ID '{oauth_client_id}' and topic '{topic_name}'. "
                       f"Each Salesforce Connected App can only have one listener per topic."
            )

    for field, value in data.items():
        setattr(client, field, value)

    # Force env/default every time, so changes take effect immediately on restart
    client.pubsub_host = os.getenv("SF_PUBSUB_HOST", DEFAULT_PUBSUB_HOST) 

    # Re-validate the whole entity after mutation
    Client.model_validate(client)

    session.add(client)
    await session.commit()
    await session.refresh(client)

    # RESTART/STOP based on is_active
    try:
        if client.is_active:
            # pass DB session to restart signature
            await manager.restart(session, client.id)
        else:
            await manager.stop(client.id)
    except Exception as e:
        # Don’t 500 the request because listener recycle failed; return 200 and log it.
        log.error("post-update listener action failed for %s: %r", client.id, e)

    return client if include_secrets else to_safe(client)


@router.delete(
    "/{client_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_roles(RoleEnum.admin))],
)
async def delete_client(client_id: int, session: AsyncSession = Depends(get_session)):
    client = await fetch_client_or_404(session, client_id)

    # STOP ON DELETE (no-op if not running)
    try:
        await manager.stop(client.id)
    except Exception as e:
        log.warning("stop on delete failed for %s: %r", client.id, e)

    await session.delete(client)
    await session.commit()
    return {"message": "deleted"}
