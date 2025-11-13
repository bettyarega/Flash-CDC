from __future__ import annotations

import asyncio
import copy
import json
import logging
import random
import os
import base64
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Callable, TypedDict

import requests
import avro.schema
import avro.io
import grpc

# Prefer proto under app.sfproto, fall back to flat
try:
    from app.sfproto import pubsub_api_pb2 as pb2  # type: ignore
    from app.sfproto import pubsub_api_pb2_grpc as pb2_grpc  # type: ignore
except Exception:
    import pubsub_api_pb2 as pb2  # type: ignore
    import pubsub_api_pb2_grpc as pb2_grpc  # type: ignore

# --- DB helpers for offsets (with graceful fallback) ---
from sqlalchemy import text as sql_text
from app.db import async_session_factory, DB_SCHEMA, RUN_DDL  # type: ignore


class FatalConfigError(Exception):
    """Non-recoverable misconfiguration (bad topic, bad creds, etc)."""
    pass


LOG = logging.getLogger("sf-listener")

def clog(client_id: str, level: int, msg: str):
    LOG.log(level, f"[{client_id}] {msg}")


HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "60"))
IDLE_RESET_SECONDS = int(os.getenv("IDLE_RESET_SECONDS", "300"))
FAIL_FAST_NOT_FOUND = os.getenv("FAIL_FAST_NOT_FOUND", "true").lower() in ("1", "true", "yes")
FAIL_FAST_AUTH = os.getenv("FAIL_FAST_AUTH", "true").lower() in ("1", "true", "yes")

DEFAULT_PUBSUB_HOST = os.getenv("SF_PUBSUB_HOST", "api.pubsub.salesforce.com:7443")

# ---------- Replay configuration ----------

@dataclass
class ReplayArgs:
    """
    mode:
      - 'stored'   : use DB/in-memory stored replay_id if present, else EARLIEST
      - 'latest'   : start from now
      - 'earliest' : start from earliest retained
      - 'custom'   : start from provided replay_id_b64
      - 'since'    : start earliest; locally drop events until commitTimestamp >= now - since_minutes
    """
    mode: str = "stored"
    replay_id_b64: Optional[str] = None
    since_minutes: Optional[int] = None


@dataclass
class OAuthConfig:
    login_url: str
    client_id: str
    client_secret: str
    username: Optional[str] = None
    password: Optional[str] = None
    auth_grant_type: str = "client_credentials"  # or "password"


@dataclass
class ClientConfig:
    client_db_id: int       # DB id for offsets
    client_id: str          # label used for logs
    topic_name: str         # e.g. "/data/OpportunityChangeEvent"
    webhook_url: str
    oauth: OAuthConfig
    pubsub_host: Optional[str] = None
    tenant_id: Optional[str] = None
    flow_batch_size: int = 100


# ---- Offsets storage (DB with graceful in-memory fallback) ----

# in-memory fallback map when table is not available
# key: (client_id, topic) -> (last_replay_b64, last_commit_ms)
_OFFSETS_MEM: Dict[Tuple[int, str], Tuple[Optional[str], Optional[int]]] = {}

async def _ensure_offsets_table():
    """
    Create a table compatible with your SQLModel if missing.
    Matches: id, client_id, topic_name, last_replay_b64, last_commit_ts, updated_at
    """
    if not RUN_DDL:
        return
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.listener_offsets (
        id              BIGSERIAL PRIMARY KEY,
        client_id       INT NOT NULL,
        topic_name      TEXT NOT NULL,
        last_replay_b64 TEXT,
        last_commit_ts  TIMESTAMPTZ,
        updated_at      TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS listener_offsets_client_topic_idx
      ON {DB_SCHEMA}.listener_offsets (client_id, topic_name);
    """
    try:
        async with async_session_factory() as s:
            await s.execute(sql_text(f"SET search_path TO {DB_SCHEMA}, public"))
            for stmt in ddl.strip().split(";"):
                if stmt.strip():
                    await s.execute(sql_text(stmt))
            await s.commit()
            LOG.info("Ensured table %s.listener_offsets exists.", DB_SCHEMA)
    except Exception as e:
        LOG.warning("listener_offsets DDL failed (memory fallback will be used if needed): %r", e)

async def _load_replay_b64(client_db_id: int, topic: str) -> Optional[str]:
    """Read last_replay_b64 from DB; fall back to memory cache."""
    try:
        async with async_session_factory() as s:
            await s.execute(sql_text(f"SET search_path TO {DB_SCHEMA}, public"))
            q = f"""
                SELECT last_replay_b64
                FROM {DB_SCHEMA}.listener_offsets
                WHERE client_id=:cid AND topic_name=:tn
                ORDER BY updated_at DESC
                LIMIT 1
            """
            row = (await s.execute(sql_text(q), {"cid": client_db_id, "tn": topic})).first()
            if row:
                return row[0]
    except Exception as e:
        LOG.debug("load replay fallback due to error: %r", e)
    return _OFFSETS_MEM.get((client_db_id, topic), (None, None))[0]

async def _save_replay_b64(client_db_id: int, topic: str, replay_b64: str, last_commit_ms: Optional[int]) -> None:
    """Upsert last_replay_b64 and last_commit_ts."""
    dt_ts: Optional[datetime] = None
    if last_commit_ms is not None:
        try:
            dt_ts = datetime.fromtimestamp(last_commit_ms / 1000.0, tz=timezone.utc)
        except Exception:
            dt_ts = None
    try:
        await _ensure_offsets_table()
        async with async_session_factory() as s:
            await s.execute(sql_text(f"SET search_path TO {DB_SCHEMA}, public"))
            params = {"cid": client_db_id, "tn": topic, "rid": replay_b64, "ts": dt_ts}
            upd = f"""
                UPDATE {DB_SCHEMA}.listener_offsets
                SET last_replay_b64=:rid, last_commit_ts=:ts, updated_at=now()
                WHERE client_id=:cid AND topic_name=:tn
            """
            result = await s.execute(sql_text(upd), params)
            if getattr(result, "rowcount", 0) == 0:
                ins = f"""
                    INSERT INTO {DB_SCHEMA}.listener_offsets
                    (client_id, topic_name, last_replay_b64, last_commit_ts)
                    VALUES (:cid, :tn, :rid, :ts)
                """
                await s.execute(sql_text(ins), params)
            await s.commit()
            _OFFSETS_MEM[(client_db_id, topic)] = (replay_b64, last_commit_ms)
            return
    except Exception as e:
        LOG.debug("offset upsert failed (fallback to memory): %r", e)
    _OFFSETS_MEM[(client_db_id, topic)] = (replay_b64, last_commit_ms)

async def _clear_replay_b64(client_db_id: int, topic: str) -> None:
    """Clear the stored replay_id (e.g., when it's invalid)."""
    try:
        await _ensure_offsets_table()
        async with async_session_factory() as s:
            await s.execute(sql_text(f"SET search_path TO {DB_SCHEMA}, public"))
            upd = f"""
                UPDATE {DB_SCHEMA}.listener_offsets
                SET last_replay_b64=NULL, updated_at=now()
                WHERE client_id=:cid AND topic_name=:tn
            """
            await s.execute(sql_text(upd), {"cid": client_db_id, "tn": topic})
            await s.commit()
            # Also clear from memory cache
            if (client_db_id, topic) in _OFFSETS_MEM:
                old_b64, old_ms = _OFFSETS_MEM[(client_db_id, topic)]
                _OFFSETS_MEM[(client_db_id, topic)] = (None, old_ms)
    except Exception as e:
        LOG.debug("offset clear failed (fallback to memory): %r", e)
        # Clear from memory cache anyway
        if (client_db_id, topic) in _OFFSETS_MEM:
            old_b64, old_ms = _OFFSETS_MEM[(client_db_id, topic)]
            _OFFSETS_MEM[(client_db_id, topic)] = (None, old_ms)

def _b64encode(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")

def _b64decode(s: str) -> bytes:
    """Decode base64 string to bytes, with error handling."""
    try:
        return base64.b64decode(s.encode("ascii"))
    except Exception as e:
        raise ValueError(f"Invalid base64 replay_id: {e}") from e

def _now_ms() -> int:
    return int(time.time() * 1000)

def _normalize_commit_ms(val: Any) -> Optional[int]:
    try:
        x = int(val)
    except Exception:
        return None
    # heuristics: ns > 1e14, ms > 1e11, s > 1e9
    if x > 10**14:      # ns
        return x // 1_000_000
    if x > 10**11:      # ms
        return x
    if x > 10**9:       # s
        return x * 1000
    return x  # small test values


class SalesforceAuth:
    def __init__(self, cfg: OAuthConfig, client_name: str):
        self.cfg = cfg
        self.client_name = client_name
        self.access_token: Optional[str] = None
        self.instance_url: Optional[str] = None
        self.org_id: Optional[str] = None

    def authenticate(self):
        if self.cfg.auth_grant_type == "password":
            assert self.cfg.username and self.cfg.password, "Password grant requires username/password"
            data = {
                "grant_type": "password",
                "client_id": self.cfg.client_id,
                "client_secret": self.cfg.client_secret,
                "username": self.cfg.username,
                "password": self.cfg.password,
            }
        else:  # client_credentials
            # Salesforce's client_credentials flow requires username, password, and response_type
            assert self.cfg.username and self.cfg.password, "Client credentials grant requires username/password for Salesforce"
            data = {
                "grant_type": "client_credentials",
                "client_id": self.cfg.client_id,
                "client_secret": self.cfg.client_secret,
                "username": self.cfg.username,
                "password": self.cfg.password,
                "response_type": "code",
            }

        token_url = f"{self.cfg.login_url}/services/oauth2/token"
        LOG.info("[%s] Authenticating %s at %s", self.client_name, self.cfg.auth_grant_type, token_url)

        try:
            resp = requests.post(token_url, data=data, timeout=30)
            resp.raise_for_status()
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            err_txt = ""
            try:
                err_json = e.response.json()
                err_txt = f"{err_json.get('error')}:{err_json.get('error_description')}"
            except Exception:
                err_txt = e.response.text if getattr(e, "response", None) else str(e)
            if status in (400, 401, 403):
                # Provide helpful guidance for client_credentials domain issues
                if self.cfg.auth_grant_type == "client_credentials" and "not supported" in err_txt.lower():
                    raise FatalConfigError(
                        f"OAuth failed ({status}): {err_txt}\n"
                        f"For client_credentials grant type, you may need to use your Salesforce custom domain URL "
                        f"(e.g., https://yourdomain.my.salesforce.com or https://yourdomain--sandbox.sandbox.my.salesforce.com) "
                        f"instead of https://login.salesforce.com in the Login URL field."
                    ) from e
                raise FatalConfigError(f"OAuth failed ({status}): {err_txt}") from e
            raise

        payload = resp.json()
        self.access_token = payload.get("access_token")
        if not self.access_token:
            raise FatalConfigError("OAuth succeeded but no access_token returned")

        self.instance_url = payload.get("instance_url")

        identity_url = payload.get("id")
        if identity_url:
            id_resp = requests.get(identity_url, headers={"Authorization": f"Bearer {self.access_token}"}, timeout=20)
            try:
                id_resp.raise_for_status()
            except requests.HTTPError as e:
                raise FatalConfigError(f"Identity call failed: {e.response.text[:200] if e.response else e}") from e
            self.org_id = id_resp.json().get("organization_id")
            LOG.info("[%s] Org (tenant) id resolved: %s", self.client_name, self.org_id)
        else:
            LOG.warning("[%s] No identity URL in token response; ensure tenant_id is set.", self.client_name)

        return self.access_token, self.instance_url, self.org_id


class AvroDecoder:
    def __init__(self, stub: pb2_grpc.PubSubStub, metadata_provider: Callable[[], List[Tuple[str, str]]], client_name: str):
        self.stub = stub
        self.metadata_provider = metadata_provider
        self.client_name = client_name
        self._schema_cache: Dict[str, avro.schema.Schema] = {}

    async def get_schema(self, schema_id: str) -> avro.schema.Schema:
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]
        req = pb2.SchemaRequest(schema_id=schema_id)
        resp = await self.stub.GetSchema(req, metadata=self.metadata_provider())
        schema = avro.schema.parse(resp.schema_json)
        self._schema_cache[schema_id] = schema
        LOG.debug("[%s] Cached Avro schema %s", self.client_name, schema_id)
        return schema

    async def decode(self, schema_id: str, payload: bytes) -> Dict[str, Any]:
        schema = await self.get_schema(schema_id)
        reader = avro.io.DatumReader(schema)
        import io
        decoder = avro.io.BinaryDecoder(io.BytesIO(payload))
        return reader.read(decoder)


async def _post_webhook(url: str, payload: Dict[str, Any], client_name: str, max_attempts: int = 3) -> int:
    loop = asyncio.get_running_loop()

    def _do_post():
        return requests.post(url, json=payload, timeout=15)

    delay = 1.0
    last_status = 0
    for attempt in range(1, max_attempts + 1):
        try:
            resp = await loop.run_in_executor(None, _do_post)
            last_status = resp.status_code
            if 200 <= resp.status_code < 300:
                LOG.debug("[%s] Webhook OK (%s)", client_name, resp.status_code)
                return last_status
            else:
                LOG.warning("[%s] Webhook HTTP %s: %s", client_name, resp.status_code, resp.text[:250])
        except Exception as e:
            LOG.warning("[%s] Webhook attempt %s failed: %r", client_name, attempt, e)
        if attempt < max_attempts:
            await asyncio.sleep(delay + random.random() * 0.25)
            delay = min(delay * 2, 30.0)
    LOG.error("[%s] Webhook failed after %s attempts", client_name, max_attempts)
    return last_status


# ---------- Listener ----------

@dataclass
class ReplayStart:
    preset: int  # pb2.LATEST / EARLIEST / CUSTOM
    replay_id: Optional[bytes] = None
    drop_before_ms: Optional[int] = None  # used when mode='since'

class SFListener:
    """
    One client = one gRPC channel + one subscription stream.
    Keeps simple status for diagnostics.
    """
    def __init__(self, cfg: ClientConfig, replay_start: ReplayStart | None = None):
        self.cfg = cfg
        self.auth = SalesforceAuth(cfg.oauth, cfg.client_id)
        self._channel: Optional[grpc.aio.Channel] = None
        self._stub: Optional[pb2_grpc.PubSubStub] = None
        self._decoder: Optional[AvroDecoder] = None
        self._stop = asyncio.Event()

        self._replay_start = replay_start or ReplayStart(preset=pb2.LATEST)

        self.status: Dict[str, Any] = {
            "client_id": cfg.client_id,
            "topic": cfg.topic_name,
            "running": False,
            "events_received": 0,
            "last_event_at": None,
            "last_error": None,
            "last_webhook_status": None,
            "schema_id": None,
            "fatal": False,
            # a little visibility into our start choice
            "replay_start": {
                "preset": ("LATEST" if self._replay_start.preset == pb2.LATEST else
                           "EARLIEST" if self._replay_start.preset == pb2.EARLIEST else "CUSTOM"),
                "has_id": bool(self._replay_start.replay_id),
                "drop_before_ms": self._replay_start.drop_before_ms,
            },
            "last_replay_b64": None,
        }

    def _md(self) -> List[Tuple[str, str]]:
        return [
            ("accesstoken", self.auth.access_token or ""),
            ("tenantid", self.cfg.tenant_id or ""),
            ("instanceurl", self.auth.instance_url or ""),
        ]

    async def _reload_replay_start_from_db(self):
        """Reload the replay_start from database to get the latest saved offset."""
        rid_b64 = await _load_replay_b64(self.cfg.client_db_id, self.cfg.topic_name)
        if rid_b64:
            try:
                rid_bytes = _b64decode(rid_b64)
                LOG.info("[%s] Reloaded replay_id from DB for reconnection", self.cfg.client_id)
                self._replay_start = ReplayStart(preset=pb2.CUSTOM, replay_id=rid_bytes)
                self.status["replay_start"] = {
                    "preset": "CUSTOM",
                    "has_id": True,
                    "drop_before_ms": None,
                }
            except ValueError as e:
                LOG.warning("[%s] Invalid replay_id in DB (corrupted base64): %s. Clearing and using EARLIEST.", 
                           self.cfg.client_id, e)
                await _clear_replay_b64(self.cfg.client_db_id, self.cfg.topic_name)
                # Fall back to EARLIEST if we were using stored mode
                if self._replay_start.preset == pb2.CUSTOM:
                    self._replay_start = ReplayStart(preset=pb2.EARLIEST)
                    self.status["replay_start"] = {
                        "preset": "EARLIEST",
                        "has_id": False,
                        "drop_before_ms": None,
                    }
        else:
            # No saved offset - keep original replay_start (likely EARLIEST or LATEST)
            LOG.debug("[%s] No saved replay_id in DB, using original replay_start", self.cfg.client_id)

    async def start(self):
        backoff = 1.0
        self._stop.clear()
        self.status["running"] = True
        while not self._stop.is_set():
            clog(self.cfg.client_id, logging.INFO, "Listener loop starting")
            try:
                self.auth.authenticate()
                if not self.cfg.tenant_id and self.auth.org_id:
                    self.cfg.tenant_id = self.auth.org_id
                # Reload replay_id from DB on each reconnection to get latest saved offset
                await self._reload_replay_start_from_db()
                await self._connect_channel()
                await self._diag_gettopic_getschema()
                await self._subscribe_loop()
                if self._stop.is_set():
                    break

            except FatalConfigError as e:
                self.status["last_error"] = str(e)
                self.status["fatal"] = True
                LOG.error("[%s] Fatal config error: %s. Stopping; fix config then restart.", self.cfg.client_id, e)
                break

            except grpc.aio.AioRpcError as e:
                code = e.code()
                msg = e.details()
                self.status["last_error"] = f"{code.name}: {msg}"
                
                # Handle INVALID_ARGUMENT errors related to replay_id validation
                if code == grpc.StatusCode.INVALID_ARGUMENT and "replay" in msg.lower() and "id" in msg.lower():
                    LOG.warning("[%s] Invalid replay_id detected: %s. Clearing stored replay_id and falling back to EARLIEST.", 
                               self.cfg.client_id, msg)
                    await _clear_replay_b64(self.cfg.client_db_id, self.cfg.topic_name)
                    # Reset to EARLIEST for next connection attempt
                    self._replay_start = ReplayStart(preset=pb2.EARLIEST)
                    self.status["replay_start"] = {
                        "preset": "EARLIEST",
                        "has_id": False,
                        "drop_before_ms": None,
                    }
                    # Continue to reconnect with EARLIEST
                
                elif code in (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.PERMISSION_DENIED) and FAIL_FAST_NOT_FOUND:
                    self.status["fatal"] = True
                    LOG.error("[%s] gRPC error %s: %s. Stopping (fail-fast).", self.cfg.client_id, code.name, msg)
                    break
                else:
                    LOG.error("[%s] gRPC error %s: %s", self.cfg.client_id, code.name, msg)

            except Exception as e:
                self.status["last_error"] = repr(e)
                LOG.error("[%s] Client error: %r", self.cfg.client_id, e)

            finally:
                await self._cleanup_channel()
                clog(self.cfg.client_id, logging.INFO, "Disconnected")

            if not self._stop.is_set() and not self.status.get("fatal"):
                clog(self.cfg.client_id, logging.WARNING, f"Reconnecting in {backoff:.1f}s")
                await asyncio.sleep(backoff + random.random() * 0.5)
                backoff = min(backoff * 2, 60.0)

        self.status["running"] = False

    async def stop(self):
        self._stop.set()
        await self._cleanup_channel()

    async def _connect_channel(self):
        host = self.cfg.pubsub_host or DEFAULT_PUBSUB_HOST
        LOG.info("[%s] Connecting gRPC to %s …", self.cfg.client_id, host)
        creds = grpc.ssl_channel_credentials()
        options = [
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ("grpc.keepalive_time_ms", 30_000),
            ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.min_time_between_pings_ms", 30_000),
            ("grpc.http2.max_pings_without_data", 0),
        ]
        self._channel = grpc.aio.secure_channel(host, creds, options=options)
        self._stub = pb2_grpc.PubSubStub(self._channel)
        self._decoder = AvroDecoder(self._stub, self._md, self.cfg.client_id)

    async def _cleanup_channel(self):
        if self._channel:
            try:
                await self._channel.close()
            except Exception:
                pass
        self._channel = None
        self._stub = None
        self._decoder = None

    async def _diag_gettopic_getschema(self):
        assert self._stub is not None
        try:
            topic_resp = await self._stub.GetTopic(pb2.TopicRequest(topic_name=self.cfg.topic_name), metadata=self._md())
            schema_id = getattr(topic_resp, "schema_id", None) or getattr(topic_resp, "schemaId", None)
            if schema_id:
                self.status["schema_id"] = schema_id
                await self._decoder.get_schema(schema_id)
                LOG.info("[%s] Pre-flight OK: topic=%s schema=%s", self.cfg.client_id, self.cfg.topic_name, schema_id)
            else:
                raise FatalConfigError(f"Topic {self.cfg.topic_name} returned no schema_id")
        except grpc.aio.AioRpcError as e:
            code = e.code()
            if code in (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.PERMISSION_DENIED) and FAIL_FAST_NOT_FOUND:
                raise FatalConfigError(f"GetTopic failed ({code.name}): {e.details()}")
            if code == grpc.StatusCode.UNAUTHENTICATED and FAIL_FAST_AUTH:
                raise FatalConfigError(f"GetTopic unauthenticated: {e.details()}")
            raise

    async def _subscribe_loop(self):
        assert self._stub and self._decoder
        queue: asyncio.Queue = asyncio.Queue()
        stop_local = False
        loop = asyncio.get_running_loop()
        last_rx = loop.time()

        async def request_gen():
            try:
                # Initial credit with our replay choice
                req_kwargs = dict(
                    topic_name=self.cfg.topic_name,
                    replay_preset=self._replay_start.preset,
                    num_requested=self.cfg.flow_batch_size,
                )
                if self._replay_start.preset == pb2.CUSTOM and self._replay_start.replay_id:
                    req_kwargs["replay_id"] = self._replay_start.replay_id  # bytes
                yield pb2.FetchRequest(**req_kwargs)

                while True:
                    req = await queue.get()
                    if req is None:
                        return
                    yield req
            except asyncio.CancelledError:
                return

        async def heartbeater():
            try:
                while True:
                    await asyncio.sleep(HEARTBEAT_SECONDS)
                    await queue.put(pb2.FetchRequest(num_requested=self.cfg.flow_batch_size))
                    LOG.info("[%s] Heartbeat fetch sent", self.cfg.client_id)
            except asyncio.CancelledError:
                pass

        async def watchdog():
            try:
                while True:
                    await asyncio.sleep(max(HEARTBEAT_SECONDS, 30))
                    if loop.time() - last_rx > IDLE_RESET_SECONDS:
                        LOG.warning("[%s] No messages for %ss; resetting stream", self.cfg.client_id, IDLE_RESET_SECONDS)
                        raise RuntimeError("Idle timeout")
            except asyncio.CancelledError:
                pass

        stream = self._stub.Subscribe(request_gen(), metadata=self._md())
        hb_task = asyncio.create_task(heartbeater())
        wd_task = asyncio.create_task(watchdog())

        try:
            async for resp in stream:
                last_rx = loop.time()
                events = getattr(resp, "events", [])
                if not events:
                    await queue.put(pb2.FetchRequest(num_requested=self.cfg.flow_batch_size))
                    continue
                for ce in events:
                    try:
                        ev = ce.event
                        schema_id = ev.schema_id
                        payload = ev.payload

                        decoded = await self._decoder.decode(schema_id, payload)
                        header = decoded.get("ChangeEventHeader", {})
                        commit_ms = _normalize_commit_ms(header.get("commitTimestamp"))

                        # If running with "since", optionally skip old events,
                        # but still advance the stored replay id so restarts don't re-stream them.
                        if self._replay_start.drop_before_ms is not None and commit_ms is not None:
                            if commit_ms < self._replay_start.drop_before_ms:
                                rid_bytes = getattr(ev, "replay_id", None) or getattr(ce, "replay_id", None)
                                if isinstance(rid_bytes, (bytes, bytearray)):
                                    rid_b64 = _b64encode(bytes(rid_bytes))
                                    await _save_replay_b64(self.cfg.client_db_id, self.cfg.topic_name, rid_b64, commit_ms)
                                    self.status["last_replay_b64"] = rid_b64
                                continue  # skip webhook

                        # process
                        entity = header.get("entityName", "Unknown")
                        change_type = header.get("changeType", "Unknown")
                        ids = header.get("recordIds", [])
                        LOG.info("[%s] Event: entity=%s type=%s ids=%s ts=%s",
                                 self.cfg.client_id, entity, change_type, ids, commit_ms)

                        # Log full decoded event payload for debugging
                        try:
                            # Convert to JSON-serializable format for logging
                            decoded_log = json.dumps(decoded, default=str, indent=2)
                            LOG.info("[%s] Full event payload:\n%s", self.cfg.client_id, decoded_log)
                        except Exception as e:
                            LOG.warning("[%s] Could not serialize event payload for logging: %r", self.cfg.client_id, e)
                            LOG.info("[%s] Event payload keys: %s", self.cfg.client_id, list(decoded.keys()))

                        # Get FlashField__c - could be a list (one per record) or a single value
                        flash_field_raw = decoded.get("FlashField__c")
                        LOG.info("[%s] FlashField__c raw value: %r (type=%s, exists=%s)", 
                                 self.cfg.client_id, flash_field_raw, 
                                 type(flash_field_raw).__name__ if flash_field_raw is not None else "None",
                                 "FlashField__c" in decoded)
                        
                        # Track webhook attempts and results - need ALL attempted webhooks to succeed before saving offset
                        webhook_attempted_count = 0
                        webhook_succeeded_count = 0
                        attempted_any_webhook = False
                        last_webhook_status = None

                        # Process each record ID separately
                        if not ids:
                            LOG.warning("[%s] Event has no recordIds, skipping", self.cfg.client_id)
                            # Still save offset for events with no recordIds
                            rid_bytes = getattr(ev, "replay_id", None) or getattr(ce, "replay_id", None)
                            if isinstance(rid_bytes, (bytes, bytearray)):
                                rid_b64 = _b64encode(bytes(rid_bytes))
                                await _save_replay_b64(self.cfg.client_db_id, self.cfg.topic_name, rid_b64, commit_ms)
                                self.status["last_replay_b64"] = rid_b64
                            continue

                        # Get replay_id for this event
                        rid_bytes = getattr(ev, "replay_id", None) or getattr(ce, "replay_id", None)
                        rid_b64 = None
                        if isinstance(rid_bytes, (bytes, bytearray)):
                            rid_b64 = _b64encode(bytes(rid_bytes))

                        for idx, record_id in enumerate(ids):
                            # Determine FlashField__c value for this record
                            # If it's a list, use the index; otherwise use the single value
                            if isinstance(flash_field_raw, list):
                                flash_field = flash_field_raw[idx] if idx < len(flash_field_raw) else None
                            else:
                                flash_field = flash_field_raw

                            # Debug: Log what FlashField__c value we got
                            LOG.info("[%s] FlashField__c check for recordId=%s: value=%r (type=%s, repr=%r)",
                                     self.cfg.client_id, record_id, flash_field, 
                                     type(flash_field).__name__ if flash_field is not None else "None",
                                     repr(flash_field))

                            # Normalize FlashField__c value - handle boolean True, string "true", "True", etc.
                            flash_field_normalized = None
                            if flash_field is True:
                                flash_field_normalized = True
                            elif flash_field is False:
                                flash_field_normalized = False
                            elif isinstance(flash_field, str):
                                # Handle string values: "true", "True", "TRUE", "1", etc.
                                flash_field_lower = flash_field.lower().strip()
                                if flash_field_lower in ("true", "1", "yes", "y"):
                                    flash_field_normalized = True
                                elif flash_field_lower in ("false", "0", "no", "n", ""):
                                    flash_field_normalized = False
                                else:
                                    LOG.warning("[%s] FlashField__c has unexpected string value: %r for recordId=%s",
                                               self.cfg.client_id, flash_field, record_id)
                            elif flash_field is None:
                                flash_field_normalized = None
                            else:
                                # Try to convert to bool if it's a number (1 = True, 0 = False)
                                try:
                                    flash_field_normalized = bool(flash_field)
                                    LOG.info("[%s] FlashField__c converted from %r to bool: %r for recordId=%s",
                                            self.cfg.client_id, flash_field, flash_field_normalized, record_id)
                                except Exception:
                                    LOG.warning("[%s] FlashField__c has unexpected type/value: %r (type=%s) for recordId=%s",
                                               self.cfg.client_id, flash_field, type(flash_field).__name__, record_id)

                            # Only send webhook if FlashField__c is explicitly True
                            # Skip if it's None, False, or missing from the event
                            if flash_field_normalized is not True:
                                if "FlashField__c" in decoded:
                                    LOG.info("[%s] Skipping webhook: FlashField__c is %r (normalized: %r, not True) for recordId=%s (entity=%s)",
                                             self.cfg.client_id, flash_field, flash_field_normalized, record_id, entity)
                                else:
                                    LOG.info("[%s] Skipping webhook: FlashField__c is missing for recordId=%s (entity=%s)",
                                             self.cfg.client_id, record_id, entity)
                                continue

                            # Send webhook for this single record ID
                            attempted_any_webhook = True
                            webhook_attempted_count += 1
                            LOG.info("[%s] Sending webhook for recordId=%s (entity=%s)", 
                                     self.cfg.client_id, record_id, entity)
                            
                            # Create a modified decoded payload with only this recordId
                            # Deep copy and modify the decoded payload to have only this recordId
                            decoded_copy = copy.deepcopy(decoded)
                            if "ChangeEventHeader" in decoded_copy:
                                decoded_copy["ChangeEventHeader"] = copy.deepcopy(header)
                                decoded_copy["ChangeEventHeader"]["recordIds"] = [record_id]
                            
                            webhook_payload = {
                                "client_id": self.cfg.client_id,
                                "topic": self.cfg.topic_name,
                                "schema_id": schema_id,
                                "recordId": record_id,  # Single record ID in body
                                "decoded": decoded_copy,  # Modified decoded with only this recordId
                            }

                            status = await _post_webhook(self.cfg.webhook_url, webhook_payload, self.cfg.client_id)
                            last_webhook_status = status
                            
                            if 200 <= status < 300:
                                webhook_succeeded_count += 1

                        # Save offset after processing event
                        # Option A: Only save offset if ALL attempted webhooks succeeded OR no webhook was needed
                        # If ANY webhook failed, DON'T save offset so Salesforce will replay the entire event on reconnect
                        if rid_b64:
                            if attempted_any_webhook:
                                # Webhook(s) were attempted - only save if ALL succeeded
                                if webhook_succeeded_count == webhook_attempted_count:
                                    # All webhooks succeeded - save offset
                                    await _save_replay_b64(
                                        self.cfg.client_db_id, 
                                        self.cfg.topic_name, 
                                        rid_b64, 
                                        commit_ms
                                    )
                                    self.status["last_replay_b64"] = rid_b64
                                    LOG.info("[%s] Saved offset - all %d webhook(s) succeeded", 
                                            self.cfg.client_id, webhook_succeeded_count)
                                else:
                                    # At least one webhook failed - DON'T save offset, Salesforce will replay entire event
                                    LOG.warning("[%s] %d of %d webhook(s) failed - NOT saving offset, will replay entire event on reconnect to retry failed webhooks",
                                               self.cfg.client_id, 
                                               webhook_attempted_count - webhook_succeeded_count,
                                               webhook_attempted_count)
                            else:
                                # No webhook attempted (all records skipped) - save offset since no webhook needed
                                await _save_replay_b64(self.cfg.client_db_id, self.cfg.topic_name, rid_b64, commit_ms)
                                self.status["last_replay_b64"] = rid_b64
                                LOG.debug("[%s] Saved offset (no webhook needed - all records skipped)",
                                         self.cfg.client_id)

                        self.status["events_received"] += 1
                        self.status["last_event_at"] = commit_ms
                        if last_webhook_status is not None:
                            self.status["last_webhook_status"] = last_webhook_status
                    except Exception as e:
                        self.status["last_error"] = f"Event processing error: {e!r}"
                        LOG.error("[%s] Event processing error: %r", self.cfg.client_id, e)
                await queue.put(pb2.FetchRequest(num_requested=self.cfg.flow_batch_size))
        except asyncio.CancelledError:
            raise
        finally:
            for t in (hb_task, wd_task):
                try:
                    t.cancel()
                except Exception:
                    pass
            if not stop_local:
                stop_local = True
                try:
                    await queue.put(None)
                except Exception:
                    pass
                try:
                    await stream.cancel()
                except Exception:
                    pass
            LOG.info("[%s] Subscribe loop ended", self.cfg.client_id)

    async def diag(self) -> Dict[str, Any]:
        return dict(self.status)


# --- entrypoint used by ListenerManager ---
async def run_salesforce_pubsub(
    client_row,
    stop_event: asyncio.Event,
    clog: Callable[[int, str], None] | None = None,
    *,
    replay: Optional[ReplayArgs] = None,
) -> None:
    """
    Adapter so ListenerManager can start/stop without knowing SF internals.
    """
    # Ensure offsets table exists if allowed
    await _ensure_offsets_table()

    oauth = OAuthConfig(
        login_url=client_row.login_url,
        client_id=client_row.oauth_client_id,
        client_secret=client_row.oauth_client_secret,
        username=getattr(client_row, "oauth_username", None),
        password=getattr(client_row, "oauth_password", None),
        auth_grant_type=client_row.oauth_grant_type,
    )
    cfg = ClientConfig(
        client_db_id=client_row.id,
        client_id=client_row.client_name,
        topic_name=client_row.topic_name,
        webhook_url=client_row.webhook_url,
        oauth=oauth,
        pubsub_host=(getattr(client_row, "pubsub_host", None) or DEFAULT_PUBSUB_HOST),
        tenant_id=getattr(client_row, "tenant_id", None),
        flow_batch_size=int(getattr(client_row, "flow_batch_size", 100) or 100),
    )

    # Compute the replay start
    mode = (replay.mode if replay else "stored").lower()
    if mode == "latest":
        LOG.info("[%s] Replay start: LATEST", cfg.client_id)
        start = ReplayStart(preset=pb2.LATEST)

    elif mode == "earliest":
        LOG.info("[%s] Replay start: EARLIEST (backfill within SF retention)", cfg.client_id)
        start = ReplayStart(preset=pb2.EARLIEST)

    elif mode == "custom" and replay and replay.replay_id_b64:
        try:
            rid = _b64decode(replay.replay_id_b64)
            LOG.info("[%s] Replay start: CUSTOM id provided", cfg.client_id)
            start = ReplayStart(preset=pb2.CUSTOM, replay_id=rid)
        except ValueError as e:
            LOG.error("[%s] invalid custom replay_id_b64 (base64 decode failed): %s; falling back to LATEST", 
                     cfg.client_id, e)
            start = ReplayStart(preset=pb2.LATEST)
        except Exception as e:
            LOG.error("[%s] invalid custom replay_id_b64: %s; falling back to LATEST", cfg.client_id, e)
            start = ReplayStart(preset=pb2.LATEST)

    elif mode == "since" and replay and replay.since_minutes and replay.since_minutes > 0:
        cutoff = _now_ms() - (replay.since_minutes * 60 * 1000)
        LOG.info("[%s] Replay start: SINCE %s min (EARLIEST + local drop before %s)",
                 cfg.client_id, replay.since_minutes, cutoff)
        start = ReplayStart(preset=pb2.EARLIEST, drop_before_ms=cutoff)

    else:  # stored (default): use saved id if present, else earliest
        # With Option A: if webhook fails, we don't save offset, so last_replay_b64 is the last successful webhook
        # Salesforce will automatically replay from last saved offset, retrying any failed webhooks
        rid_b64 = await _load_replay_b64(cfg.client_db_id, cfg.topic_name)
        if rid_b64:
            try:
                rid_bytes = _b64decode(rid_b64)
                LOG.info("[%s] Replay start: STORED (using saved id) - will retry any failed webhooks automatically",
                         cfg.client_id)
                start = ReplayStart(preset=pb2.CUSTOM, replay_id=rid_bytes)
            except ValueError as e:
                LOG.warning("[%s] Invalid replay_id in DB (corrupted base64): %s. Clearing and using EARLIEST.", 
                           cfg.client_id, e)
                await _clear_replay_b64(cfg.client_db_id, cfg.topic_name)
                LOG.info("[%s] Replay start: no saved id -> EARLIEST (first run backfill within retention)", cfg.client_id)
                start = ReplayStart(preset=pb2.EARLIEST)
        else:
            LOG.info("[%s] Replay start: no saved id -> EARLIEST (first run backfill within retention)", cfg.client_id)
            start = ReplayStart(preset=pb2.EARLIEST)

    listener = SFListener(cfg, replay_start=start)
    task = asyncio.create_task(listener.start(), name=f"sf-listener-{client_row.id}")

    try:
        done, pending = await asyncio.wait(
            {task, asyncio.create_task(stop_event.wait())},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_event.is_set() and not task.done():
            if clog:
                clog(logging.INFO, "Stopping listener...")
            await listener.stop()
            try:
                await task
            except asyncio.CancelledError:
                pass
    finally:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


# --- connection test helper ---

class TestConnResult(TypedDict, total=False):
    ok: bool
    auth: dict
    topic: dict

async def test_salesforce_connection(
    oauth: OAuthConfig,
    *,
    topic_name: Optional[str] = None,
    pubsub_host: Optional[str] = None,
    tenant_id: Optional[str] = None,
) -> TestConnResult:
    """
    Try OAuth (and optionally GetTopic) and return a structured result.
    Does not persist or start listeners.
    """
    result: TestConnResult = {"ok": False}

    auth = SalesforceAuth(oauth, client_name="test")
    loop = asyncio.get_running_loop()

    try:
        await loop.run_in_executor(None, auth.authenticate)
    except FatalConfigError as e:
        result["auth"] = {"ok": False, "error": str(e)}
        return result
    except Exception as e:
        result["auth"] = {"ok": False, "error": repr(e)}
        return result

    result["auth"] = {
        "ok": True,
        "org_id": auth.org_id,
        "instance_url": auth.instance_url,
    }

    if topic_name:
        host = (pubsub_host or DEFAULT_PUBSUB_HOST)
        channel = grpc.aio.secure_channel(
            host,
            grpc.ssl_channel_credentials(),
            options=[
                ("grpc.max_receive_message_length", 64 * 1024 * 1024),
            ],
        )
        stub = pb2_grpc.PubSubStub(channel)
        md = [
            ("accesstoken", auth.access_token or ""),
            ("tenantid", (tenant_id or auth.org_id or "")),
            ("instanceurl", auth.instance_url or ""),
        ]
        try:
            resp = await stub.GetTopic(pb2.TopicRequest(topic_name=topic_name), metadata=md)
            schema_id = getattr(resp, "schema_id", None) or getattr(resp, "schemaId", None)
            result["topic"] = {"ok": True, "schema_id": schema_id}
        except grpc.aio.AioRpcError as e:
            result["topic"] = {
                "ok": False,
                "code": e.code().name,
                "error": e.details(),
            }
        finally:
            try:
                await channel.close()
            except Exception:
                pass

    result["ok"] = result["auth"]["ok"] and (result.get("topic", {"ok": True})["ok"])
    return result
