# app/services/sf_pubsub.py
from __future__ import annotations

import asyncio
import logging
import random
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Callable

import requests
import avro.schema
import avro.io
import grpc
from typing import TypedDict

# Prefer proto under app.sfproto, fall back to flat
try:
    from app.sfproto import pubsub_api_pb2 as pb2  # type: ignore
    from app.sfproto import pubsub_api_pb2_grpc as pb2_grpc  # type: ignore
except Exception:
    import pubsub_api_pb2 as pb2  # type: ignore
    import pubsub_api_pb2_grpc as pb2_grpc  # type: ignore


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
    client_id: str          # label used for logs
    topic_name: str         # e.g. "/data/OpportunityChangeEvent"
    webhook_url: str
    oauth: OAuthConfig
    pubsub_host: Optional[str] = None
    tenant_id: Optional[str] = None
    flow_batch_size: int = 100


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
        else:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.cfg.client_id,
                "client_secret": self.cfg.client_secret,
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


class SFListener:
    """
    One client = one gRPC channel + one subscription stream.
    Keeps simple status for diagnostics.
    """
    def __init__(self, cfg: ClientConfig):
        self.cfg = cfg
        self.auth = SalesforceAuth(cfg.oauth, cfg.client_id)
        self._channel: Optional[grpc.aio.Channel] = None
        self._stub: Optional[pb2_grpc.PubSubStub] = None
        self._decoder: Optional[AvroDecoder] = None
        self._stop = asyncio.Event()

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
        }

    def _md(self) -> List[Tuple[str, str]]:
        return [
            ("accesstoken", self.auth.access_token or ""),
            ("tenantid", self.cfg.tenant_id or ""),
            ("instanceurl", self.auth.instance_url or ""),
        ]

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
                if code in (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.PERMISSION_DENIED) and FAIL_FAST_NOT_FOUND:
                    self.status["fatal"] = True
                    LOG.error("[%s] gRPC error %s: %s. Stopping (fail-fast).", self.cfg.client_id, code.name, msg)
                    break
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
        host = self.cfg.pubsub_host or "api.pubsub.salesforce.com:7443"
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
                yield pb2.FetchRequest(
                    topic_name=self.cfg.topic_name,
                    replay_preset=pb2.LATEST,
                    num_requested=self.cfg.flow_batch_size,
                )
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
                        entity = header.get("entityName", "Unknown")
                        change_type = header.get("changeType", "Unknown")
                        commit_ts = header.get("commitTimestamp")
                        ids = header.get("recordIds", [])
                        LOG.info("[%s] Event: entity=%s type=%s ids=%s ts=%s",
                                 self.cfg.client_id, entity, change_type, ids, commit_ts)
                        status = await _post_webhook(self.cfg.webhook_url, {
                            "client_id": self.cfg.client_id,
                            "topic": self.cfg.topic_name,
                            "schema_id": schema_id,
                            "decoded": decoded,
                        }, self.cfg.client_id)
                        self.status["events_received"] += 1
                        self.status["last_event_at"] = commit_ts
                        self.status["last_webhook_status"] = status
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
) -> None:
    """
    Adapter so ListenerManager can start/stop without knowing SF internals.
    """
    oauth = OAuthConfig(
        login_url=client_row.login_url,
        client_id=client_row.oauth_client_id,
        client_secret=client_row.oauth_client_secret,
        username=getattr(client_row, "oauth_username", None),
        password=getattr(client_row, "oauth_password", None),
        auth_grant_type=client_row.oauth_grant_type,
    )
    cfg = ClientConfig(
        client_id=client_row.client_name,
        topic_name=client_row.topic_name,
        webhook_url=client_row.webhook_url,
        oauth=oauth,
        pubsub_host=(getattr(client_row, "pubsub_host", None) or "api.pubsub.salesforce.com:7443"),
        tenant_id=getattr(client_row, "tenant_id", None),
        flow_batch_size=int(getattr(client_row, "flow_batch_size", 100) or 100),
    )

    listener = SFListener(cfg)
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

    # Run sync requests in a thread so we don't block the event loop
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

    # Optionally validate the topic via Pub/Sub GetTopic
    if topic_name:
        host = (pubsub_host or "api.pubsub.salesforce.com:7443")
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

    # overall ok = auth ok AND (topic ok if provided)
    result["ok"] = result["auth"]["ok"] and (result.get("topic", {"ok": True})["ok"])
    return result
