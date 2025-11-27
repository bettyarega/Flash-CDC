from __future__ import annotations
from typing import Optional
from datetime import datetime
from enum import Enum
from sqlmodel import SQLModel, Field, Column
from sqlalchemy import DateTime, func, UniqueConstraint, MetaData
from pydantic import (
    ConfigDict,
    TypeAdapter,
    field_validator,
    model_validator,
    EmailStr,   # used only for validation
    HttpUrl,    # used only for validation
)


from pydantic import EmailStr
from sqlmodel import SQLModel

# ---------- Enums & helpers ----------

class GrantType(str, Enum):
    password = "password"
    client_credentials = "client_credentials"

def _mask(value: Optional[str], keep_last: int = 4) -> Optional[str]:
    if not value:
        return value
    if len(value) <= keep_last:
        return "*" * len(value)
    return "*" * (len(value) - keep_last) + value[-keep_last:]

# validators
_email_adapter = TypeAdapter(EmailStr)
_url_adapter = TypeAdapter(HttpUrl)

# ---------- Base & Table Model ----------

class ClientBase(SQLModel):
    model_config = ConfigDict(from_attributes=True)

    client_name: str = Field(index=True, unique=True, min_length=2, max_length=100)

    # Store as str; validate as URL
    login_url: str = Field(default="https://login.salesforce.com")
    oauth_grant_type: GrantType = Field(default=GrantType.password)

    oauth_client_id: str = Field(min_length=6)
    oauth_client_secret: str = Field(min_length=6)

    # Store as str; validate as email if provided
    oauth_username: Optional[str] = None
    oauth_password: Optional[str] = None

    topic_name: str = Field(min_length=5, max_length=200, description="e.g. /data/AccountChangeEvent")

    # Store as str; validate as URL
    webhook_url: str

    pubsub_host: Optional[str] = Field(
        default="api.pubsub.salesforce.com:7443",
        description="host:port",
    )

    tenant_id: Optional[str] = None
    flow_batch_size: int = Field(default=100, ge=1, le=5000)
    is_active: bool = True

    @field_validator("login_url")
    @classmethod
    def validate_login_url(cls, v: str) -> str:
        # Normalize: ensure URL has a scheme (https://)
        v = v.strip()
        if not v.startswith(("http://", "https://")):
            v = f"https://{v}"
        _ = _url_adapter.validate_python(v)
        return str(v)

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, v: str) -> str:
        _ = _url_adapter.validate_python(v)
        return str(v)

    @field_validator("oauth_username")
    @classmethod
    def validate_email_optional(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            _ = _email_adapter.validate_python(v)
        return v

    @field_validator("topic_name")
    @classmethod
    def validate_topic(cls, v: str) -> str:
        if not v.startswith("/data/") or "ChangeEvent" not in v:
            raise ValueError("topic_name must look like /data/<Something>ChangeEvent")
        return v

    @field_validator("pubsub_host")
    @classmethod
    def validate_pubsub_host(cls, v: Optional[str]) -> Optional[str]:
        if v and ":" not in v:
            raise ValueError("pubsub_host must be in host:port form, e.g. api.pubsub.salesforce.com:7443")
        return v

    @model_validator(mode="after")
    def cross_field_oauth_rules(self) -> "ClientBase":
        if self.oauth_grant_type == GrantType.password:
            if not self.oauth_username or not self.oauth_password:
                raise ValueError("For grant_type=password, oauth_username and oauth_password are required.")
        else:  # client_credentials
            # Salesforce's client_credentials flow requires username and password
            if not self.oauth_username or not self.oauth_password:
                raise ValueError("For grant_type=client_credentials, oauth_username and oauth_password are required for Salesforce.")
        return self


class Client(ClientBase, table=True):
    __tablename__ = "clients"
    # Schema is set programmatically in init_db() (see app/db.py)
    # We can't mix schema dict with UniqueConstraint in __table_args__ tuple in SQLAlchemy
    # So the schema is set on the table metadata during database initialization
    __table_args__ = (
        UniqueConstraint("oauth_client_id", "topic_name", name="uq_clients_oauth_topic"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    created_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), server_default=func.now())
    )
    updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    )

# ---------- DTOs ----------

class ClientCreate(ClientBase):
    pass

class ClientUpdate(SQLModel):
    model_config = ConfigDict(from_attributes=True)

    client_name: Optional[str] = None
    login_url: Optional[str] = None
    oauth_grant_type: Optional[GrantType] = None
    oauth_client_id: Optional[str] = None
    oauth_client_secret: Optional[str] = None
    oauth_username: Optional[str] = None
    oauth_password: Optional[str] = None
    topic_name: Optional[str] = None
    webhook_url: Optional[str] = None
    pubsub_host: Optional[str] = None
    tenant_id: Optional[str] = None
    flow_batch_size: Optional[int] = None
    is_active: Optional[bool] = None

    @model_validator(mode="after")
    def cross_field_update_rules(self) -> "ClientUpdate":
        if self.oauth_grant_type == GrantType.password:
            if (self.oauth_username is None) ^ (self.oauth_password is None):
                raise ValueError("When setting grant_type=password, both oauth_username and oauth_password must be provided together.")
        if self.oauth_grant_type == GrantType.client_credentials:
            # Salesforce's client_credentials flow requires username and password
            if (self.oauth_username is None) ^ (self.oauth_password is None):
                raise ValueError("When setting grant_type=client_credentials, both oauth_username and oauth_password must be provided together for Salesforce.")
        # Re-validate changed url/email fields if present
        if self.login_url is not None:
            _url_adapter.validate_python(self.login_url)
        if self.webhook_url is not None:
            _url_adapter.validate_python(self.webhook_url)
        if self.oauth_username is not None:
            _email_adapter.validate_python(self.oauth_username)
        if self.topic_name is not None:
            if not self.topic_name.startswith("/data/") or "ChangeEvent" not in self.topic_name:
                raise ValueError("topic_name must look like /data/<Something>ChangeEvent")
        if self.pubsub_host is not None and ":" not in self.pubsub_host:
            raise ValueError("pubsub_host must be in host:port form")
        return self

class ClientReadBase(SQLModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    client_name: str
    login_url: str
    oauth_grant_type: GrantType
    topic_name: str
    webhook_url: str
    pubsub_host: Optional[str]
    tenant_id: Optional[str]
    flow_batch_size: int
    is_active: bool
    created_at: datetime
    updated_at: datetime

class ClientReadSafe(ClientReadBase):
    oauth_client_id_masked: Optional[str] = None

class ClientReadWithSecrets(ClientReadBase):
    oauth_client_id: str
    oauth_client_secret: str
    oauth_username: Optional[str]
    oauth_password: Optional[str]

def to_safe(client: Client) -> ClientReadSafe:
    return ClientReadSafe(
        id=client.id,
        client_name=client.client_name,
        login_url=client.login_url,
        oauth_grant_type=client.oauth_grant_type,
        topic_name=client.topic_name,
        webhook_url=client.webhook_url,
        pubsub_host=client.pubsub_host,
        tenant_id=client.tenant_id,
        flow_batch_size=client.flow_batch_size,
        is_active=client.is_active,
        created_at=client.created_at,
        updated_at=client.updated_at,
        oauth_client_id_masked=_mask(client.oauth_client_id),
    )


class RoleEnum(str, Enum):
    admin = "admin"
    user = "user"
    amsa = "amsa"

class User(SQLModel, table=True):
    __tablename__ = "users"
    __table_args__ = {"schema": "flash"}

    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(index=True, unique=True)
    password_hash: str
    role: RoleEnum = Field(default=RoleEnum.user)
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)



class UserCreate(SQLModel):
    email: EmailStr
    password: str
    role: RoleEnum | None = None  # default to 'user' if omitted
    is_active: bool = True

class UserUpdate(SQLModel):
    role: RoleEnum | None = None
    is_active: bool | None = None
    password: str | None = None   # optional password reset

class UserRead(SQLModel):
    id: int
    email: str
    role: RoleEnum
    is_active: bool
    created_at: datetime



class ListenerOffset(SQLModel, table=True):
    __tablename__ = "listener_offsets"
    __table_args__ = {"schema": "flash"}  # keep your schema

    id: Optional[int] = Field(default=None, primary_key=True)
    client_id: int = Field(index=True)
    topic_name: str = Field(index=True)
    # Store replay_id as base64 string to remain portable (bytes in SF)
    last_replay_b64: Optional[str] = Field(default=None)
    last_commit_ts: Optional[datetime] = Field(default=None, index=True)
    updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
    )