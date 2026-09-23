"""Typed storage configuration and its editor-facing JSON Schema."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)


class DefaultChainCredentials(_Model):
    type: Literal["default_chain"] = "default_chain"


class ProfileCredentials(_Model):
    type: Literal["profile"] = "profile"
    profile: str


class EnvCredentials(_Model):
    """Fields contain environment variable names, not secret values."""

    type: Literal["env"] = "env"
    access_key_id: str = Field(description="Name of the access-key environment variable")
    secret_access_key: str = Field(description="Name of the secret-key environment variable")
    session_token: str | None = None


class InlineCredentials(_Model):
    """Serialization includes secrets for the native client; repr omits them."""

    type: Literal["inline"] = "inline"
    access_key_id: str = Field(repr=False)
    secret_access_key: str = Field(repr=False, json_schema_extra={"writeOnly": True})
    session_token: str | None = Field(
        default=None, repr=False, json_schema_extra={"writeOnly": True}
    )


Credentials = Annotated[
    DefaultChainCredentials | ProfileCredentials | EnvCredentials | InlineCredentials,
    Field(discriminator="type"),
]


class S3Config(_Model):
    type: Literal["s3"] = "s3"
    bucket: str = Field(min_length=1, description="Bucket name, without s3://")
    root: str | None = Field(
        default=None, description="Optional object-key prefix inside the bucket"
    )
    endpoint: str | None = Field(default=None, description="S3 API endpoint; omit for AWS S3")
    region: str | None = None
    credentials: Credentials | None = Field(
        default=None, description="Omit to use the default credential chain"
    )
    write_chunk_bytes: int = Field(default=16_777_216, ge=5_242_880, strict=True)
    write_concurrency: int = Field(default=8, ge=1, strict=True)


class FilesystemConfig(_Model):
    type: Literal["filesystem"] = "filesystem"
    root: Path


class HttpConfig(_Model):
    type: Literal["http"] = "http"
    base_url: str = Field(description="Read-only HTTP(S) base URL for object keys")


StoreConfig = Annotated[S3Config | FilesystemConfig | HttpConfig, Field(discriminator="type")]
StoreId = Annotated[str, Field(pattern=r"^[a-zA-Z0-9_-]+$")]


class CatalogConfig(_Model):
    store: StoreId | None = None
    prefix: str = "catalog"


class ShardPackConfig(_Model):
    """Storage configuration. A single store is selected automatically."""

    version: Literal[1] = 1
    default_store: StoreId | None = None
    stores: dict[StoreId, StoreConfig] = Field(min_length=1)
    catalog: CatalogConfig = Field(default_factory=CatalogConfig)

    @model_validator(mode="after")
    def check_store_references(self) -> Self:
        for store in (self.default_store, self.catalog.store):
            if store is not None and store not in self.stores:
                raise ValueError(f"unknown store {store!r}")
        return self


def config_json_schema() -> dict:
    """Generate the structural schema; store-reference checks run in the model."""
    schema = ShardPackConfig.model_json_schema()
    # Constructors supply tags automatically, but hand-written TOML/JSON must
    # supply them for Rust's tagged enums and Pydantic's discriminated unions.
    for definition in schema.get("$defs", {}).values():
        if "type" in definition.get("properties", {}):
            definition.setdefault("required", []).append("type")
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        **schema,
    }
