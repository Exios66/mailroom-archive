"""Runtime settings, all from MAILROOM_ARCHIVE_* environment variables."""

from pathlib import Path
from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MAILROOM_ARCHIVE_")

    cassandra_hosts: Annotated[list[str], NoDecode] = ["127.0.0.1"]
    cassandra_username: str | None = None
    cassandra_password: str | None = None
    keyspace: str = "archive"
    token: str = ""
    read_token: str = ""
    data_dir: Path = Path("./data")
    shards: int = 64
    queue_size: int = 2000

    @field_validator("cassandra_hosts", mode="before")
    @classmethod
    def _split_hosts(cls, v):
        return [h.strip() for h in v.split(",") if h.strip()] if isinstance(v, str) else v

    @property
    def spool_path(self) -> Path:
        return self.data_dir / "spool.db"

    @property
    def blob_dir(self) -> Path:
        return self.data_dir / "blobs"
