"""Central typed configuration loader.

All pipeline code imports `settings` from this module instead of reading
`os.environ` directly. Values come from environment variables / a local
`.env` file (see `.env.example` for the full list and defaults).
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    openlake_env: str = Field(default="local")

    source_db_host: str = "localhost"
    source_db_port: int = 5432
    source_db_name: str = "openlake_source"
    source_db_user: str = "openlake"
    source_db_password: str = ""

    warehouse_db_host: str = "localhost"
    warehouse_db_port: int = 5433
    warehouse_db_name: str = "openlake_warehouse"
    warehouse_db_user: str = "openlake"
    warehouse_db_password: str = ""

    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_secure: bool = False
    minio_bucket_landing: str = "openlake-landing"
    minio_bucket_bronze: str = "openlake-bronze"
    minio_bucket_silver: str = "openlake-silver"
    minio_bucket_gold: str = "openlake-gold"

    customer_api_base_url: str = "http://localhost:8080"
    customer_api_key: str = ""

    log_level: str = "INFO"

    @property
    def source_db_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.source_db_user}:{self.source_db_password}"
            f"@{self.source_db_host}:{self.source_db_port}/{self.source_db_name}"
        )

    @property
    def warehouse_db_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.warehouse_db_user}:{self.warehouse_db_password}"
            f"@{self.warehouse_db_host}:{self.warehouse_db_port}/{self.warehouse_db_name}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
