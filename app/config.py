from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str = "postgresql+asyncpg://teyca:teyca@localhost:5432/teyca"
    database_pool_size: int = 5
    database_pool_max_overflow: int = 10
    database_pool_timeout_seconds: float = 30.0

    # RabbitMQ
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    rabbitmq_consumer_prefetch_count: int = 4
    rabbitmq_consumer_max_concurrency: int = 4

    # Webhook auth
    webhook_auth_enabled: bool = True
    # Teyca sends this value in Authorization header (no JWT)
    webhook_auth_token: str = ""
    webhook: str = "/webhook"

    # Teyca API (outgoing)
    teyca_base_url: str = "https://api.teyca.ru"
    teyca_api_key: str = ""
    teyca_token: str = ""
    teyca_rate_limit_redis_url: str = ""
    teyca_rate_limit_redis_prefix: str = "teyca-rate-limit"

    # Old DB (read-only, merge)
    export_db_url: str = ""
    export_db_request_timeout_seconds: float = 15.0

    # Listmonk
    listmonk_url: str = ""
    listmonk_user: str = ""
    listmonk_password: str = ""
    listmonk_list_ids: str = ""
    listmonk_request_timeout_seconds: float = 15.0
    listmonk_request_max_retries: int = 2
    listmonk_request_retry_backoff_seconds: float = 0.5
    consent_bonus_amount: str = "100.0"
    consent_bonus_ttl_days: int = 30
    consent_sync_batch_size: int = 500

    # Optional Loki
    loki_url: str | None = None
    loki_username: str | None = None
    loki_password: str | None = None
    log_component: str = "app"


def get_settings() -> Settings:
    return Settings()
