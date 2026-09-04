from decimal import Decimal

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

    # Webhook inbox (Postgres queue, replaces RabbitMQ, teyca-sync-8ib)
    webhook_inbox_batch_size: int = 100
    webhook_inbox_poll_interval_seconds: float = 1.0
    webhook_inbox_max_concurrency: int = 4
    webhook_inbox_shutdown_drain_timeout_seconds: float = 30.0
    webhook_inbox_stale_claim_seconds: float = 300.0
    webhook_inbox_retry_base_delay_ms: int = 1_000
    webhook_inbox_retry_max_delay_ms: int = 15 * 60_000
    webhook_inbox_max_retries: int = 25
    webhook_inbox_lock_busy_retry_base_delay_ms: int = 1_000
    webhook_inbox_lock_busy_retry_max_delay_ms: int = 30_000
    webhook_inbox_lock_busy_retry_max_retries: int = 5
    webhook_inbox_teyca_rate_limit_retry_base_delay_ms: int = 60_000
    webhook_inbox_teyca_rate_limit_retry_max_delay_ms: int = 15 * 60_000
    webhook_inbox_teyca_rate_limit_retry_max_retries: int = 10
    external_dispatcher_batch_size: int = 100
    external_dispatcher_retry_base_delay_ms: int = 1_000
    external_dispatcher_retry_max_delay_ms: int = 15 * 60_000
    external_dispatcher_max_retries: int = 25
    external_dispatcher_teyca_rate_limit_max_wait_seconds: float = 0.0
    external_dispatcher_stale_claim_seconds: float = 300.0
    external_dispatcher_poll_interval_seconds: float = 5.0
    consent_sync_interval_seconds: float = 3600.0
    listmonk_reconcile_interval_seconds: float = 300.0
    worker_shutdown_drain_timeout_seconds: float = 60.0

    # Webhook auth
    webhook_auth_enabled: bool = True
    # Teyca sends this value in Authorization header (no JWT)
    webhook_auth_token: str = ""
    webhook: str = "/webhook"
    # teyca-sync-iil.4: ingress accepts any well-formed pass payload (raw body
    # goes to webhook_inbox, schema validation moved to the consumer where a
    # failure is a retryable/inspectable `dead` row instead of a lost 422).
    # This is the only remaining ingress-side guard against abuse.
    webhook_max_body_bytes: int = 1_000_000

    # Teyca API (outgoing)
    teyca_base_url: str = "https://api.teyca.ru"
    teyca_api_key: str = ""
    teyca_token: str = ""
    # Real Teyca limits, outgoing requests only (webhooks don't count). Enforced
    # by a Postgres budget table (teyca_call_budget), not Redis.
    teyca_rate_limit_per_second: int = 5
    teyca_rate_limit_per_minute: int = 50
    teyca_rate_limit_per_hour: int = 500
    teyca_rate_limit_per_day: int = 5000
    teyca_request_max_retries: int = 2
    teyca_request_retry_backoff_seconds: float = 1.0
    teyca_connect_timeout_seconds: float = 5.0
    teyca_read_timeout_seconds: float = 15.0
    teyca_write_timeout_seconds: float = 15.0
    teyca_pool_timeout_seconds: float = 5.0
    teyca_circuit_breaker_failure_threshold: int = 5
    teyca_circuit_breaker_cooldown_seconds: float = 30.0

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
    listmonk_connect_timeout_seconds: float = 5.0
    listmonk_read_timeout_seconds: float = 15.0
    listmonk_write_timeout_seconds: float = 15.0
    listmonk_pool_timeout_seconds: float = 5.0
    listmonk_circuit_breaker_failure_threshold: int = 5
    listmonk_circuit_breaker_cooldown_seconds: float = 30.0
    consent_bonus_amount: Decimal = Decimal("100.0")
    consent_bonus_ttl_days: int = 30
    consent_sync_batch_size: int = 500

    # Optional Loki
    loki_url: str | None = None
    loki_username: str | None = None
    loki_password: str | None = None
    loki_request_timeout_seconds: float = 5.0
    log_component: str = "app"


def get_settings() -> Settings:
    return Settings()
