from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str = "postgresql+asyncpg://teyca:teyca@localhost:5432/teyca"

    # RabbitMQ
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # Webhook auth: Teyca sends this value in Authorization header (no JWT)
    webhook_auth_token: str = ""

    # Teyca API (outgoing)
    teyca_base_url: str = "https://api.teyca.ru"
    teyca_api_key: str = ""
    teyca_token: str = ""

    # Old DB (read-only, merge)
    export_db_url: str = ""

    # Listmonk
    listmonk_url: str = ""
    listmonk_user: str = ""
    listmonk_password: str = ""
    listmonk_list_ids: str = ""
    consent_bonus_value: str = "100.0"
    consent_bonus_ttl_days: int = 30
    consent_sync_batch_size: int = 500

    # Optional Loki
    loki_url: str | None = None


def get_settings() -> Settings:
    return Settings()
