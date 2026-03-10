import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context

from app.config import get_settings
from app.db.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def get_url() -> str:
    """
    Return the database URL from application settings.
    
    Returns:
        database_url (str): The SQLAlchemy database connection URL configured for the application.
    """
    return get_settings().database_url


def run_migrations_offline() -> None:
    """
    Run Alembic migrations in offline mode using the configured database URL.
    
    Configures the Alembic migration context to emit SQL statements (with literal-bound parameters and named parameter style) and executes the migration scripts inside a transaction.
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """
    Run Alembic migrations using the given SQLAlchemy connection.
    
    Parameters:
        connection (Connection): An active SQLAlchemy connection to use for configuring the Alembic context and executing migrations within a transaction.
    """
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async() -> None:
    """
    Create an asynchronous SQLAlchemy engine from the Alembic config, run migrations using a database connection, and dispose the engine.
    
    Reads the Alembic configuration, injects the application's database URL, opens an asynchronous connection and executes migrations via do_run_migrations, then disposes the created engine.
    """
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    """
    Execute online database migrations against the configured database using the asynchronous migration workflow.
    
    Starts an asyncio event loop and performs migrations with an async SQLAlchemy engine and connection, applying schema changes defined by the migration environment.
    """
    asyncio.run(run_async())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
