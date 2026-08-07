"""Shared Postgres container fixtures for tests/integration/.

Manages the container directly via the `docker` CLI rather than
testcontainers' PostgresContainer: testcontainers' Ryuk reaper cannot reach
published ports under rootless Docker in this environment, while a plain
`docker run -p` mapping works — as long as the published port isn't in the
kernel's ephemeral range (/proc/sys/net/ipv4/ip_local_port_range,
32768-60999 here), which the rootless port forwarder cannot reliably serve.
"""

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import AsyncGenerator, Generator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

REPO_ROOT = Path(__file__).resolve().parents[2]
PORT_RANGE_START = 15000
PORT_RANGE_END = 32000
PORT_ATTEMPTS = 12
READY_LOG_MARKER = "database system is ready to accept connections"


def _container_name(suffix: str) -> str:
    """Per-process, per-fixture name so concurrent pytest runs (e.g. `make
    coverage` overlapping another run) don't remove each other's container."""
    return f"teyca-sync-it-pg-{suffix}-{os.getpid()}"


def _candidate_ports(seed: int) -> list[int]:
    """Ports to try, spread by pid+seed so concurrent runs rarely collide."""
    span = PORT_RANGE_END - PORT_RANGE_START
    first = (os.getpid() * 97 + seed * 733) % span
    return [PORT_RANGE_START + (first + step * 137) % span for step in range(PORT_ATTEMPTS)]


def _server_truly_ready(container_name: str, *, timeout_seconds: float = 30.0) -> bool:
    """Wait for Postgres to be ready for real, external connections.

    The official image logs "database system is ready to accept
    connections" once after initdb, then restarts internally, then logs
    it again once the server that actually serves external clients is
    up. `pg_isready`/a bare TCP connect can succeed during that first,
    short-lived window — the connection then resets mid-handshake right
    after. Waiting for the marker twice avoids that race.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        logs = subprocess.run(
            ["docker", "logs", container_name], capture_output=True, text=True
        )
        if (logs.stdout + logs.stderr).count(READY_LOG_MARKER) >= 2:
            return True
        time.sleep(0.25)
    return False


def _start_postgres(container_name: str, host_port: int) -> bool:
    """Start the container on `host_port`; True if it came up and is reachable."""
    started = subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "-e",
            "POSTGRES_PASSWORD=test",
            "-e",
            "POSTGRES_DB=test",
            "-p",
            f"{host_port}:5432",
            "postgres:17-alpine",
        ],
        capture_output=True,
    )
    if started.returncode != 0:
        return False

    return _server_truly_ready(container_name)


def _postgres_url_fixture(*, seed: int, suffix: str) -> Generator[str]:
    container_name = _container_name(suffix)
    host_port: int | None = None
    for candidate in _candidate_ports(seed):
        if _start_postgres(container_name, candidate):
            host_port = candidate
            break
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, check=False)
    if host_port is None:
        raise RuntimeError(
            f"could not start postgres on any of {PORT_ATTEMPTS} candidate ports "
            f"in {PORT_RANGE_START}-{PORT_RANGE_END}"
        )

    try:
        # ssl=disable: asyncpg's SSLRequest probe gets reset by this
        # sandbox's docker networking, so skip it — this is a local,
        # unencrypted-by-design test container anyway.
        url = f"postgresql+asyncpg://postgres:test@localhost:{host_port}/test?ssl=disable"
        os.environ["DATABASE_URL"] = url
        config = Config(str(REPO_ROOT / "alembic.ini"))
        command.upgrade(config, "head")
        yield url
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, check=False)


@pytest.fixture(scope="module")
def postgres_url() -> Generator[str]:
    yield from _postgres_url_fixture(seed=1, suffix="main")


@pytest.fixture
async def engine(postgres_url: str) -> AsyncGenerator[AsyncEngine]:
    engine = create_async_engine(postgres_url)
    yield engine
    await engine.dispose()
