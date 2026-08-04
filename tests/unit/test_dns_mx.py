"""Level 3 (domain MX record) email validation (teyca-sync-fh6).

See docs/reverse-engineering-plan.md, section 5: catches syntactically
valid junk like 123@mail.ru that level 1 alone lets through, via one
DNS query per new domain (cached by domain, not by address).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import dns.exception
import dns.resolver
import pytest

from app.clients.dns_mx import DomainMxCache


def _mx_answer(*exchanges: str) -> list[SimpleNamespace]:
    return [SimpleNamespace(exchange=exchange) for exchange in exchanges]


@pytest.mark.asyncio
async def test_has_mx_true_when_domain_has_mx_records() -> None:
    cache = DomainMxCache()
    with patch(
        "app.clients.dns_mx.dns.asyncresolver.resolve",
        new=AsyncMock(return_value=_mx_answer("mail.example.com.")),
    ):
        assert await cache.has_mx("example.com") is True


@pytest.mark.asyncio
async def test_has_mx_false_on_nxdomain() -> None:
    cache = DomainMxCache()
    with patch(
        "app.clients.dns_mx.dns.asyncresolver.resolve",
        new=AsyncMock(side_effect=dns.resolver.NXDOMAIN()),
    ):
        assert await cache.has_mx("gmail.con") is False


@pytest.mark.asyncio
async def test_has_mx_false_on_no_answer() -> None:
    cache = DomainMxCache()
    with patch(
        "app.clients.dns_mx.dns.asyncresolver.resolve",
        new=AsyncMock(side_effect=dns.resolver.NoAnswer()),
    ):
        assert await cache.has_mx("example.com") is False


@pytest.mark.asyncio
async def test_has_mx_false_on_rfc7505_null_mx() -> None:
    """A null MX ("0 .") is an explicit RFC 7505 "does not accept mail"."""
    cache = DomainMxCache()
    with patch(
        "app.clients.dns_mx.dns.asyncresolver.resolve",
        new=AsyncMock(return_value=_mx_answer(".")),
    ):
        assert await cache.has_mx("example.com") is False


@pytest.mark.asyncio
async def test_has_mx_fails_open_on_timeout_or_network_error() -> None:
    """A transient DNS/network failure must never block a legitimate email."""
    cache = DomainMxCache()
    with patch(
        "app.clients.dns_mx.dns.asyncresolver.resolve",
        new=AsyncMock(side_effect=dns.exception.Timeout()),
    ):
        assert await cache.has_mx("example.com") is True


@pytest.mark.asyncio
async def test_has_mx_caches_by_domain_and_is_case_insensitive() -> None:
    cache = DomainMxCache(ttl_seconds=3600.0)
    resolve = AsyncMock(return_value=_mx_answer("mail.example.com."))
    with patch("app.clients.dns_mx.dns.asyncresolver.resolve", new=resolve):
        assert await cache.has_mx("Example.com") is True
        assert await cache.has_mx("example.com") is True

    resolve.assert_awaited_once()


@pytest.mark.asyncio
async def test_has_mx_re_resolves_after_ttl_expires() -> None:
    cache = DomainMxCache(ttl_seconds=-1.0)
    resolve = AsyncMock(return_value=_mx_answer("mail.example.com."))
    with patch("app.clients.dns_mx.dns.asyncresolver.resolve", new=resolve):
        assert await cache.has_mx("example.com") is True
        assert await cache.has_mx("example.com") is True

    assert resolve.await_count == 2
