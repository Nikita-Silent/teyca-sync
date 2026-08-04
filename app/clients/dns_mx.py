"""Domain-level MX record check for email validation level 3.

Caches by domain, not by address (docs/reverse-engineering-plan.md,
section 5): hundreds of domains vs. thousands of addresses, so one DNS
query per new domain is enough. Only NXDOMAIN/no-MX marks a domain
invalid — any resolver/network failure fails open, since a transient
DNS outage must never block a legitimate email.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import dns.asyncresolver
import dns.exception
import dns.resolver
import structlog

logger = structlog.get_logger()


@dataclass(slots=True)
class DomainMxCache:
    """In-memory TTL cache of domain -> has-MX, plus a resolver timeout."""

    ttl_seconds: float = 3600.0
    timeout_seconds: float = 5.0
    _entries: dict[str, tuple[bool, float]] = field(default_factory=dict)

    async def has_mx(self, domain: str) -> bool:
        """Return whether `domain` has at least one MX record.

        Fails open (True) on timeouts, network errors, or any resolver
        error other than NXDOMAIN/NoAnswer, since those mean "could not
        check" rather than "domain does not accept mail".
        """
        normalized_domain = domain.strip().lower()
        cached = self._entries.get(normalized_domain)
        if cached is not None and cached[1] > time.monotonic():
            return cached[0]

        result = await self._resolve(normalized_domain)
        self._entries[normalized_domain] = (result, time.monotonic() + self.ttl_seconds)
        return result

    async def _resolve(self, domain: str) -> bool:
        try:
            answer = await dns.asyncresolver.resolve(
                domain, "MX", lifetime=self.timeout_seconds
            )
            # RFC 7505 null MX ("0 .") is an explicit "does not accept mail".
            return any(str(rdata.exchange) != "." for rdata in answer)
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
            return False
        except dns.exception.DNSException as exc:
            logger.warning(
                "email_domain_mx_check_failed_open",
                domain=domain,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return True


_default_cache = DomainMxCache()


async def has_valid_mx(domain: str) -> bool:
    """Check `domain` for MX records using the process-wide domain cache."""
    return await _default_cache.has_mx(domain)
