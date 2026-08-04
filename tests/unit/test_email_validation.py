"""Level 1 (syntax), 2 (uniqueness), and 3 (MX record) email validation.

See docs/reverse-engineering-plan.md, section 5, "Что считается
нормальным email": four levels total. This file locks in level 1
(syntax via is_valid_email), level 3's combination with syntax
(is_email_deliverable — MX behavior itself is tested in
test_dns_mx.py), and documents that level 2 (uniqueness) is enforced
by the duplicate-email check consumers run before enqueueing Listmonk
sync. Level 4 (bounce) is asynchronous and out of scope here.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from app.consumers.common import is_email_deliverable, is_valid_email


@pytest.mark.parametrize(
    "email",
    [
        "user@example.com",
        "user.name+tag@example.co.uk",
        "  user@example.com  ",  # trimmed before validation
        "a" * 64 + "@example.com",  # local part at the 64-char limit
        "user@" + "a" * 245 + ".com",  # total length at the 254-char limit
    ],
)
def test_valid_emails_pass_syntax_check(email: str) -> None:
    assert is_valid_email(email) is True


def test_syntactically_valid_but_nonexistent_domain_still_passes_level_1() -> None:
    """Level 1 is form-only; catching junk like 123@mail.ru is level 3+."""
    assert is_valid_email("123@mail.ru") is True


@pytest.mark.asyncio
async def test_is_email_deliverable_passes_when_syntax_and_mx_are_both_valid() -> None:
    with patch(
        "app.consumers.common.has_valid_mx", new=AsyncMock(return_value=True)
    ) as has_valid_mx:
        assert await is_email_deliverable("user@example.com") is True
    has_valid_mx.assert_awaited_once_with("example.com")


@pytest.mark.asyncio
async def test_is_email_deliverable_rejects_junk_domain_without_mx() -> None:
    """Level 3 catches what level 1 alone lets through, e.g. 123@mail.ru."""
    with patch("app.consumers.common.has_valid_mx", new=AsyncMock(return_value=False)):
        assert await is_email_deliverable("123@mail.ru") is False


@pytest.mark.asyncio
async def test_is_email_deliverable_rejects_bad_syntax_without_checking_mx() -> None:
    with patch(
        "app.consumers.common.has_valid_mx", new=AsyncMock(return_value=True)
    ) as has_valid_mx:
        assert await is_email_deliverable("not-an-email") is False
    has_valid_mx.assert_not_awaited()


@pytest.mark.parametrize(
    "email",
    [
        None,
        "",
        "   ",
        "no-at-sign.example.com",
        "two@at@signs.com",
        " user @example.com",
        "user example@example.com",
        "@example.com",
        "user@",
        ".user@example.com",
        "user.@example.com",
        "us..er@example.com",
        "user@ex..ample.com",
        "a" * 65 + "@example.com",  # local part over 64 chars
        "user@" + "a" * 246 + ".com",  # total length over 254 chars
        "user@example",
    ],
)
def test_invalid_emails_fail_syntax_check(email: str | None) -> None:
    assert is_valid_email(email) is False
