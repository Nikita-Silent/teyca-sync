"""Level 1 (syntax) and level 2 (uniqueness) email validation behavior.

See docs/reverse-engineering-plan.md, section 5, "Что считается
нормальным email": four levels total, this file locks in the two that
are implemented (1: syntax via is_valid_email, 2: uniqueness via the
duplicate-email check consumers run before enqueueing Listmonk sync).
Level 3 (MX/domain plausibility) is not implemented pending an owner
decision; level 4 (bounce) is asynchronous and out of scope here.
"""

from __future__ import annotations

import pytest

from app.consumers.common import is_valid_email


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
