"""Р12/teyca-sync-s5f: consent_pending must mean "needs a Listmonk recheck"
only. It is written from four technical call sites — none of them may be able
to reach bonus accrual or Teyca. The consent bonus is granted from exactly one
place: a successful CRM-driven listmonk_upsert (teyca-sync-4ue, Р4а), in
app.workers.external_dispatcher_worker._accrue_consent_bonus_if_needed.

These tests assert the architectural invariant at the module level: reconcile
(phase 1 + phase 2) and the bulk subscriber-id refresh CLI — which all call
`set_consent_pending`/write `consent_pending=True` directly — must not import
or reference bonus accrual or Teyca clients at all. If someone wires money
into those modules later, this test fails immediately instead of relying on
someone noticing a stray accrual in production, like the 10 659-bonus
incident this rule exists to prevent (see docs/reverse-engineering-plan.md,
section 3 and Р4а).
"""

import ast
from pathlib import Path

import app.workers.listmonk_reconcile_worker as reconcile_module
import app.workers.listmonk_refresh_subscriber_ids as refresh_module

_FORBIDDEN_NAMES = {
    "BonusAccrualRepository",
    "TeycaClient",
    "build_teyca_client",
    "accrue_bonuses",
    "BonusOperation",
}


def _referenced_names(module_path: Path) -> set[str]:
    tree = ast.parse(module_path.read_text())
    return {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)} | {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }


def test_reconcile_worker_never_references_bonus_or_teyca() -> None:
    module_path = Path(reconcile_module.__file__)
    assert _referenced_names(module_path).isdisjoint(_FORBIDDEN_NAMES)


def test_refresh_subscriber_ids_never_references_bonus_or_teyca() -> None:
    module_path = Path(refresh_module.__file__)
    assert _referenced_names(module_path).isdisjoint(_FORBIDDEN_NAMES)


def test_consent_sync_worker_never_references_bonus_or_teyca() -> None:
    """Since teyca-sync-4ue/k5u, consent_sync_worker only tracks unsubscribes —
    it must not touch bonus accrual or Teyca either."""
    import app.workers.consent_sync_worker as consent_sync_module

    module_path = Path(consent_sync_module.__file__)
    assert _referenced_names(module_path).isdisjoint(_FORBIDDEN_NAMES)


def test_external_dispatcher_worker_is_the_sole_bonus_trigger() -> None:
    """Contrast case: this is the one module allowed to accrue the bonus."""
    import app.workers.external_dispatcher_worker as dispatcher_module

    module_path = Path(dispatcher_module.__file__)
    assert "BonusAccrualRepository" in _referenced_names(module_path)
    assert "accrue_bonuses" in _referenced_names(module_path)
