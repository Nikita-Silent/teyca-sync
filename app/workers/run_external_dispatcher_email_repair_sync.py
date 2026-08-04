"""Run one iteration of the duplicate-email-repair Teyca dispatcher (teyca-sync-y1c)."""

from app.workers.external_dispatcher_worker import EMAIL_REPAIR_SYNC_OUTBOX_OPERATIONS
from app.workers.run_external_dispatcher import run_main


def main() -> None:
    run_main(
        service_name="external-dispatcher-email-repair-sync",
        operations=EMAIL_REPAIR_SYNC_OUTBOX_OPERATIONS,
    )


if __name__ == "__main__":
    main()
