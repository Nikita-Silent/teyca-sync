"""Run one iteration of the consent-unsubscribe Teyca dispatcher (teyca-sync-dd2.1)."""

from app.workers.external_dispatcher_worker import CONSENT_BLOCK_OUTBOX_OPERATIONS
from app.workers.run_external_dispatcher import run_main


def main() -> None:
    run_main(
        service_name="external-dispatcher-consent-block",
        operations=CONSENT_BLOCK_OUTBOX_OPERATIONS,
    )


if __name__ == "__main__":
    main()
