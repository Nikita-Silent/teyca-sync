"""Run one iteration of the merge finalize dispatcher."""

from app.workers.external_dispatcher_worker import MERGE_OUTBOX_OPERATIONS
from app.workers.run_external_dispatcher import run_main


def main() -> None:
    run_main(
        service_name="external-dispatcher-merge",
        operations=MERGE_OUTBOX_OPERATIONS,
    )


if __name__ == "__main__":
    main()
