import asyncio
import logging

import asyncpg

from app import db
from app.config import Settings

logger = logging.getLogger("processor")


class TransactionProcessor:
    """Runs transaction processing in the background of the web process.

    Postgres is the source of truth (a durable queue): every accepted webhook is a
    PROCESSING row before we respond. Work is triggered immediately after the insert,
    and a periodic sweeper re-drives anything left behind (crash, restart, transient
    DB error), so no accepted transaction is lost.
    """

    def __init__(self, pool: asyncpg.Pool, settings: Settings):
        self.pool = pool
        self.settings = settings
        self._tasks: dict[str, asyncio.Task] = {}
        self._sweeper: asyncio.Task | None = None

    def submit(self, transaction_id: str) -> None:
        """Schedule processing without blocking the caller. No-op if already running here."""
        if transaction_id in self._tasks:
            return
        task = asyncio.create_task(self._process(transaction_id), name=f"txn:{transaction_id}")
        self._tasks[transaction_id] = task
        task.add_done_callback(lambda _t: self._tasks.pop(transaction_id, None))

    async def _process(self, transaction_id: str) -> None:
        try:
            if not await db.claim(self.pool, transaction_id, self.settings.lease_seconds):
                # Already processed, or another worker currently holds the lease.
                return
            logger.info("processing %s", transaction_id)
            await self._call_external_services(transaction_id)
            if await db.mark_processed(self.pool, transaction_id):
                logger.info("processed %s", transaction_id)
        except asyncio.CancelledError:
            # Shutting down mid-processing: the lease will expire and the sweeper
            # (here or on the next instance) will pick it up again.
            raise
        except Exception as exc:
            logger.exception("processing failed for %s", transaction_id)
            try:
                await db.release_after_failure(
                    self.pool, transaction_id, repr(exc), self.settings.retry_backoff_seconds
                )
            except Exception:
                logger.exception("could not record failure for %s", transaction_id)

    async def _call_external_services(self, transaction_id: str) -> None:
        # Simulates calls to downstream systems (fraud checks, ledger, bank APIs...).
        await asyncio.sleep(self.settings.processing_delay_seconds)

    async def sweep_once(self) -> int:
        pending = await db.find_unclaimed(self.pool)
        for transaction_id in pending:
            self.submit(transaction_id)
        return len(pending)

    async def _sweep_forever(self) -> None:
        while True:
            try:
                count = await self.sweep_once()
                if count:
                    logger.info("sweeper re-queued %d pending transaction(s)", count)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("sweeper iteration failed")
            await asyncio.sleep(self.settings.sweep_interval_seconds)

    def start(self) -> None:
        self._sweeper = asyncio.create_task(self._sweep_forever(), name="sweeper")

    async def stop(self) -> None:
        tasks = list(self._tasks.values())
        if self._sweeper:
            tasks.append(self._sweeper)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    @property
    def in_flight(self) -> int:
        return len(self._tasks)
