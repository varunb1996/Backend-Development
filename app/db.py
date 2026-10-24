from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg

from app.config import Settings
from app.schemas import TransactionWebhook

STATUS_PROCESSING = "PROCESSING"
STATUS_PROCESSED = "PROCESSED"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      TEXT PRIMARY KEY,
    source_account      TEXT NOT NULL,
    destination_account TEXT NOT NULL,
    amount              NUMERIC(18, 2) NOT NULL,
    currency            CHAR(3) NOT NULL,
    status              TEXT NOT NULL DEFAULT 'PROCESSING'
                        CHECK (status IN ('PROCESSING', 'PROCESSED')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at        TIMESTAMPTZ,
    attempts            INTEGER NOT NULL DEFAULT 0,
    locked_until        TIMESTAMPTZ,
    last_error          TEXT
);

CREATE INDEX IF NOT EXISTS idx_transactions_pending
    ON transactions (created_at)
    WHERE status = 'PROCESSING';
"""

PUBLIC_COLUMNS = """
    transaction_id, source_account, destination_account, amount, currency,
    status, created_at, processed_at
"""


def _normalize_dsn(dsn: str) -> str:
    """Drop libpq-only query params that asyncpg would forward to the server as settings.

    Neon's copy-paste connection strings include `channel_binding=require`.
    """
    parts = urlsplit(dsn)
    query = [(k, v) for k, v in parse_qsl(parts.query) if k != "channel_binding"]
    return urlunsplit(parts._replace(query=urlencode(query)))


async def create_pool(settings: Settings, min_size: int, max_size: int) -> asyncpg.Pool:
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return await asyncpg.create_pool(
        dsn=_normalize_dsn(settings.database_url),
        min_size=min_size,
        max_size=max_size,
        # Neon's pooled endpoint runs PgBouncer in transaction mode, which does not
        # support server-side prepared statements across transactions.
        statement_cache_size=0,
        command_timeout=10,
    )


async def init_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


async def insert_if_new(pool: asyncpg.Pool, txn: TransactionWebhook) -> bool:
    """Store a webhook. Returns True if this is the first time we've seen the transaction_id.

    The primary key makes this atomic: concurrent duplicates race on the same row and
    exactly one INSERT wins.
    """
    row = await pool.fetchrow(
        """
        INSERT INTO transactions
            (transaction_id, source_account, destination_account, amount, currency, status)
        VALUES ($1, $2, $3, $4, $5, 'PROCESSING')
        ON CONFLICT (transaction_id) DO NOTHING
        RETURNING transaction_id
        """,
        txn.transaction_id,
        txn.source_account,
        txn.destination_account,
        txn.amount,
        txn.currency,
    )
    return row is not None


async def get_transaction(pool: asyncpg.Pool, transaction_id: str) -> Optional[asyncpg.Record]:
    return await pool.fetchrow(
        f"SELECT {PUBLIC_COLUMNS} FROM transactions WHERE transaction_id = $1",
        transaction_id,
    )


async def claim(pool: asyncpg.Pool, transaction_id: str, lease_seconds: float) -> bool:
    """Take a time-limited lease on a pending transaction.

    Only one worker can hold an unexpired lease, so a transaction is never processed
    concurrently. If the worker dies, the lease expires and the sweeper retries it.
    """
    row = await pool.fetchrow(
        """
        UPDATE transactions
           SET locked_until = now() + make_interval(secs => $2),
               attempts = attempts + 1
         WHERE transaction_id = $1
           AND status = 'PROCESSING'
           AND (locked_until IS NULL OR locked_until < now())
        RETURNING transaction_id
        """,
        transaction_id,
        lease_seconds,
    )
    return row is not None


async def mark_processed(pool: asyncpg.Pool, transaction_id: str) -> bool:
    row = await pool.fetchrow(
        """
        UPDATE transactions
           SET status = 'PROCESSED',
               processed_at = now(),
               locked_until = NULL,
               last_error = NULL
         WHERE transaction_id = $1
           AND status = 'PROCESSING'
        RETURNING transaction_id
        """,
        transaction_id,
    )
    return row is not None


async def release_after_failure(
    pool: asyncpg.Pool, transaction_id: str, error: str, backoff_seconds: float
) -> None:
    await pool.execute(
        """
        UPDATE transactions
           SET locked_until = now() + make_interval(secs => $3),
               last_error = $2
         WHERE transaction_id = $1
           AND status = 'PROCESSING'
        """,
        transaction_id,
        error[:1000],
        backoff_seconds,
    )


async def find_unclaimed(pool: asyncpg.Pool, limit: int = 100) -> list[str]:
    """Pending transactions with no live lease: never started, or whose worker died."""
    rows = await pool.fetch(
        """
        SELECT transaction_id
          FROM transactions
         WHERE status = 'PROCESSING'
           AND (locked_until IS NULL OR locked_until < now())
         ORDER BY created_at
         LIMIT $1
        """,
        limit,
    )
    return [r["transaction_id"] for r in rows]
