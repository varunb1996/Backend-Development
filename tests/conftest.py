import os

import asyncpg
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

if not TEST_DATABASE_URL:
    pytest.skip("TEST_DATABASE_URL is not set", allow_module_level=True)

# Fast timings so the suite runs in seconds instead of minutes.
os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ["PROCESSING_DELAY_SECONDS"] = "1"
os.environ["LEASE_SECONDS"] = "5"
os.environ["SWEEP_INTERVAL_SECONDS"] = "0.5"
os.environ["RETRY_BACKOFF_SECONDS"] = "0.5"

from app.main import app  # noqa: E402


@pytest_asyncio.fixture
async def client():
    conn = await asyncpg.connect(TEST_DATABASE_URL)
    await conn.execute("DROP TABLE IF EXISTS transactions")
    await conn.close()
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


@pytest_asyncio.fixture
async def raw_db():
    conn = await asyncpg.connect(TEST_DATABASE_URL)
    yield conn
    await conn.close()
