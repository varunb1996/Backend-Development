import asyncio
import time
import uuid

import pytest

from app.main import app

pytestmark = pytest.mark.asyncio


def make_payload(**overrides):
    payload = {
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }
    payload.update(overrides)
    return payload


async def wait_for_status(client, transaction_id, expected, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = await client.get(f"/v1/transactions/{transaction_id}")
        if resp.status_code == 200 and resp.json()[0]["status"] == expected:
            return resp.json()[0]
        await asyncio.sleep(0.2)
    raise AssertionError(f"{transaction_id} did not reach {expected}")


async def test_health(client):
    resp = await client.get("/")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "HEALTHY"
    assert body["current_time"].endswith("Z")


async def test_webhook_accepted_quickly_then_processed(client):
    payload = make_payload()
    started = time.perf_counter()
    resp = await client.post("/v1/webhooks/transactions", json=payload)
    elapsed = time.perf_counter() - started

    assert resp.status_code == 202
    assert resp.json()["duplicate"] is False
    assert elapsed < 0.5

    resp = await client.get(f"/v1/transactions/{payload['transaction_id']}")
    assert resp.status_code == 200
    [txn] = resp.json()
    assert txn["status"] == "PROCESSING"
    assert txn["processed_at"] is None
    assert txn["amount"] == 1500
    assert txn["currency"] == "INR"

    txn = await wait_for_status(client, payload["transaction_id"], "PROCESSED")
    assert txn["processed_at"] is not None
    assert txn["created_at"].endswith("Z") and txn["processed_at"].endswith("Z")


async def test_duplicate_webhooks_processed_once(client, raw_db):
    payload = make_payload(amount=150.50, currency="usd")
    responses = await asyncio.gather(
        *[client.post("/v1/webhooks/transactions", json=payload) for _ in range(10)]
    )
    assert all(r.status_code == 202 for r in responses)
    assert sum(not r.json()["duplicate"] for r in responses) == 1

    txn = await wait_for_status(client, payload["transaction_id"], "PROCESSED")
    assert txn["amount"] == 150.5
    assert txn["currency"] == "USD"

    # A webhook arriving after processing is still accepted and changes nothing.
    late = await client.post("/v1/webhooks/transactions", json=payload)
    assert late.status_code == 202 and late.json()["duplicate"] is True

    rows = await raw_db.fetch(
        "SELECT attempts FROM transactions WHERE transaction_id = $1", payload["transaction_id"]
    )
    assert len(rows) == 1
    assert rows[0]["attempts"] == 1


async def test_many_concurrent_webhooks_stay_fast(client):
    payloads = [make_payload() for _ in range(50)]

    async def timed_post(p):
        started = time.perf_counter()
        r = await client.post("/v1/webhooks/transactions", json=p)
        return r.status_code, time.perf_counter() - started

    results = await asyncio.gather(*[timed_post(p) for p in payloads])
    assert all(code == 202 for code, _ in results)
    assert max(t for _, t in results) < 0.5

    for p in payloads:
        await wait_for_status(client, p["transaction_id"], "PROCESSED")


async def test_unknown_transaction_returns_404(client):
    resp = await client.get("/v1/transactions/txn_does_not_exist")
    assert resp.status_code == 404


@pytest.mark.parametrize(
    "overrides",
    [
        {"amount": -5},
        {"amount": 0},
        {"currency": "RUPEES"},
        {"transaction_id": "   "},
        {"amount": 10.123},
    ],
)
async def test_invalid_payload_rejected(client, overrides):
    resp = await client.post("/v1/webhooks/transactions", json=make_payload(**overrides))
    assert resp.status_code == 422


async def test_missing_field_rejected(client):
    payload = make_payload()
    del payload["destination_account"]
    resp = await client.post("/v1/webhooks/transactions", json=payload)
    assert resp.status_code == 422


async def test_stranded_transaction_is_recovered(client, raw_db):
    # Simulate a crash: a row was accepted but its worker died while holding the lease.
    txn_id = f"txn_{uuid.uuid4().hex[:12]}"
    await raw_db.execute(
        """
        INSERT INTO transactions
            (transaction_id, source_account, destination_account, amount, currency,
             status, attempts, locked_until)
        VALUES ($1, 'a', 'b', 10, 'INR', 'PROCESSING', 1, now() - interval '1 second')
        """,
        txn_id,
    )
    txn = await wait_for_status(client, txn_id, "PROCESSED")
    assert txn["processed_at"] is not None


async def test_failed_attempt_is_retried(client, monkeypatch):
    processor = app.state.processor
    original = processor._call_external_services
    calls = {"n": 0}

    async def flaky(transaction_id):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("upstream timeout")
        await original(transaction_id)

    monkeypatch.setattr(processor, "_call_external_services", flaky)
    payload = make_payload()
    resp = await client.post("/v1/webhooks/transactions", json=payload)
    assert resp.status_code == 202

    await wait_for_status(client, payload["transaction_id"], "PROCESSED")
    assert calls["n"] == 2
