"""End-to-end check of a running deployment against the assessment's success criteria.

Usage:
    python scripts/smoke_test.py https://your-service.onrender.com
    python scripts/smoke_test.py http://localhost:8000 --load 100
"""

import argparse
import asyncio
import statistics
import sys
import time
import uuid

import aiohttp

WEBHOOK_PATH = "/v1/webhooks/transactions"


def new_payload(**overrides):
    payload = {
        "transaction_id": f"txn_smoke_{uuid.uuid4().hex[:12]}",
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }
    payload.update(overrides)
    return payload


class Response:
    def __init__(self, status_code, body, text):
        self.status_code = status_code
        self.body = body
        self.text = text

    def json(self):
        return self.body


async def request(client, method, path, **kwargs):
    async with client.request(method, path, **kwargs) as resp:
        text = await resp.text()
        try:
            body = await resp.json(content_type=None)
        except ValueError:
            body = None
        return Response(resp.status, body, text)


async def timed_post(client, payload):
    started = time.perf_counter()
    resp = await request(client, "POST", WEBHOOK_PATH, json=payload)
    return resp, (time.perf_counter() - started) * 1000


async def wait_until_processed(client, transaction_id, timeout):
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        resp = await request(client, "GET", f"/v1/transactions/{transaction_id}")
        if resp.status_code == 200 and resp.json()[0]["status"] == "PROCESSED":
            return resp.json()[0], time.monotonic() - started
        await asyncio.sleep(2)
    raise AssertionError(f"{transaction_id} was not processed within {timeout}s")


def check(condition, message):
    print(("  PASS " if condition else "  FAIL ") + message)
    if not condition:
        raise SystemExit(1)


async def main(base_url, load, wait_timeout):
    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(base_url, timeout=timeout, connector=connector) as client:
        print("0. Health check (also wakes up a sleeping free-tier instance)")
        resp = await request(client, "GET", "/")
        check(resp.status_code == 200 and resp.json()["status"] == "HEALTHY", f"GET / -> {resp.text}")

        print("1. Single transaction")
        payload = new_payload()
        resp, ms = await timed_post(client, payload)
        check(resp.status_code == 202, f"webhook returned {resp.status_code} in {ms:.0f}ms")
        check(ms < 500, "acknowledged within 500ms")
        status = (await request(client, "GET", f"/v1/transactions/{payload['transaction_id']}")).json()[0]
        check(status["status"] == "PROCESSING", "status is PROCESSING right after the webhook")
        txn, waited = await wait_until_processed(client, payload["transaction_id"], wait_timeout)
        check(True, f"PROCESSED after ~{waited:.0f}s (created_at={txn['created_at']}, "
                    f"processed_at={txn['processed_at']})")

        print("2. Duplicate prevention")
        payload = new_payload(amount=150.50, currency="USD")
        results = await asyncio.gather(*[timed_post(client, payload) for _ in range(5)])
        check(all(r.status_code == 202 for r, _ in results), "all 5 duplicate webhooks returned 202")
        firsts = sum(not r.json()["duplicate"] for r, _ in results)
        check(firsts == 1, f"exactly one webhook created the transaction (got {firsts})")
        records = (await request(client, "GET", f"/v1/transactions/{payload['transaction_id']}")).json()
        check(len(records) == 1, "query endpoint returns a single transaction")

        print(f"3. Performance: {load} concurrent webhooks")
        results = await asyncio.gather(*[timed_post(client, new_payload()) for _ in range(load)])
        latencies = sorted(ms for _, ms in results)
        check(all(r.status_code == 202 for r, _ in results), f"all {load} returned 202")
        p50 = statistics.median(latencies)
        p95 = latencies[int(len(latencies) * 0.95) - 1]
        print(f"     p50={p50:.0f}ms p95={p95:.0f}ms max={latencies[-1]:.0f}ms "
              "(client-observed, includes network)")

        print("4. Reliability: bad input is rejected cleanly")
        resp = await request(client, "POST", WEBHOOK_PATH, json={"transaction_id": "x"})
        check(resp.status_code == 422, f"malformed payload -> {resp.status_code}")
        resp = await request(client, "GET", "/v1/transactions/txn_does_not_exist")
        check(resp.status_code == 404, f"unknown transaction -> {resp.status_code}")

        print("   waiting for the duplicate-test transaction to finish...")
        await wait_until_processed(client, payload["transaction_id"], wait_timeout)
        check(True, "duplicate-test transaction processed once")

    print("\nAll checks passed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("base_url", nargs="?", default="http://localhost:8000")
    parser.add_argument("--load", type=int, default=50, help="concurrent webhooks for the load test")
    parser.add_argument("--timeout", type=float, default=90, help="seconds to wait for processing")
    args = parser.parse_args()
    try:
        asyncio.run(main(args.base_url.rstrip("/"), args.load, args.timeout))
    except AssertionError as exc:
        print(f"  FAIL {exc}")
        sys.exit(1)
