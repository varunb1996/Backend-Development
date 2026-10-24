import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from app import db
from app.config import get_settings
from app.processor import TransactionProcessor
from app.schemas import (
    HealthResponse,
    TransactionOut,
    TransactionWebhook,
    WebhookAck,
    to_iso_z,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    # Keep the request pool fully warm so a webhook never waits on a new TLS handshake.
    api_pool = await db.create_pool(settings, settings.api_pool_size, settings.api_pool_size)
    worker_pool = await db.create_pool(settings, 1, settings.worker_pool_size)
    await db.init_schema(api_pool)
    processor = TransactionProcessor(worker_pool, settings)
    processor.start()
    app.state.pool = api_pool
    app.state.processor = processor
    logger.info(
        "service started (processing delay %.1fs, lease %.1fs)",
        settings.processing_delay_seconds,
        settings.lease_seconds,
    )
    try:
        yield
    finally:
        await processor.stop()
        await worker_pool.close()
        await api_pool.close()


app = FastAPI(
    title="Transaction Webhook Service",
    description="Receives payment transaction webhooks, acknowledges immediately and "
    "processes them reliably in the background.",
    version="1.0.0",
    lifespan=lifespan,
)


class ProcessTimeMiddleware:
    """Adds an X-Process-Time-Ms header with server-side handling time.

    Written as plain ASGI middleware: Starlette's @app.middleware("http") wraps every
    request in extra tasks and streams, which is noticeable on small CPU allocations.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()

        async def send_with_timing(message):
            if message["type"] == "http.response.start":
                elapsed = f"{(time.perf_counter() - started) * 1000:.1f}".encode()
                message["headers"] = [*message.get("headers", []), (b"x-process-time-ms", elapsed)]
            await send(message)

        await self.app(scope, receive, send_with_timing)


app.add_middleware(ProcessTimeMiddleware)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


@app.get("/", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(status="HEALTHY", current_time=to_iso_z(datetime.now(timezone.utc)))


@app.post(
    "/v1/webhooks/transactions",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=WebhookAck,
)
async def receive_transaction_webhook(payload: TransactionWebhook, request: Request) -> JSONResponse:
    # Persist first so the transaction survives a crash, then hand off to the
    # background processor. The response never waits for processing.
    is_new = await db.insert_if_new(request.app.state.pool, payload)
    if is_new:
        request.app.state.processor.submit(payload.transaction_id)
    else:
        logger.info("duplicate webhook ignored for %s", payload.transaction_id)
    # Returned as a ready-made response (shape documented by WebhookAck) to skip
    # response-model validation on the hot path.
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={
            "message": "Webhook accepted",
            "transaction_id": payload.transaction_id,
            "duplicate": not is_new,
        },
    )


@app.get("/v1/transactions/{transaction_id}", response_model=list[TransactionOut])
async def get_transaction(transaction_id: str, request: Request) -> list[TransactionOut]:
    row = await db.get_transaction(request.app.state.pool, transaction_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return [TransactionOut(**dict(row))]
