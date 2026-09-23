import logging
import os
import random
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("proxy-service")

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://localhost:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://localhost:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true").lower() == "true"


def _migration_percent() -> float:
    try:
        return float(os.getenv("MOVIES_MIGRATION_PERCENT", "50"))
    except ValueError:
        return 50.0


EXCLUDED_HEADERS = {
    "host",
    "accept-encoding",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}

client: httpx.AsyncClient = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    async with httpx.AsyncClient(timeout=30.0) as http_client:
        client = http_client
        yield


app = FastAPI(title="CinemaAbyss Proxy Service", lifespan=lifespan)


def _movies_upstream() -> str:
    if not GRADUAL_MIGRATION:
        return MOVIES_SERVICE_URL
    if random.random() * 100 < _migration_percent():
        return MOVIES_SERVICE_URL
    return MONOLITH_URL


def _forwarded_headers(headers: httpx.Headers) -> dict:
    return {
        key: value
        for key, value in headers.items()
        if key.lower() not in EXCLUDED_HEADERS
    }


@app.api_route("/health")
async def health(request: Request):
    logger.info("Proxy service is healthy")
    accepts = request.headers.get("accept", "")
    if "application/json" in accepts:
        return JSONResponse({"status": True})
    return Response(content="I am is healthy", media_type="text/plain")


@app.api_route(
    "/api/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
)
async def proxy(path: str, request: Request):
    if client is None:
        return JSONResponse({"error": "http client is None"}, status_code=503)

    rest = f"/{path}" if path else ""
    if rest.startswith("/movies"):
        upstream = _movies_upstream()
    elif rest.startswith("/events"):
        upstream = EVENTS_SERVICE_URL
    else:
        upstream = MONOLITH_URL

    target = f"{upstream}/api{rest}"
    if request.url.query:
        target += f"?{request.url.query}"

    body = (
        await request.body()
        if request.method in ("POST", "PUT", "PATCH", "DELETE")
        else None
    )
    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in EXCLUDED_HEADERS
    }

    logger.info("Proxying %s %s -> %s", request.method, request.url.path, target)

    try:
        upstream_response = await client.request(
            method=request.method,
            url=target,
            content=body,
            headers=headers,
        )
    except httpx.HTTPError as exc:
        logger.error("Upstream request failed: %s", exc)
        return JSONResponse(
            {"error": "upstream service unavailable"}, status_code=502
        )

    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers=_forwarded_headers(upstream_response.headers),
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
    )