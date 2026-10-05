import asyncio
import json
import logging
import random
from datetime import datetime

from aiohttp import web, WSMsgType


HOST = "0.0.0.0"
PORT = 8080

clients = set()
sse_clients = set()


# ------------------------------------------------------------
# Logging
# ------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SERVER] %(message)s",
)

log = logging.getLogger("server")


# ------------------------------------------------------------
# Utility
# ------------------------------------------------------------

def now():
    return datetime.now().strftime("%H:%M:%S")


async def broadcast(message):
    dead = []

    for ws in clients:
        try:
            await ws.send_str(json.dumps(message))
        except Exception:
            dead.append(ws)

    for ws in dead:
        clients.discard(ws)


async def sse_broadcast(message):
    dead = []

    data = f"data: {json.dumps(message)}\n\n"

    for response in sse_clients:
        try:
            await response.write(data.encode())
        except Exception:
            dead.append(response)

    for response in dead:
        sse_clients.discard(response)


# ------------------------------------------------------------
# WebSocket
# ------------------------------------------------------------

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    clients.add(ws)

    peer = request.remote

    log.info("WebSocket connected: %s", peer)

    try:
        async for msg in ws:

            if msg.type == WSMsgType.TEXT:

                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue

                log.info(
                    "WS received: %s",
                    data,
                )

                # Relay WebSocket messages.
                await broadcast(data)

            elif msg.type == WSMsgType.ERROR:
                log.error(
                    "WebSocket error: %s",
                    ws.exception(),
                )

    finally:
        clients.discard(ws)
        log.info("WebSocket disconnected: %s", peer)

    return ws


# ------------------------------------------------------------
# HTTP chunked message receiver
# ------------------------------------------------------------

async def chunked_message_handler(request):
    """
    Receives an HTTP request whose body is delivered using
    HTTP chunked transfer encoding.

    The streamer deliberately sends the 20-character message
    in several chunks.
    """

    user = request.query.get("user", "unknown")

    log.info(
        "HTTP chunked stream opened from %s",
        user,
    )

    parts = []

    try:
        async for chunk in request.content.iter_chunked(4):

            text = chunk.decode()

            parts.append(text)

            log.info(
                "chunk from %s: %r",
                user,
                text,
            )

            # Artificially allow chunks to arrive independently.
            await asyncio.sleep(0.03)

    except Exception as exc:

        log.error(
            "chunked receiver error: %s",
            exc,
        )

        return web.Response(
            status=500,
            text="chunked receive failed",
        )

    message = "".join(parts)

    payload = {
        "type": "chat",
        "user": user,
        "message": message,
        "time": now(),
    }

    log.info(
        "COMPLETE HTTP MESSAGE [%s]: %s",
        user,
        message,
    )

    # Send completed message to both WebSocket users.
    await broadcast(payload)

    return web.Response(
        text="message accepted\n",
    )


# ------------------------------------------------------------
# SSE
# ------------------------------------------------------------

async def sse_handler(request):

    response = web.StreamResponse(
        status=200,
        reason="OK",
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

    await response.prepare(request)

    sse_clients.add(response)

    log.info(
        "SSE client connected: %s",
        request.remote,
    )

    try:

        # Initial event.
        await response.write(
            (
                "event: connected\n"
                "data: {\"message\":\"thinking stream connected\"}\n\n"
            ).encode()
        )

        while True:
            await asyncio.sleep(3600)

    except (asyncio.CancelledError, ConnectionResetError):
        pass

    finally:
        sse_clients.discard(response)

        log.info(
            "SSE client disconnected: %s",
            request.remote,
        )

    return response


# ------------------------------------------------------------
# Pseudo thinking generator
# ------------------------------------------------------------

THINKING_MESSAGES = [
    "thinking...",
    "processing...",
    "checking context...",
    "considering next response...",
    "matching message...",
    "waiting for another thought...",
    "generating response...",
]


async def thinking_loop():

    while True:

        # Random delay between thoughts.
        delay = random.randint(2, 7)

        await asyncio.sleep(delay)

        message = random.choice(THINKING_MESSAGES)

        payload = {
            "type": "thinking",
            "message": message,
            "time": now(),
        }

        log.info(
            "SSE THINKING: %s",
            message,
        )

        await sse_broadcast(payload)


# ------------------------------------------------------------
# Status
# ------------------------------------------------------------

async def status_handler(request):

    return web.json_response(
        {
            "websocket_clients": len(clients),
            "sse_clients": len(sse_clients),
        }
    )


# ------------------------------------------------------------
# Application
# ------------------------------------------------------------

async def start_background_tasks(app):

    app["thinking_task"] = asyncio.create_task(
        thinking_loop()
    )


async def stop_background_tasks(app):

    app["thinking_task"].cancel()

    try:
        await app["thinking_task"]
    except asyncio.CancelledError:
        pass


app = web.Application()

app.router.add_get(
    "/ws",
    websocket_handler,
)

app.router.add_post(
    "/stream",
    chunked_message_handler,
)

app.router.add_get(
    "/thinking",
    sse_handler,
)

app.router.add_get(
    "/status",
    status_handler,
)

app.on_startup.append(start_background_tasks)
app.on_cleanup.append(stop_background_tasks)


if __name__ == "__main__":

    log.info(
        "Starting server on %s:%s",
        HOST,
        PORT,
    )

    web.run_app(
        app,
        host=HOST,
        port=PORT,
    )
