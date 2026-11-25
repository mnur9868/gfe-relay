#!/usr/bin/env python3
import hmac
import logging
import os
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RELAY] %(levelname)s: %(message)s",
)
log = logging.getLogger("gfe-relay")

AUTH_TOKEN = os.environ.get("RELAY_TOKEN", "GANTI_TOKEN_RAHASIA_INI_2026")
# ROOMS[cid][role][lane] = web.WebSocketResponse
ROOMS: dict[str, dict[str, dict[str, web.WebSocketResponse]]] = {}


def get_peer_socket(cid: str, peer_role: str, preferred_lane: str) -> web.WebSocketResponse | None:
    lanes = ROOMS.get(cid, {}).get(peer_role, {})
    ws = lanes.get(preferred_lane)
    if ws is not None and not ws.closed:
        return ws
    for candidate in lanes.values():
        if candidate is not None and not candidate.closed:
            return candidate
    return None


async def health_handler(request: web.Request) -> web.Response:
    return web.Response(text="OK", status=200)


async def ws_handler(request: web.Request) -> web.StreamResponse:
    token = request.query.get("token", "")
    if not hmac.compare_digest(token, AUTH_TOKEN):
        return web.Response(text="Unauthorized", status=401)

    role = request.query.get("role", "").strip().lower()
    if role not in ("client", "vps"):
        return web.Response(text="Invalid role", status=400)

    cid = request.query.get("cid", "default").strip() or "default"
    lane = request.query.get("lane", "0").strip() or "0"
    peer_role = "vps" if role == "client" else "client"

    ws = web.WebSocketResponse(heartbeat=15.0, max_msg_size=1024 * 1024)
    await ws.prepare(request)

    room = ROOMS.setdefault(cid, {"client": {}, "vps": {}})
    old_ws = room[role].get(lane)
    if old_ws is not None and not old_ws.closed:
        await old_ws.close(code=4000, message=b"Replaced by new lane connection")
    room[role][lane] = ws

    log.info(
        "CONNECTED cid=%s role=%s lane=%s (active_client=%d, active_vps=%d)",
        cid,
        role,
        lane,
        len(room["client"]),
        len(room["vps"]),
    )

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.BINARY:
                peer_ws = get_peer_socket(cid, peer_role, lane)
                if peer_ws is not None:
                    try:
                        await peer_ws.send_bytes(msg.data)
                    except Exception:
                        pass
            elif msg.type == web.WSMsgType.ERROR:
                log.warning("WS error cid=%s role=%s lane=%s: %s", cid, role, lane, ws.exception())
                break
    finally:
        if room[role].get(lane) is ws:
            del room[role][lane]
        if not room["client"] and not room["vps"]:
            ROOMS.pop(cid, None)
        log.info("DISCONNECTED cid=%s role=%s lane=%s", cid, role, lane)

    return ws


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/ws", ws_handler)
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    web.run_app(create_app(), host="0.0.0.0", port=port, access_log=None)
