"""HTTPS + WebSocket relay for browser-based LAN intercom clients."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import getpass
import hmac
import json
import os
from pathlib import Path
import ssl
import time
from typing import Iterable

from aiohttp import WSMsgType, web

from .certs import ensure_self_signed_cert, local_ipv4_addresses
from .metrics import WebIntercomMetrics


DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8443
MAX_AUDIO_FRAME_BYTES = 64_000
AUDIO_PACKET_MAGIC = b"SWI1"
AUDIO_PACKET_HEADER_BYTES = 20
STATIC_DIR_KEY = web.AppKey("static_dir", Path)


@dataclass
class Client:
    websocket: web.WebSocketResponse
    client_id: str
    name: str
    room: str
    joined_at: float


class WebIntercomServer:
    def __init__(self, room_key: str):
        if not room_key:
            raise ValueError("room key cannot be empty")
        self.room_key = room_key
        self.clients: dict[web.WebSocketResponse, Client] = {}
        self.lock = asyncio.Lock()
        self.metrics = WebIntercomMetrics()

    async def index(self, request: web.Request) -> web.Response:
        return web.FileResponse(request.app[STATIC_DIR_KEY] / "index.html")

    async def websocket(self, request: web.Request) -> web.WebSocketResponse:
        websocket = web.WebSocketResponse(max_msg_size=MAX_AUDIO_FRAME_BYTES + 4096)
        await websocket.prepare(request)

        client: Client | None = None
        try:
            join_message = await websocket.receive(timeout=15)
            if join_message.type != WSMsgType.TEXT:
                self.metrics.inc("invalid_messages")
                await websocket.close(code=4000, message=b"join message required")
                return websocket

            try:
                payload = json.loads(join_message.data)
            except json.JSONDecodeError:
                self.metrics.inc("invalid_messages")
                await websocket.close(code=4001, message=b"invalid join json")
                return websocket

            if payload.get("type") != "join":
                self.metrics.inc("invalid_messages")
                await websocket.close(code=4002, message=b"join type required")
                return websocket

            provided_key = str(payload.get("key") or "")
            if not hmac.compare_digest(provided_key, self.room_key):
                self.metrics.inc("auth_failures")
                await websocket.send_json({"type": "error", "message": "Invalid room key."})
                await websocket.close(code=4003, message=b"invalid room key")
                return websocket

            name = str(payload.get("name") or "guest")[:40]
            room = str(payload.get("room") or "main")[:40]
            client_id = self.metrics.next_client_id()
            joined_at = time.monotonic()
            client = Client(websocket=websocket, client_id=client_id, name=name, room=room, joined_at=joined_at)
            async with self.lock:
                self.clients[websocket] = client
            self.metrics.record_join(client_id, name, room, joined_at)
            await websocket.send_json({"type": "joined", "name": name, "room": room})
            await self.broadcast_presence(room)
            print(f"Joined {name} in room {room}")

            async for message in websocket:
                if message.type == WSMsgType.BINARY:
                    await self.handle_audio(client, message.data)
                elif message.type == WSMsgType.TEXT:
                    await self.handle_text(client, message.data)
                elif message.type == WSMsgType.ERROR:
                    break
        except asyncio.TimeoutError:
            self.metrics.inc("invalid_messages")
            await websocket.close(code=4004, message=b"join timeout")
        finally:
            if client is not None:
                async with self.lock:
                    self.clients.pop(websocket, None)
                self.metrics.record_leave(client.client_id)
                await self.broadcast_presence(client.room)
                print(f"Left {client.name} from room {client.room}")
        return websocket

    async def handle_audio(self, sender: Client, data: bytes) -> None:
        if not data or len(data) > MAX_AUDIO_FRAME_BYTES:
            self.metrics.inc("dropped_audio_frames")
            return
        payload_size = audio_payload_size(data)
        if payload_size is None:
            self.metrics.inc("dropped_audio_frames")
            return
        self.metrics.record_audio_in(len(data), payload_size)
        recipients = await self.room_recipients(sender.room, exclude=sender.websocket)
        for recipient in recipients:
            try:
                await recipient.send_bytes(data)
                self.metrics.record_audio_relay(len(data), payload_size)
            except ConnectionError:
                continue

    async def handle_text(self, sender: Client, data: str) -> None:
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            self.metrics.inc("invalid_messages")
            return
        if payload.get("type") == "ping":
            await sender.websocket.send_json({"type": "pong", "time": time.time()})
        elif payload.get("type") == "qos_ping":
            await sender.websocket.send_json(
                {
                    "type": "qos_pong",
                    "client_time_ms": payload.get("client_time_ms"),
                    "server_time": time.time(),
                }
            )
        elif payload.get("type") == "metrics":
            metrics = payload.get("metrics")
            if isinstance(metrics, dict):
                self.metrics.record_client_metrics(sender.client_id, metrics)
            else:
                self.metrics.inc("invalid_messages")
        else:
            self.metrics.inc("invalid_messages")

    async def room_recipients(
        self,
        room: str,
        exclude: web.WebSocketResponse | None = None,
    ) -> list[web.WebSocketResponse]:
        async with self.lock:
            return [
                client.websocket
                for websocket, client in self.clients.items()
                if client.room == room and websocket is not exclude
            ]

    async def broadcast_presence(self, room: str) -> None:
        async with self.lock:
            clients = [client for client in self.clients.values() if client.room == room]
        names = sorted(client.name for client in clients)
        message = {"type": "presence", "room": room, "count": len(names), "clients": names}
        for client in clients:
            try:
                await client.websocket.send_json(message)
            except ConnectionError:
                continue

    async def stats_loop(self, interval: float, metrics_xlsx: str | None) -> None:
        while True:
            await asyncio.sleep(interval)
            async with self.lock:
                active_clients = [
                    (client.client_id, client.name, client.room)
                    for client in self.clients.values()
                ]
            row = self.metrics.sample(active_clients)
            if metrics_xlsx:
                try:
                    await asyncio.to_thread(self.metrics.write_workbook, metrics_xlsx)
                except PermissionError:
                    print(
                        f"[metrics] Could not update {metrics_xlsx}; close it in Excel and the next interval will retry.",
                        flush=True,
                    )
            print(
                "[server stats] "
                f"clients={row['active_clients']} "
                f"rx_audio_packets={row['rx_audio_packets']} "
                f"relayed_packets={row['relayed_packets']} "
                f"rx_kbps={row['avg_rx_kbps']} "
                f"relayed_kbps={row['avg_relayed_kbps']} "
                f"xlsx={metrics_xlsx or 'off'}",
                flush=True,
            )


def audio_payload_size(data: bytes) -> int | None:
    if len(data) <= AUDIO_PACKET_HEADER_BYTES:
        return None
    if data[: len(AUDIO_PACKET_MAGIC)] != AUDIO_PACKET_MAGIC:
        return None
    return len(data) - AUDIO_PACKET_HEADER_BYTES


def load_room_key(value: str | None) -> str:
    key = value or os.environ.get("WEB_INTERCOM_KEY")
    if key is None:
        key = getpass.getpass("Web intercom room key: ")
    if not key:
        raise ValueError("room key cannot be empty")
    return key


def create_app(
    room_key: str,
    static_dir: Path,
    stats_interval: float,
    metrics_xlsx: str | None = None,
) -> web.Application:
    relay = WebIntercomServer(room_key)
    app = web.Application()
    app[STATIC_DIR_KEY] = static_dir
    app.router.add_get("/", relay.index)
    app.router.add_get("/ws", relay.websocket)
    app.router.add_static("/static", static_dir, show_index=False)

    async def on_startup(_app: web.Application) -> None:
        if stats_interval > 0 or metrics_xlsx:
            interval = stats_interval if stats_interval > 0 else 5.0
            _app["stats_task"] = asyncio.create_task(relay.stats_loop(interval, metrics_xlsx))

    async def on_cleanup(_app: web.Application) -> None:
        task = _app.get("stats_task")
        if task is not None:
            task.cancel()

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the browser-based secure LAN intercom server.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host/IP to bind. Default: %(default)s")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="HTTPS port. Default: %(default)s")
    parser.add_argument("--key", help="Room key. Defaults to WEB_INTERCOM_KEY or secure prompt.")
    parser.add_argument("--cert-dir", default="certs", help="Directory for generated self-signed HTTPS certificate.")
    parser.add_argument("--stats-interval", type=float, default=5.0, help="Print server stats every N seconds.")
    parser.add_argument(
        "--metrics-xlsx",
        help="Write processed measurement data to this Excel workbook.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    room_key = load_room_key(args.key)
    project_dir = Path(__file__).resolve().parents[1]
    static_dir = project_dir / "static"
    cert_hosts = ["localhost", *local_ipv4_addresses()]
    cert_path, key_path = ensure_self_signed_cert(project_dir / args.cert_dir, cert_hosts)

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(cert_path, key_path)
    app = create_app(room_key, static_dir, args.stats_interval, args.metrics_xlsx)

    urls = [f"https://localhost:{args.port}"]
    urls.extend(f"https://{address}:{args.port}" for address in local_ipv4_addresses() if not address.startswith("127."))
    print("Open this URL on client browsers:")
    for url in urls:
        print(f"  {url}")
    print("The first browser visit will show a self-signed certificate warning. Continue to the site.")
    web.run_app(app, host=args.host, port=args.port, ssl_context=ssl_context)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
