import asyncio
import time

from openpyxl import load_workbook

from web_intercom.metrics import estimate_e_model
from web_intercom.server import (
    Client,
    WebIntercomServer,
    audio_payload_size,
    audio_stream_id,
    build_arg_parser,
    create_app,
    load_room_key,
)


def test_parser_defaults():
    args = build_arg_parser().parse_args([])

    assert args.host == "0.0.0.0"
    assert args.port == 8443
    assert args.stats_interval == 5.0
    assert args.metrics_xlsx is None


def test_load_room_key_from_value():
    assert load_room_key("secret") == "secret"


def test_room_recipients_excludes_sender():
    relay = WebIntercomServer("secret")
    sender = object()
    receiver = object()
    relay.clients[sender] = type("Client", (), {"websocket": sender, "name": "a", "room": "main"})()
    relay.clients[receiver] = type("Client", (), {"websocket": receiver, "name": "b", "room": "main"})()

    recipients = asyncio.run(relay.room_recipients("main", exclude=sender))

    assert recipients == [receiver]


def test_create_app_routes(tmp_path):
    (tmp_path / "index.html").write_text("<html></html>", encoding="utf-8")
    app = create_app("secret", tmp_path, 0)
    paths = {route.resource.canonical for route in app.router.routes()}

    assert "/" in paths
    assert "/ws" in paths


def test_audio_packet_header_validation():
    assert audio_payload_size(b"SWI1" + b"\x00" * 16 + b"pcm") == 3
    assert audio_payload_size(b"BAD!" + b"\x00" * 16 + b"pcm") is None
    assert audio_payload_size(b"SWI1") is None
    packet = b"SWI1" + (123456).to_bytes(4, "big") + b"\x00" * 12 + b"pcm"
    assert audio_stream_id(packet) == 123456


def test_handle_audio_does_not_block_on_slow_recipient():
    class SlowWebSocket:
        async def send_bytes(self, data: bytes) -> None:
            await asyncio.sleep(1)

    async def run() -> None:
        relay = WebIntercomServer("secret")
        sender_ws = object()
        slow_ws = SlowWebSocket()
        sender = Client(sender_ws, "client-0001", "alice", "main", time.monotonic())
        slow = Client(slow_ws, "client-0002", "bob", "main", time.monotonic())
        relay.clients[sender_ws] = sender
        relay.clients[slow_ws] = slow
        packet = b"SWI1" + b"\x00" * 16 + b"pcm"

        started_at = time.perf_counter()
        await relay.handle_audio(sender, packet)
        elapsed = time.perf_counter() - started_at
        await asyncio.sleep(0.05)
        sample = relay.metrics.sample([])

        assert elapsed < 0.05
        assert sample["rx_audio_packets"] == 1
        assert sample["relay_send_drops"] == 1

    asyncio.run(run())


def test_handle_audio_registers_sender_stream_id():
    async def run() -> None:
        relay = WebIntercomServer("secret")
        sender_ws = object()
        sender = Client(sender_ws, "client-0001", "alice", "main", time.monotonic())
        relay.clients[sender_ws] = sender
        packet = b"SWI1" + (42).to_bytes(4, "big") + b"\x00" * 12 + b"pcm"

        await relay.handle_audio(sender, packet)

        assert sender.stream_ids == {42}

    asyncio.run(run())


def test_handle_audio_drops_when_recipient_send_is_pending():
    class HealthyWebSocket:
        async def send_bytes(self, data: bytes) -> None:
            raise AssertionError("send should not be scheduled while pending")

    async def run() -> None:
        relay = WebIntercomServer("secret")
        sender_ws = object()
        recipient_ws = HealthyWebSocket()
        sender = Client(sender_ws, "client-0001", "alice", "main", time.monotonic())
        sender.stream_ids.add(0)
        recipient = Client(recipient_ws, "client-0002", "bob", "main", time.monotonic())
        recipient.send_pending = True
        relay.clients[sender_ws] = sender
        relay.clients[recipient_ws] = recipient
        packet = b"SWI1" + b"\x00" * 16 + b"pcm"

        await relay.handle_audio(sender, packet)
        sample = relay.metrics.sample([])

        assert len(relay.relay_tasks) == 0
        assert sample["rx_audio_packets"] == 1
        assert sample["relay_send_drops"] == 1

    asyncio.run(run())


def test_broadcast_presence_skips_broken_client():
    class BrokenWebSocket:
        async def send_json(self, message: dict) -> None:
            raise RuntimeError("Cannot write to closing transport")

    class HealthyWebSocket:
        def __init__(self) -> None:
            self.messages = []

        async def send_json(self, message: dict) -> None:
            self.messages.append(message)

    async def run() -> None:
        relay = WebIntercomServer("secret")
        broken_ws = BrokenWebSocket()
        healthy_ws = HealthyWebSocket()
        relay.clients[broken_ws] = Client(broken_ws, "client-0001", "bad", "main", time.monotonic())
        relay.clients[healthy_ws] = Client(healthy_ws, "client-0002", "good", "main", time.monotonic())

        await relay.broadcast_presence("main")

        assert healthy_ws.messages == [
            {
                "type": "presence",
                "room": "main",
                "count": 2,
                "clients": ["bad", "good"],
                "active_stream_ids": [],
            }
        ]

    asyncio.run(run())


def test_broadcast_presence_includes_active_stream_ids():
    class HealthyWebSocket:
        def __init__(self) -> None:
            self.messages = []

        async def send_json(self, message: dict) -> None:
            self.messages.append(message)

    async def run() -> None:
        relay = WebIntercomServer("secret")
        websocket = HealthyWebSocket()
        client = Client(websocket, "client-0001", "alice", "main", time.monotonic())
        client.stream_ids.update({99, 7})
        relay.clients[websocket] = client

        await relay.broadcast_presence("main")

        assert websocket.messages[-1]["active_stream_ids"] == [7, 99]

    asyncio.run(run())


def test_e_model_degrades_with_delay_and_late_drop():
    good_r, good_mos = estimate_e_model(delay_ms=40, late_drop_percent=0)
    poor_r, poor_mos = estimate_e_model(delay_ms=250, late_drop_percent=5)

    assert good_r > poor_r
    assert good_mos > poor_mos
    assert 1 <= poor_mos <= 4.5


def test_metrics_workbook_contains_processed_sheets(tmp_path):
    relay = WebIntercomServer("secret")
    joined_at = time.monotonic()
    relay.metrics.record_join("client-0001", "alice", "main", joined_at)
    relay.metrics.record_client_metrics(
        "client-0001",
        {
            "captured_frames": 10,
            "sent_packets": 10,
            "sent_bytes": 6600,
            "sent_payload_bytes": 6400,
            "received_packets": 8,
            "received_bytes": 5280,
            "received_payload_bytes": 5120,
            "played_packets": 8,
            "rtt_ms": 4,
            "estimated_owd_ms": 2,
            "rfc3550_jitter_ms": 0.8,
            "playback_queue_seconds": 0.02,
            "late_dropped_packets": 0,
            "buffer_underrun_events": 0,
            "session_duration_seconds": 5,
            "last_sent_kbps": 12.8,
            "last_rx_kbps": 10.2,
        },
    )
    relay.metrics.record_audio_in(660, 640)
    relay.metrics.record_audio_relay(660, 640)
    relay.metrics.sample([("client-0001", "alice", "main")])
    output = tmp_path / "web_metrics.xlsx"

    relay.metrics.write_workbook(output)

    workbook = load_workbook(output, data_only=True)
    assert workbook.sheetnames == [
        "Summary",
        "QoS Summary",
        "QoE Summary",
        "Jitter CDF",
        "Assessment",
        "Client Summary",
        "Samples",
        "Client Samples",
        "Metric Guide",
    ]
    assert workbook["Summary"]["A1"].value == "Secure Web Intercom Measurement Summary"
    assert workbook["QoS Summary"]["A1"].value == "Application-Level QoS Summary"
    assert workbook["QoE Summary"]["A1"].value == "QoE and E-Model Summary"
    assert workbook["Client Summary"]["A4"].value == "client_id"
    assert workbook["Samples"]["A4"].value == "timestamp"
    assert workbook["Metric Guide"]["A4"].value == "Source"
