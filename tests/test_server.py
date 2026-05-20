import asyncio
import time

from openpyxl import load_workbook

from web_intercom.server import WebIntercomServer, build_arg_parser, create_app, load_room_key


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


def test_metrics_workbook_contains_processed_sheets(tmp_path):
    relay = WebIntercomServer("secret")
    joined_at = time.monotonic()
    relay.metrics.record_join("client-0001", "alice", "main", joined_at)
    relay.metrics.record_client_metrics(
        "client-0001",
        {
            "captured_frames": 10,
            "sent_packets": 10,
            "sent_bytes": 6400,
            "received_packets": 8,
            "received_bytes": 5120,
            "played_packets": 8,
            "last_sent_kbps": 12.8,
            "last_rx_kbps": 10.2,
        },
    )
    relay.metrics.record_audio_in(640)
    relay.metrics.record_audio_relay(640)
    relay.metrics.sample([("client-0001", "alice", "main")])
    output = tmp_path / "web_metrics.xlsx"

    relay.metrics.write_workbook(output)

    workbook = load_workbook(output, data_only=True)
    assert workbook.sheetnames == [
        "Summary",
        "Assessment",
        "Client Summary",
        "Samples",
        "Client Samples",
        "Metric Guide",
    ]
    assert workbook["Summary"]["A1"].value == "Secure Web Intercom Measurement Summary"
    assert workbook["Client Summary"]["A4"].value == "client_id"
    assert workbook["Samples"]["A4"].value == "timestamp"
    assert workbook["Metric Guide"]["A4"].value == "Source"
