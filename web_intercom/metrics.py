"""Processed measurement workbook export for the web intercom server."""

from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import threading
import time
from typing import Mapping


MetricValue = int | float | str
MetricRow = dict[str, MetricValue]


SERVER_GUIDE = {
    "timestamp": ("Timestamp", "", "Local time when the measurement row was captured."),
    "uptime_seconds": ("Uptime", "seconds", "Elapsed server runtime when the sample was captured."),
    "active_clients": ("Active clients", "clients", "Connected browser clients at this sample."),
    "active_rooms": ("Active rooms", "rooms", "Rooms with at least one connected client."),
    "join_events": ("Join events", "events", "Successful browser client joins."),
    "leave_events": ("Leave events", "events", "Browser client disconnects."),
    "auth_failures": ("Authentication failures", "events", "Join attempts rejected because the room key was wrong."),
    "invalid_messages": ("Invalid control messages", "events", "Malformed or unexpected text messages."),
    "rx_audio_packets": ("Audio packets received", "packets", "Binary audio packets accepted by the server."),
    "rx_audio_bytes": ("Audio bytes received", "bytes", "Audio payload bytes received from browsers."),
    "relayed_packets": ("Audio packets relayed", "packets", "Audio packets forwarded to other clients."),
    "relayed_bytes": ("Audio bytes relayed", "bytes", "Audio payload bytes forwarded to browsers."),
    "dropped_audio_frames": ("Dropped audio frames", "frames", "Audio frames rejected because they were empty or too large."),
    "avg_rx_kbps": ("Average receive bitrate", "kbps", "Average server audio receive bitrate since start."),
    "avg_relayed_kbps": ("Average relay bitrate", "kbps", "Average server audio forwarding bitrate since start."),
    "interval_rx_kbps": ("Interval receive bitrate", "kbps", "Audio receive bitrate during the latest interval."),
    "interval_relayed_kbps": ("Interval relay bitrate", "kbps", "Audio forwarding bitrate during the latest interval."),
    "interval_rx_pps": ("Interval receive packet rate", "packets/s", "Audio packets received per second in the latest interval."),
    "interval_relayed_pps": ("Interval relay packet rate", "packets/s", "Audio packets relayed per second in the latest interval."),
    "browser_captured_frames": ("Browser captured frames", "frames", "Total microphone callback frames reported by active browsers."),
    "browser_sent_packets": ("Browser sent packets", "packets", "Total audio packets browsers report sending."),
    "browser_sent_bytes": ("Browser sent bytes", "bytes", "Total audio bytes browsers report sending."),
    "browser_received_packets": ("Browser received packets", "packets", "Total audio packets browsers report receiving."),
    "browser_received_bytes": ("Browser received bytes", "bytes", "Total audio bytes browsers report receiving."),
    "browser_played_packets": ("Browser played packets", "packets", "Total received packets scheduled for speaker playback."),
    "browser_capture_errors": ("Browser capture errors", "events", "Microphone capture errors reported by browsers."),
}


CLIENT_GUIDE = {
    "client_id": ("Client ID", "", "Server-assigned unique client connection ID."),
    "name": ("Name", "", "Browser display name."),
    "room": ("Room", "", "Intercom room name."),
    "status": ("Status", "", "Whether the client is active at workbook export time."),
    "uptime_seconds": ("Client uptime", "seconds", "Elapsed time since this client joined."),
    "captured_frames": ("Captured frames", "frames", "Microphone callback frames captured in the browser."),
    "sent_packets": ("Sent packets", "packets", "Audio packets sent from the browser to the server."),
    "sent_bytes": ("Sent bytes", "bytes", "Audio payload bytes sent from the browser to the server."),
    "received_packets": ("Received packets", "packets", "Audio packets received from the server."),
    "received_bytes": ("Received bytes", "bytes", "Audio payload bytes received from the server."),
    "played_packets": ("Played packets", "packets", "Received packets scheduled for speaker playback."),
    "capture_errors": ("Capture errors", "events", "Browser-side capture/send errors."),
    "last_sent_kbps": ("Latest send bitrate", "kbps", "Browser-calculated send bitrate in the latest UI interval."),
    "last_rx_kbps": ("Latest receive bitrate", "kbps", "Browser-calculated receive bitrate in the latest UI interval."),
    "playback_queue_seconds": ("Playback queue", "seconds", "Estimated browser playback buffer delay."),
}


@dataclass
class ClientMetricState:
    client_id: str
    name: str
    room: str
    joined_at: float
    active: bool = True
    latest: dict[str, MetricValue] = field(default_factory=dict)
    samples: list[MetricRow] = field(default_factory=list)


class WebIntercomMetrics:
    def __init__(self) -> None:
        self.started_at = time.monotonic()
        self._lock = threading.Lock()
        self._next_client_id = 1
        self.server_counters: defaultdict[str, int] = defaultdict(int)
        self.client_states: dict[str, ClientMetricState] = {}
        self.server_samples: list[MetricRow] = []
        self._last_server_sample: MetricRow | None = None

    def next_client_id(self) -> str:
        with self._lock:
            value = f"client-{self._next_client_id:04d}"
            self._next_client_id += 1
            return value

    def inc(self, field: str, amount: int = 1) -> None:
        if amount < 0:
            raise ValueError("metric increment cannot be negative")
        with self._lock:
            self.server_counters[field] += amount

    def record_join(self, client_id: str, name: str, room: str, joined_at: float) -> None:
        with self._lock:
            self.server_counters["join_events"] += 1
            self.client_states[client_id] = ClientMetricState(
                client_id=client_id,
                name=name,
                room=room,
                joined_at=joined_at,
            )

    def record_leave(self, client_id: str) -> None:
        with self._lock:
            self.server_counters["leave_events"] += 1
            if client_id in self.client_states:
                self.client_states[client_id].active = False

    def record_audio_in(self, byte_count: int) -> None:
        with self._lock:
            self.server_counters["rx_audio_packets"] += 1
            self.server_counters["rx_audio_bytes"] += byte_count

    def record_audio_relay(self, byte_count: int) -> None:
        with self._lock:
            self.server_counters["relayed_packets"] += 1
            self.server_counters["relayed_bytes"] += byte_count

    def record_client_metrics(self, client_id: str, metrics: Mapping[str, object]) -> None:
        with self._lock:
            state = self.client_states.get(client_id)
            if state is None:
                return
            cleaned = {
                field: _clean_metric_value(value)
                for field, value in metrics.items()
                if field in CLIENT_GUIDE
            }
            state.latest.update(cleaned)
            state.samples.append(
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "client_id": client_id,
                    "name": state.name,
                    "room": state.room,
                    **cleaned,
                }
            )

    def sample(self, active_clients: list[tuple[str, str, str]]) -> MetricRow:
        with self._lock:
            uptime = time.monotonic() - self.started_at
            counters = dict(self.server_counters)
            active_ids = {client_id for client_id, _name, _room in active_clients}
            active_rooms = {room for _client_id, _name, room in active_clients}
            browser_totals = self._browser_totals(active_ids)
            row: MetricRow = {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "uptime_seconds": round(uptime, 3),
                "active_clients": len(active_ids),
                "active_rooms": len(active_rooms),
                "join_events": counters.get("join_events", 0),
                "leave_events": counters.get("leave_events", 0),
                "auth_failures": counters.get("auth_failures", 0),
                "invalid_messages": counters.get("invalid_messages", 0),
                "rx_audio_packets": counters.get("rx_audio_packets", 0),
                "rx_audio_bytes": counters.get("rx_audio_bytes", 0),
                "relayed_packets": counters.get("relayed_packets", 0),
                "relayed_bytes": counters.get("relayed_bytes", 0),
                "dropped_audio_frames": counters.get("dropped_audio_frames", 0),
                "avg_rx_kbps": kbps(counters.get("rx_audio_bytes", 0), uptime),
                "avg_relayed_kbps": kbps(counters.get("relayed_bytes", 0), uptime),
                **browser_totals,
            }
            self._add_interval_fields(row)
            self.server_samples.append(row)
            self._last_server_sample = row
            return dict(row)

    def snapshot(self) -> tuple[list[MetricRow], list[ClientMetricState]]:
        with self._lock:
            return deepcopy(self.server_samples), deepcopy(list(self.client_states.values()))

    def write_workbook(self, path: str | Path) -> None:
        server_samples, client_states = self.snapshot()
        write_metrics_workbook(Path(path), server_samples, client_states)

    def _browser_totals(self, active_ids: set[str]) -> dict[str, int]:
        totals = {
            "browser_captured_frames": 0,
            "browser_sent_packets": 0,
            "browser_sent_bytes": 0,
            "browser_received_packets": 0,
            "browser_received_bytes": 0,
            "browser_played_packets": 0,
            "browser_capture_errors": 0,
        }
        mapping = {
            "captured_frames": "browser_captured_frames",
            "sent_packets": "browser_sent_packets",
            "sent_bytes": "browser_sent_bytes",
            "received_packets": "browser_received_packets",
            "received_bytes": "browser_received_bytes",
            "played_packets": "browser_played_packets",
            "capture_errors": "browser_capture_errors",
        }
        for client_id in active_ids:
            state = self.client_states.get(client_id)
            if state is None:
                continue
            for source, target in mapping.items():
                totals[target] += int(_as_float(state.latest.get(source)))
        return totals

    def _add_interval_fields(self, row: MetricRow) -> None:
        previous = self._last_server_sample
        elapsed = _as_float(row["uptime_seconds"]) - _as_float(previous.get("uptime_seconds")) if previous else 0.0
        intervals = {
            "rx_audio_bytes": ("interval_rx_kbps", 8 / 1000),
            "relayed_bytes": ("interval_relayed_kbps", 8 / 1000),
            "rx_audio_packets": ("interval_rx_pps", 1),
            "relayed_packets": ("interval_relayed_pps", 1),
        }
        for source, (target, scale) in intervals.items():
            if previous is None or elapsed <= 0:
                row[target] = 0.0
                continue
            delta = max(0.0, _as_float(row.get(source)) - _as_float(previous.get(source)))
            row[target] = round(delta * scale / elapsed, 3)


def kbps(byte_count: int | float, uptime_seconds: int | float) -> float:
    uptime = _as_float(uptime_seconds)
    if uptime <= 0:
        return 0.0
    return round((_as_float(byte_count) * 8) / uptime / 1000, 3)


def write_metrics_workbook(
    path: Path,
    server_samples: list[MetricRow],
    client_states: list[ClientMetricState],
) -> None:
    try:
        from openpyxl import Workbook
        from openpyxl.chart import LineChart, Reference
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.worksheet.table import Table, TableStyleInfo
    except ImportError as exc:
        raise RuntimeError("Excel export requires openpyxl. Run: python -m pip install -r requirements.txt") from exc

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    summary = workbook.active
    summary.title = "Summary"
    assessment = workbook.create_sheet("Assessment")
    client_summary = workbook.create_sheet("Client Summary")
    samples = workbook.create_sheet("Samples")
    client_samples = workbook.create_sheet("Client Samples")
    guide = workbook.create_sheet("Metric Guide")

    latest = server_samples[-1] if server_samples else {}
    _build_summary(summary, latest, server_samples, client_states, Font, PatternFill)
    _build_assessment(assessment, latest, client_states, Font, PatternFill)
    _build_client_summary(client_summary, client_states, Table, TableStyleInfo, Font, PatternFill)
    _build_samples(samples, server_samples, Table, TableStyleInfo, Font, PatternFill)
    _build_client_samples(client_samples, client_states, Table, TableStyleInfo, Font, PatternFill)
    _build_guide(guide, Font, PatternFill)
    _add_bitrate_chart(summary, samples, len(server_samples), LineChart, Reference)

    for sheet in workbook.worksheets:
        sheet.sheet_view.showGridLines = False
        sheet.freeze_panes = "A4"
        _size_columns(sheet, Alignment)

    temp_path = path.with_name(f"{path.stem}.tmp{path.suffix}")
    workbook.save(temp_path)
    temp_path.replace(path)


def _build_summary(sheet: object, latest: MetricRow, rows: list[MetricRow], clients: list[ClientMetricState], font_cls: object, fill_cls: object) -> None:
    _title(sheet, "Secure Web Intercom Measurement Summary", font_cls, fill_cls)
    headers = ["Metric", "Value", "Unit", "Interpretation"]
    _write_row(sheet, 4, headers)
    _style_header(sheet, "A4:D4", font_cls, fill_cls)

    summary_rows = [
        ("Measurement duration", _as_float(latest.get("uptime_seconds")), "seconds", "How long this measurement run has been active."),
        ("Sample count", len(rows), "samples", "Number of server-side measurement intervals saved."),
        ("Active clients", _as_int(latest.get("active_clients")), "clients", "Browsers connected at the latest sample."),
        ("Active rooms", _as_int(latest.get("active_rooms")), "rooms", "Rooms currently in use."),
        ("Total joins", _as_int(latest.get("join_events")), "events", "Successful browser connections."),
        ("Authentication failures", _as_int(latest.get("auth_failures")), "events", "Rejected attempts due to wrong room key."),
        ("Audio received", _mb(latest.get("rx_audio_bytes")), "MB", "Audio payload received from browsers."),
        ("Audio relayed", _mb(latest.get("relayed_bytes")), "MB", "Audio payload forwarded to other browsers."),
        ("Average receive bitrate", _as_float(latest.get("avg_rx_kbps")), "kbps", "Average inbound audio rate since start."),
        ("Average relay bitrate", _as_float(latest.get("avg_relayed_kbps")), "kbps", "Average server forwarding rate since start."),
        ("Peak interval receive bitrate", _max(rows, "interval_rx_kbps"), "kbps", "Highest inbound audio bitrate in any measurement interval."),
        ("Peak interval relay bitrate", _max(rows, "interval_relayed_kbps"), "kbps", "Highest forwarding bitrate in any measurement interval."),
        ("Browser sent audio", _mb(latest.get("browser_sent_bytes")), "MB", "Client-reported microphone audio uploaded."),
        ("Browser received audio", _mb(latest.get("browser_received_bytes")), "MB", "Client-reported audio downloaded for playback."),
        ("Browser playback packets", _as_int(latest.get("browser_played_packets")), "packets", "Client-reported received packets scheduled for speakers."),
        ("Dropped audio frames", _as_int(latest.get("dropped_audio_frames")), "frames", "Frames rejected by server validation."),
        ("Browser capture errors", _as_int(latest.get("browser_capture_errors")), "events", "Capture errors reported by browsers."),
        ("Total clients seen", len(clients), "clients", "Unique browser connections during the run."),
    ]
    for index, row in enumerate(summary_rows, start=5):
        _write_row(sheet, index, row)
        if isinstance(row[1], float):
            sheet.cell(index, 2).number_format = "0.000"


def _build_assessment(sheet: object, latest: MetricRow, clients: list[ClientMetricState], font_cls: object, fill_cls: object) -> None:
    _title(sheet, "Assessment", font_cls, fill_cls)
    headers = ["Check", "Status", "Evidence", "Recommendation"]
    _write_row(sheet, 4, headers)
    _style_header(sheet, "A4:D4", font_cls, fill_cls)

    active_clients = _as_int(latest.get("active_clients"))
    relayed_packets = _as_int(latest.get("relayed_packets"))
    sent_packets = _as_int(latest.get("browser_sent_packets"))
    received_packets = _as_int(latest.get("browser_received_packets"))
    auth_failures = _as_int(latest.get("auth_failures"))
    capture_errors = _as_int(latest.get("browser_capture_errors"))
    client_names = ", ".join(sorted({client.name for client in clients if client.active})) or "none"

    rows = [
        _assessment("Client presence", active_clients >= 2, f"{active_clients} active client(s): {client_names}", "Run at least two browser clients in the same room for a real intercom test."),
        _assessment("Audio upload", sent_packets > 0, f"{sent_packets} browser-reported sent packet(s)", "If zero, verify microphone permission and that clients clicked Connect."),
        _assessment("Server relay", relayed_packets > 0, f"{relayed_packets} relayed packet(s)", "If zero while clients are online, confirm they are in the same room and speaking."),
        _assessment("Audio playback", received_packets > 0, f"{received_packets} browser-reported received packet(s)", "If zero, connect another client and check browser audio output."),
        _assessment("Room-key security", auth_failures == 0, f"{auth_failures} failed authentication attempt(s)", "If non-zero, verify users entered the correct room key."),
        _assessment("Capture health", capture_errors == 0, f"{capture_errors} capture error(s)", "If non-zero, check browser microphone permissions and device availability."),
    ]
    for index, row in enumerate(rows, start=5):
        _write_row(sheet, index, row)
        status_cell = sheet.cell(index, 2)
        status_cell.font = font_cls(bold=True)
        status_cell.fill = fill_cls("solid", fgColor="C6EFCE" if row[1] == "OK" else "FFC7CE")


def _build_client_summary(sheet: object, clients: list[ClientMetricState], table_cls: object, style_cls: object, font_cls: object, fill_cls: object) -> None:
    _title(sheet, "Client Summary", font_cls, fill_cls)
    fields = [
        "client_id",
        "name",
        "room",
        "status",
        "uptime_seconds",
        "captured_frames",
        "sent_packets",
        "sent_bytes",
        "received_packets",
        "received_bytes",
        "played_packets",
        "capture_errors",
        "last_sent_kbps",
        "last_rx_kbps",
        "playback_queue_seconds",
    ]
    _write_table(sheet, fields, [_client_summary_row(client) for client in clients], table_cls, style_cls, font_cls, fill_cls)


def _build_samples(sheet: object, rows: list[MetricRow], table_cls: object, style_cls: object, font_cls: object, fill_cls: object) -> None:
    _title(sheet, "Server Samples", font_cls, fill_cls)
    fields = [
        "timestamp",
        "uptime_seconds",
        "active_clients",
        "active_rooms",
        "join_events",
        "leave_events",
        "auth_failures",
        "invalid_messages",
        "rx_audio_packets",
        "rx_audio_bytes",
        "relayed_packets",
        "relayed_bytes",
        "dropped_audio_frames",
        "avg_rx_kbps",
        "avg_relayed_kbps",
        "interval_rx_kbps",
        "interval_relayed_kbps",
        "interval_rx_pps",
        "interval_relayed_pps",
        "browser_captured_frames",
        "browser_sent_packets",
        "browser_sent_bytes",
        "browser_received_packets",
        "browser_received_bytes",
        "browser_played_packets",
        "browser_capture_errors",
    ]
    _write_table(sheet, fields, rows, table_cls, style_cls, font_cls, fill_cls)


def _build_client_samples(sheet: object, clients: list[ClientMetricState], table_cls: object, style_cls: object, font_cls: object, fill_cls: object) -> None:
    _title(sheet, "Browser Metric Samples", font_cls, fill_cls)
    fields = ["timestamp", "client_id", "name", "room", *CLIENT_GUIDE.keys()]
    fields = _dedupe(fields)
    rows: list[MetricRow] = []
    for client in clients:
        rows.extend(client.samples)
    _write_table(sheet, fields, rows, table_cls, style_cls, font_cls, fill_cls)


def _build_guide(sheet: object, font_cls: object, fill_cls: object) -> None:
    _title(sheet, "Metric Guide", font_cls, fill_cls)
    fields = ["Source", "Column", "Label", "Unit", "Meaning"]
    _write_row(sheet, 4, fields)
    _style_header(sheet, "A4:E4", font_cls, fill_cls)
    index = 5
    for source, guide in [("Server", SERVER_GUIDE), ("Browser", CLIENT_GUIDE)]:
        for field, (label, unit, meaning) in guide.items():
            _write_row(sheet, index, [source, field, label, unit, meaning])
            index += 1


def _write_table(sheet: object, fields: list[str], rows: list[Mapping[str, object]], table_cls: object, style_cls: object, font_cls: object, fill_cls: object) -> None:
    _write_row(sheet, 4, fields)
    _style_header(sheet, f"A4:{_column_letter(len(fields))}4", font_cls, fill_cls)
    for row_index, row in enumerate(rows, start=5):
        _write_row(sheet, row_index, [row.get(field, 0) for field in fields])
    if rows:
        table_ref = f"A4:{_column_letter(len(fields))}{len(rows) + 4}"
        table_name = "".join(ch for ch in sheet.title if ch.isalnum())[:20] + "Table"
        table = table_cls(displayName=table_name, ref=table_ref)
        table.tableStyleInfo = style_cls(name="TableStyleMedium2", showRowStripes=True, showColumnStripes=False)
        sheet.add_table(table)


def _client_summary_row(client: ClientMetricState) -> MetricRow:
    latest = client.latest
    uptime = max(time.monotonic() - client.joined_at, 0.0)
    return {
        "client_id": client.client_id,
        "name": client.name,
        "room": client.room,
        "status": "active" if client.active else "disconnected",
        "uptime_seconds": round(uptime, 3),
        "captured_frames": _as_int(latest.get("captured_frames")),
        "sent_packets": _as_int(latest.get("sent_packets")),
        "sent_bytes": _as_int(latest.get("sent_bytes")),
        "received_packets": _as_int(latest.get("received_packets")),
        "received_bytes": _as_int(latest.get("received_bytes")),
        "played_packets": _as_int(latest.get("played_packets")),
        "capture_errors": _as_int(latest.get("capture_errors")),
        "last_sent_kbps": _as_float(latest.get("last_sent_kbps")),
        "last_rx_kbps": _as_float(latest.get("last_rx_kbps")),
        "playback_queue_seconds": _as_float(latest.get("playback_queue_seconds")),
    }


def _add_bitrate_chart(summary: object, samples: object, row_count: int, chart_cls: object, reference_cls: object) -> None:
    if row_count < 2:
        return
    chart = chart_cls()
    chart.title = "Interval Bitrate Trend"
    chart.y_axis.title = "kbps"
    chart.x_axis.title = "sample"
    for column in (16, 17):
        data = reference_cls(samples, min_col=column, max_col=column, min_row=4, max_row=row_count + 4)
        chart.add_data(data, titles_from_data=True)
    summary.add_chart(chart, "F4")


def _title(sheet: object, title: str, font_cls: object, fill_cls: object) -> None:
    sheet["A1"] = title
    sheet["A2"] = "Processed measurement workbook for browser-based secure LAN intercom."
    sheet["A1"].font = font_cls(size=16, bold=True, color="FFFFFF")
    sheet["A1"].fill = fill_cls("solid", fgColor="1F4E78")
    sheet.merge_cells("A1:D1")


def _style_header(sheet: object, range_name: str, font_cls: object, fill_cls: object) -> None:
    for row in sheet[range_name]:
        for cell in row:
            cell.font = font_cls(bold=True, color="FFFFFF")
            cell.fill = fill_cls("solid", fgColor="4472C4")


def _size_columns(sheet: object, alignment_cls: object) -> None:
    from openpyxl.utils import get_column_letter

    for column_index in range(1, sheet.max_column + 1):
        letter = get_column_letter(column_index)
        width = max(
            len(str(sheet.cell(row_index, column_index).value))
            if sheet.cell(row_index, column_index).value is not None
            else 0
            for row_index in range(1, sheet.max_row + 1)
        )
        sheet.column_dimensions[letter].width = min(max(width + 2, 12), 48)
    for row in sheet.iter_rows():
        for cell in row:
            cell.alignment = alignment_cls(vertical="top", wrap_text=True)
            if isinstance(cell.value, float):
                cell.number_format = "0.000"


def _write_row(sheet: object, row_index: int, values: list[object] | tuple[object, ...]) -> None:
    for column_index, value in enumerate(values, start=1):
        sheet.cell(row_index, column_index, value)


def _assessment(check: str, ok: bool, evidence: str, recommendation: str) -> tuple[str, str, str, str]:
    return (check, "OK" if ok else "Review", evidence, recommendation)


def _clean_metric_value(value: object) -> MetricValue:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, str)):
        return value
    return 0


def _as_float(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _as_int(value: object) -> int:
    return int(_as_float(value))


def _mb(value: object) -> float:
    return round(_as_float(value) / 1_000_000, 3)


def _max(rows: list[MetricRow], field: str) -> float:
    if not rows:
        return 0.0
    return round(max(_as_float(row.get(field)) for row in rows), 3)


def _column_letter(index: int) -> str:
    letters = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters or "A"


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result
