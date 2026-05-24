"""Processed MATLAB-friendly measurement export for the web intercom server."""

from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
import csv
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import threading
import time
from typing import Mapping


MetricValue = int | float | str
MetricRow = dict[str, MetricValue]

E_MODEL_R0 = 93.2
E_MODEL_IE_PCM16 = 0.0
E_MODEL_BPL_NO_PLC = 10.0


CLIENT_INPUT_FIELDS = {
    "session_duration_seconds",
    "captured_frames",
    "sent_packets",
    "sent_bytes",
    "sent_payload_bytes",
    "received_packets",
    "received_bytes",
    "received_payload_bytes",
    "played_packets",
    "capture_errors",
    "malformed_audio_packets",
    "network_loss_packets",
    "late_dropped_packets",
    "queue_overflow_dropped_packets",
    "buffer_underrun_events",
    "buffer_underrun_seconds",
    "max_buffer_underrun_ms",
    "rtt_ms",
    "estimated_owd_ms",
    "rfc3550_jitter_ms",
    "callback_interval_mean_ms",
    "callback_interval_stddev_ms",
    "callback_interval_max_ms",
    "worklet_message_interval_mean_ms",
    "worklet_message_interval_stddev_ms",
    "worklet_message_interval_max_ms",
    "audio_context_sample_rate",
    "resampled_frames",
    "capture_queue_dropped_frames",
    "active_remote_streams",
    "last_sent_kbps",
    "last_rx_kbps",
    "playback_queue_seconds",
}

CLIENT_DERIVED_FIELDS = {
    "estimated_playout_latency_ms",
    "late_drop_rate_percent",
    "buffer_underrun_rate_per_min",
    "r_factor",
    "estimated_mos",
    "mos_quality",
}

CLIENT_FIELDS = CLIENT_INPUT_FIELDS | CLIENT_DERIVED_FIELDS

CLIENT_SUMMARY_FIELDS = [
    "client_id",
    "name",
    "room",
    "status",
    "session_duration_seconds",
    "sent_packets",
    "received_packets",
    "played_packets",
    "capture_errors",
    "network_loss_packets",
    "late_dropped_packets",
    "late_drop_rate_percent",
    "buffer_underrun_events",
    "buffer_underrun_seconds",
    "max_buffer_underrun_ms",
    "rtt_ms",
    "estimated_owd_ms",
    "rfc3550_jitter_ms",
    "estimated_playout_latency_ms",
    "buffer_underrun_rate_per_min",
    "r_factor",
    "estimated_mos",
    "mos_quality",
    "callback_interval_stddev_ms",
    "audio_context_sample_rate",
    "resampled_frames",
    "capture_queue_dropped_frames",
    "active_remote_streams",
]

TIME_SERIES_FIELDS = [
    "timestamp",
    "uptime_seconds",
    "active_clients",
    "active_rooms",
    "interval_rx_payload_kbps",
    "interval_relayed_payload_kbps",
    "interval_rx_pps",
    "interval_relayed_pps",
    "browser_received_packets",
    "browser_network_loss_packets",
    "browser_late_dropped_packets",
    "browser_buffer_underrun_events",
    "browser_buffer_underrun_seconds",
    "browser_avg_rtt_ms",
    "browser_avg_estimated_owd_ms",
    "browser_max_jitter_ms",
    "browser_avg_playout_latency_ms",
    "browser_avg_estimated_mos",
    "browser_min_estimated_mos",
    "browser_max_callback_stddev_ms",
    "relay_send_drops",
    "dropped_audio_frames",
    "browser_resampled_frames",
    "browser_capture_queue_dropped_frames",
]


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

    def record_audio_in(self, byte_count: int, payload_byte_count: int | None = None) -> None:
        with self._lock:
            self.server_counters["rx_audio_packets"] += 1
            self.server_counters["rx_audio_bytes"] += byte_count
            self.server_counters["rx_audio_payload_bytes"] += payload_byte_count if payload_byte_count is not None else byte_count

    def record_audio_relay(self, byte_count: int, payload_byte_count: int | None = None) -> None:
        with self._lock:
            self.server_counters["relayed_packets"] += 1
            self.server_counters["relayed_bytes"] += byte_count
            self.server_counters["relayed_payload_bytes"] += payload_byte_count if payload_byte_count is not None else byte_count

    def record_client_metrics(self, client_id: str, metrics: Mapping[str, object]) -> None:
        with self._lock:
            state = self.client_states.get(client_id)
            if state is None:
                return
            cleaned = {
                field: _clean_metric_value(value)
                for field, value in metrics.items()
                if field in CLIENT_FIELDS
            }
            merged = {**state.latest, **cleaned}
            cleaned.update(derive_client_qoe_metrics(merged))
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
                "auth_rate_limit_rejections": counters.get("auth_rate_limit_rejections", 0),
                "connection_limit_rejections": counters.get("connection_limit_rejections", 0),
                "invalid_messages": counters.get("invalid_messages", 0),
                "rx_audio_packets": counters.get("rx_audio_packets", 0),
                "rx_audio_bytes": counters.get("rx_audio_bytes", 0),
                "rx_audio_payload_bytes": counters.get("rx_audio_payload_bytes", 0),
                "relayed_packets": counters.get("relayed_packets", 0),
                "relayed_bytes": counters.get("relayed_bytes", 0),
                "relayed_payload_bytes": counters.get("relayed_payload_bytes", 0),
                "relay_send_drops": counters.get("relay_send_drops", 0),
                "dropped_audio_frames": counters.get("dropped_audio_frames", 0),
                "avg_rx_kbps": kbps(counters.get("rx_audio_bytes", 0), uptime),
                "avg_rx_payload_kbps": kbps(counters.get("rx_audio_payload_bytes", 0), uptime),
                "avg_relayed_kbps": kbps(counters.get("relayed_bytes", 0), uptime),
                "avg_relayed_payload_kbps": kbps(counters.get("relayed_payload_bytes", 0), uptime),
                **browser_totals,
            }
            self._add_interval_fields(row)
            self.server_samples.append(row)
            self._last_server_sample = row
            return dict(row)

    def snapshot(self) -> tuple[list[MetricRow], list[ClientMetricState]]:
        with self._lock:
            return deepcopy(self.server_samples), deepcopy(list(self.client_states.values()))

    def write_matlab_export(self, directory: str | Path) -> None:
        server_samples, client_states = self.snapshot()
        write_matlab_export(Path(directory), server_samples, client_states)

    def _browser_totals(self, active_ids: set[str]) -> dict[str, MetricValue]:
        totals: dict[str, MetricValue] = {
            "browser_captured_frames": 0,
            "browser_sent_packets": 0,
            "browser_sent_bytes": 0,
            "browser_sent_payload_bytes": 0,
            "browser_received_packets": 0,
            "browser_received_bytes": 0,
            "browser_received_payload_bytes": 0,
            "browser_played_packets": 0,
            "browser_capture_errors": 0,
            "browser_malformed_audio_packets": 0,
            "browser_network_loss_packets": 0,
            "browser_late_dropped_packets": 0,
            "browser_queue_overflow_dropped_packets": 0,
            "browser_buffer_underrun_events": 0,
            "browser_buffer_underrun_seconds": 0.0,
            "browser_max_buffer_underrun_ms": 0.0,
            "browser_avg_rtt_ms": 0.0,
            "browser_max_rtt_ms": 0.0,
            "browser_avg_estimated_owd_ms": 0.0,
            "browser_max_jitter_ms": 0.0,
            "browser_avg_playout_latency_ms": 0.0,
            "browser_avg_estimated_mos": 0.0,
            "browser_min_estimated_mos": 0.0,
            "browser_max_callback_stddev_ms": 0.0,
            "browser_resampled_frames": 0,
            "browser_capture_queue_dropped_frames": 0,
            "browser_max_audio_context_sample_rate": 0.0,
            "browser_max_active_remote_streams": 0,
        }
        mapping = {
            "captured_frames": "browser_captured_frames",
            "sent_packets": "browser_sent_packets",
            "sent_bytes": "browser_sent_bytes",
            "sent_payload_bytes": "browser_sent_payload_bytes",
            "received_packets": "browser_received_packets",
            "received_bytes": "browser_received_bytes",
            "received_payload_bytes": "browser_received_payload_bytes",
            "played_packets": "browser_played_packets",
            "capture_errors": "browser_capture_errors",
            "malformed_audio_packets": "browser_malformed_audio_packets",
            "network_loss_packets": "browser_network_loss_packets",
            "late_dropped_packets": "browser_late_dropped_packets",
            "queue_overflow_dropped_packets": "browser_queue_overflow_dropped_packets",
            "buffer_underrun_events": "browser_buffer_underrun_events",
            "resampled_frames": "browser_resampled_frames",
            "capture_queue_dropped_frames": "browser_capture_queue_dropped_frames",
        }
        rtt_values: list[float] = []
        owd_values: list[float] = []
        jitter_values: list[float] = []
        latency_values: list[float] = []
        mos_values: list[float] = []
        callback_stddev_values: list[float] = []
        for client_id in active_ids:
            state = self.client_states.get(client_id)
            if state is None:
                continue
            for source, target in mapping.items():
                totals[target] = _as_int(totals[target]) + int(_as_float(state.latest.get(source)))
            totals["browser_buffer_underrun_seconds"] = _as_float(totals["browser_buffer_underrun_seconds"]) + _as_float(
                state.latest.get("buffer_underrun_seconds")
            )
            totals["browser_max_buffer_underrun_ms"] = max(
                _as_float(totals["browser_max_buffer_underrun_ms"]),
                _as_float(state.latest.get("max_buffer_underrun_ms")),
            )
            _append_positive(rtt_values, state.latest.get("rtt_ms"))
            _append_positive(owd_values, state.latest.get("estimated_owd_ms"))
            _append_positive(jitter_values, state.latest.get("rfc3550_jitter_ms"))
            _append_positive(latency_values, state.latest.get("estimated_playout_latency_ms"))
            _append_positive(mos_values, state.latest.get("estimated_mos"))
            _append_positive(callback_stddev_values, state.latest.get("callback_interval_stddev_ms"))
            totals["browser_max_audio_context_sample_rate"] = max(
                _as_float(totals["browser_max_audio_context_sample_rate"]),
                _as_float(state.latest.get("audio_context_sample_rate")),
            )
            totals["browser_max_active_remote_streams"] = max(
                _as_int(totals["browser_max_active_remote_streams"]),
                _as_int(state.latest.get("active_remote_streams")),
            )
        totals["browser_avg_rtt_ms"] = _mean(rtt_values)
        totals["browser_max_rtt_ms"] = _max_values(rtt_values)
        totals["browser_avg_estimated_owd_ms"] = _mean(owd_values)
        totals["browser_max_jitter_ms"] = _max_values(jitter_values)
        totals["browser_avg_playout_latency_ms"] = _mean(latency_values)
        totals["browser_avg_estimated_mos"] = _mean(mos_values)
        totals["browser_min_estimated_mos"] = _min_values(mos_values)
        totals["browser_max_callback_stddev_ms"] = _max_values(callback_stddev_values)
        totals["browser_buffer_underrun_seconds"] = round(_as_float(totals["browser_buffer_underrun_seconds"]), 3)
        return totals

    def _add_interval_fields(self, row: MetricRow) -> None:
        previous = self._last_server_sample
        elapsed = _as_float(row["uptime_seconds"]) - _as_float(previous.get("uptime_seconds")) if previous else 0.0
        intervals = {
            "rx_audio_bytes": ("interval_rx_kbps", 8 / 1000),
            "rx_audio_payload_bytes": ("interval_rx_payload_kbps", 8 / 1000),
            "relayed_bytes": ("interval_relayed_kbps", 8 / 1000),
            "relayed_payload_bytes": ("interval_relayed_payload_kbps", 8 / 1000),
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


def derive_client_qoe_metrics(metrics: Mapping[str, object]) -> MetricRow:
    playback_queue_ms = _as_float(metrics.get("playback_queue_seconds")) * 1000
    estimated_owd_ms = _as_float(metrics.get("estimated_owd_ms"))
    capture_quantum_ms = _as_float(metrics.get("callback_interval_mean_ms")) or 8.0
    estimated_playout_latency_ms = max(0.0, capture_quantum_ms + estimated_owd_ms + playback_queue_ms)

    late_drops = _as_float(metrics.get("late_dropped_packets"))
    received_packets = _as_float(metrics.get("received_packets"))
    late_drop_rate_percent = percentage(late_drops, received_packets)

    session_duration_seconds = _as_float(metrics.get("session_duration_seconds"))
    underrun_rate_per_min = 0.0
    if session_duration_seconds > 0:
        underrun_rate_per_min = _as_float(metrics.get("buffer_underrun_events")) / (session_duration_seconds / 60)

    r_factor, estimated_mos = estimate_e_model(
        delay_ms=estimated_playout_latency_ms,
        late_drop_percent=late_drop_rate_percent,
    )
    return {
        "estimated_playout_latency_ms": round(estimated_playout_latency_ms, 3),
        "late_drop_rate_percent": round(late_drop_rate_percent, 3),
        "buffer_underrun_rate_per_min": round(underrun_rate_per_min, 3),
        "r_factor": round(r_factor, 3),
        "estimated_mos": round(estimated_mos, 3),
        "mos_quality": mos_quality_label(estimated_mos),
    }


def estimate_e_model(delay_ms: float, late_drop_percent: float) -> tuple[float, float]:
    delay = max(0.0, delay_ms)
    delay_impairment = 0.024 * delay
    if delay > 177.3:
        delay_impairment += 0.11 * (delay - 177.3)

    packet_impairment = E_MODEL_IE_PCM16
    loss = max(0.0, late_drop_percent)
    if loss > 0:
        packet_impairment += (95.0 - E_MODEL_IE_PCM16) * loss / (loss + E_MODEL_BPL_NO_PLC)

    r_factor = max(0.0, min(100.0, E_MODEL_R0 - delay_impairment - packet_impairment))
    return r_factor, r_factor_to_mos(r_factor)


def r_factor_to_mos(r_factor: float) -> float:
    r = max(0.0, min(100.0, _as_float(r_factor)))
    if r <= 0:
        return 1.0
    if r >= 100:
        return 4.5
    return round(1 + 0.035 * r + 7e-6 * r * (r - 60) * (100 - r), 3)


def mos_quality_label(mos: float) -> str:
    value = _as_float(mos)
    if value >= 4.3:
        return "Excellent"
    if value >= 4.0:
        return "Good"
    if value >= 3.6:
        return "Fair"
    if value >= 3.1:
        return "Poor"
    if value >= 2.6:
        return "Bad"
    return "Not recommended"


def percentage(numerator: object, denominator: object) -> float:
    value = _as_float(denominator)
    if value <= 0:
        return 0.0
    return (_as_float(numerator) / value) * 100


def write_matlab_export(
    directory: Path,
    server_samples: list[MetricRow],
    client_states: list[ClientMetricState],
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    latest = server_samples[-1] if server_samples else {}
    datasets: list[tuple[str, list[str], list[MetricRow]]] = [
        (
            "summary_metrics.csv",
            ["metric", "value", "unit", "interpretation"],
            _metric_rows(_summary_metric_rows(latest, server_samples, client_states)),
        ),
        (
            "qos_summary.csv",
            ["metric", "value", "unit", "interpretation"],
            _metric_rows(_qos_metric_rows(latest, server_samples, client_states)),
        ),
        (
            "qoe_summary.csv",
            ["metric", "value", "unit", "interpretation"],
            _metric_rows(_qoe_metric_rows(latest, client_states)),
        ),
        ("jitter_cdf.csv", ["percentile", "jitter_ms"], _jitter_cdf_rows(client_states)),
        ("client_summary.csv", CLIENT_SUMMARY_FIELDS, [_client_summary_row(client) for client in client_states]),
        ("time_series.csv", TIME_SERIES_FIELDS, [_select_fields(row, TIME_SERIES_FIELDS) for row in server_samples]),
        (
            "paper_metrics.csv",
            ["metric_group", "exported_metric", "why_it_matters", "paper_use"],
            _paper_metric_rows(),
        ),
    ]
    for filename, fields, rows in datasets:
        _write_csv_atomic(directory / filename, fields, rows)
    _write_matlab_plot_script(directory / "plot_intercom_metrics.m")


def _summary_metric_rows(latest: MetricRow, rows: list[MetricRow], clients: list[ClientMetricState]) -> list[tuple[str, object, str, str]]:
    received = _as_float(latest.get("browser_received_packets"))
    relayed = _as_float(latest.get("relayed_packets"))
    jitter_values = _client_sample_values(clients, "rfc3550_jitter_ms")
    latency_values = _client_sample_values(clients, "estimated_playout_latency_ms")
    owd_values = _client_sample_values(clients, "estimated_owd_ms")
    callback_stddev_values = _client_sample_values(clients, "callback_interval_stddev_ms")
    return [
        ("Measurement duration", _as_float(latest.get("uptime_seconds")), "seconds", "How long this measurement run has been active."),
        ("Sample count", len(rows), "samples", "Number of server-side measurement intervals saved."),
        ("Active clients", _as_int(latest.get("active_clients")), "clients", "Browsers connected at the latest sample."),
        ("Total clients seen", len(clients), "clients", "Unique browser connections during the run."),
        ("Mean estimated OWD", _mean(owd_values) or _as_float(latest.get("browser_avg_estimated_owd_ms")), "ms", "RTT/2 LAN delay estimate used when client clocks are not synchronized."),
        ("P95 playout latency", _percentile(latency_values, 95), "ms", "High-percentile delay estimate for conversational QoE analysis."),
        ("P95 RFC3550 jitter", _percentile(jitter_values, 95), "ms", "High-percentile inter-arrival jitter for paper plots."),
        ("Network sequence gap rate", percentage(latest.get("browser_network_loss_packets"), received), "%", "Estimated sequence gaps divided by browser-received packets."),
        ("Late drop rate", percentage(latest.get("browser_late_dropped_packets"), received), "%", "Packets actually discarded because the playout queue budget was exceeded."),
        ("Buffer underrun events", _as_int(latest.get("browser_buffer_underrun_events")), "events", "Browser playout starvation events during the run."),
        ("Buffer underrun duration", _as_float(latest.get("browser_buffer_underrun_seconds")), "seconds", "Total estimated audible gap duration reported by browsers."),
        ("Average estimated MOS", _as_float(latest.get("browser_avg_estimated_mos")), "MOS", "Objective QoE estimate from the simplified E-model."),
        ("Minimum estimated MOS", _as_float(latest.get("browser_min_estimated_mos")), "MOS", "Worst current client MOS estimate."),
        ("Average payload relay bitrate", _as_float(latest.get("avg_relayed_payload_kbps")), "kbps", "Average forwarded PCM16 payload rate; useful for bandwidth discussion."),
        ("Peak payload relay bitrate", _max(rows, "interval_relayed_payload_kbps"), "kbps", "Highest interval forwarding rate observed by the server."),
        ("Relay send drop rate", percentage(latest.get("relay_send_drops"), relayed), "%", "Server-side drops caused by bounded relay queues or send timeouts."),
        ("AudioWorklet callback stddev max", _max_values(callback_stddev_values), "ms", "Worst browser capture callback timing instability."),
        ("Resampled frames", _as_int(latest.get("browser_resampled_frames")), "frames", "Captured frames converted to 16 kHz when hardware sample rate differed."),
        ("Capture queue drops", _as_int(latest.get("browser_capture_queue_dropped_frames")), "frames", "Captured frames discarded before send due to client-side queue pressure."),
    ]


def _qos_metric_rows(latest: MetricRow, rows: list[MetricRow], clients: list[ClientMetricState]) -> list[tuple[str, object, str, str]]:
    jitter_values = _client_sample_values(clients, "rfc3550_jitter_ms")
    latency_values = _client_sample_values(clients, "estimated_playout_latency_ms")
    owd_values = _client_sample_values(clients, "estimated_owd_ms")
    callback_stddev_values = _client_sample_values(clients, "callback_interval_stddev_ms")
    late_drops = _as_float(latest.get("browser_late_dropped_packets"))
    received = _as_float(latest.get("browser_received_packets"))
    network_gaps = _as_float(latest.get("browser_network_loss_packets"))
    relayed = _as_float(latest.get("relayed_packets"))
    return [
        ("Estimated OWD mean", _mean(owd_values), "ms", "RTT/2 approximation for the browser-to-server path; useful without synchronized LAN clocks."),
        ("Estimated OWD p95", _percentile(owd_values, 95), "ms", "95th percentile one-way delay estimate across client metric samples."),
        ("RFC3550 jitter mean", _mean(jitter_values), "ms", "Mean inter-arrival jitter calculated from packet timestamp spacing."),
        ("RFC3550 jitter p95", _percentile(jitter_values, 95), "ms", "95th percentile jitter; this is more useful for paper plots than total byte counters."),
        ("RFC3550 jitter max", _max_values(jitter_values), "ms", "Worst observed browser jitter estimate."),
        ("Network sequence gaps", network_gaps, "packets", "Estimated missing packets from stream sequence discontinuities; separate from actual playout drops."),
        ("Network sequence gap rate", percentage(network_gaps, received), "%", "Sequence gaps divided by browser-received packets; useful when comparing Wi-Fi conditions."),
        ("Late drop rate", percentage(late_drops, received), "%", "Packets actually dropped because TCP/WebSocket delivery caused a per-stream queue budget overflow."),
        ("Buffer underrun events", _as_int(latest.get("browser_buffer_underrun_events")), "events", "Playback starvation events reported by browsers."),
        ("Buffer underrun duration", _as_float(latest.get("browser_buffer_underrun_seconds")), "seconds", "Cumulative audible gap duration from browser playout underruns."),
        ("Estimated playout latency p95", _percentile(latency_values, 95), "ms", "Approximate client playout path delay including RTT/2 and playback queue."),
        ("AudioWorklet callback stddev max", _max_values(callback_stddev_values), "ms", "Worst callback interval instability reported by browsers."),
        ("Relay send drop rate", percentage(latest.get("relay_send_drops"), relayed), "%", "Server relay drops divided by relayed packets; captures bounded-queue pressure."),
        ("Peak relay payload bitrate", _max(rows, "interval_relayed_payload_kbps"), "kbps", "Peak forwarded PCM16 payload rate during a measurement interval."),
    ]


def _qoe_metric_rows(latest: MetricRow, clients: list[ClientMetricState]) -> list[tuple[str, object, str, str]]:
    mos_values = _client_sample_values(clients, "estimated_mos")
    r_values = _client_sample_values(clients, "r_factor")
    latency_values = _client_sample_values(clients, "estimated_playout_latency_ms")
    latest_mos = _as_float(latest.get("browser_avg_estimated_mos"))
    latest_min_mos = _as_float(latest.get("browser_min_estimated_mos"))
    quality_class = mos_quality_label(latest_mos) if latest_mos > 0 else "No data"
    threshold_status = "No data" if latest_mos == 0 and not mos_values else ("OK" if latest_mos >= 3.6 else "Review")
    return [
        ("Average estimated MOS", _mean(mos_values) or latest_mos, "MOS", "Objective voice quality estimate mapped from the simplified ITU-T G.107 R-factor."),
        ("Minimum estimated MOS", _min_values(mos_values) or latest_min_mos, "MOS", "Worst observed client quality estimate."),
        ("Average R-factor", _mean(r_values), "score", "Transmission rating factor before conversion to MOS."),
        ("Minimum R-factor", _min_values(r_values), "score", "Worst observed R-factor."),
        ("Latency p95", _percentile(latency_values, 95), "ms", "High-percentile playout delay used for QoE interpretation."),
        ("Latest quality class", quality_class, "", "Quality descriptor for the latest average MOS sample."),
        ("Target threshold", 3.6, "MOS", "Fair quality threshold commonly used as a minimum acceptable intercom target."),
        ("Threshold status", threshold_status, "", "Review if estimated MOS is below the fair-quality target."),
        (
            "Model note",
            "Simplified E-model",
            "",
            "The MOS value is objective, not a subjective listening test; it uses measured delay and late-drop impairment.",
        ),
    ]


def _jitter_cdf_rows(clients: list[ClientMetricState]) -> list[MetricRow]:
    values = _client_sample_values(clients, "rfc3550_jitter_ms")
    percentiles = [0, 5, 10, 25, 50, 75, 90, 95, 99, 100]
    return [{"percentile": percentile, "jitter_ms": _percentile(values, percentile)} for percentile in percentiles]


def _client_summary_row(client: ClientMetricState) -> MetricRow:
    latest = client.latest
    uptime = max(time.monotonic() - client.joined_at, 0.0)
    return {
        "client_id": client.client_id,
        "name": client.name,
        "room": client.room,
        "status": "active" if client.active else "disconnected",
        "uptime_seconds": round(uptime, 3),
        "session_duration_seconds": _as_float(latest.get("session_duration_seconds")),
        "captured_frames": _as_int(latest.get("captured_frames")),
        "sent_packets": _as_int(latest.get("sent_packets")),
        "sent_bytes": _as_int(latest.get("sent_bytes")),
        "sent_payload_bytes": _as_int(latest.get("sent_payload_bytes")),
        "received_packets": _as_int(latest.get("received_packets")),
        "received_bytes": _as_int(latest.get("received_bytes")),
        "received_payload_bytes": _as_int(latest.get("received_payload_bytes")),
        "played_packets": _as_int(latest.get("played_packets")),
        "capture_errors": _as_int(latest.get("capture_errors")),
        "malformed_audio_packets": _as_int(latest.get("malformed_audio_packets")),
        "network_loss_packets": _as_int(latest.get("network_loss_packets")),
        "late_dropped_packets": _as_int(latest.get("late_dropped_packets")),
        "queue_overflow_dropped_packets": _as_int(latest.get("queue_overflow_dropped_packets")),
        "buffer_underrun_events": _as_int(latest.get("buffer_underrun_events")),
        "buffer_underrun_seconds": _as_float(latest.get("buffer_underrun_seconds")),
        "max_buffer_underrun_ms": _as_float(latest.get("max_buffer_underrun_ms")),
        "rtt_ms": _as_float(latest.get("rtt_ms")),
        "estimated_owd_ms": _as_float(latest.get("estimated_owd_ms")),
        "rfc3550_jitter_ms": _as_float(latest.get("rfc3550_jitter_ms")),
        "estimated_playout_latency_ms": _as_float(latest.get("estimated_playout_latency_ms")),
        "late_drop_rate_percent": _as_float(latest.get("late_drop_rate_percent")),
        "buffer_underrun_rate_per_min": _as_float(latest.get("buffer_underrun_rate_per_min")),
        "r_factor": _as_float(latest.get("r_factor")),
        "estimated_mos": _as_float(latest.get("estimated_mos")),
        "mos_quality": str(latest.get("mos_quality") or ""),
        "audio_context_sample_rate": _as_float(latest.get("audio_context_sample_rate")),
        "resampled_frames": _as_int(latest.get("resampled_frames")),
        "capture_queue_dropped_frames": _as_int(latest.get("capture_queue_dropped_frames")),
        "active_remote_streams": _as_int(latest.get("active_remote_streams")),
        "callback_interval_stddev_ms": _as_float(latest.get("callback_interval_stddev_ms")),
        "worklet_message_interval_stddev_ms": _as_float(latest.get("worklet_message_interval_stddev_ms")),
        "last_sent_kbps": _as_float(latest.get("last_sent_kbps")),
        "last_rx_kbps": _as_float(latest.get("last_rx_kbps")),
        "playback_queue_seconds": _as_float(latest.get("playback_queue_seconds")),
    }


def _paper_metric_rows() -> list[MetricRow]:
    rows = [
        (
            "Delay",
            "Mean/P95 estimated OWD and P95 playout latency",
            "Voice intercom quality is delay-sensitive; high percentiles expose TCP/WSS delay spikes better than averages alone.",
            "Report per network condition and compare against the conversational-delay target.",
        ),
        (
            "Jitter",
            "RFC3550 jitter mean/P95/max and Jitter CDF",
            "Inter-arrival jitter is a standard real-time media QoS indicator and directly supports distribution plots.",
            "Use jitter_cdf.csv for the main paper figure.",
        ),
        (
            "Late delivery",
            "Late drop rate and network sequence gap rate",
            "WSS/TCP can hide transport loss but still creates packets that miss the playout budget.",
            "Report as impairment percentages under LAN, Wi-Fi, and congested Wi-Fi.",
        ),
        (
            "Playout continuity",
            "Buffer underrun events and duration",
            "Underruns are audible continuity failures and make the latency/jitter results interpretable.",
            "Use events/minute or total duration as supporting QoE evidence.",
        ),
        (
            "QoE",
            "R-factor, estimated MOS, and MOS quality class",
            "Maps delay and late-drop impairment to an objective voice-quality estimate.",
            "Summarize in the QoE table; keep the simplified E-model assumption explicit.",
        ),
        (
            "Bandwidth",
            "Average and peak relay payload bitrate",
            "Quantifies the cost of uncompressed PCM16 relay traffic without cluttering outputs with raw byte counters.",
            "Use when discussing scalability as client count increases.",
        ),
        (
            "Relay stability",
            "Relay send drop rate",
            "Shows whether bounded per-recipient queues are dropping packets due to slow receivers.",
            "Use to explain server-side behavior in congested cases.",
        ),
        (
            "Browser runtime",
            "AudioWorklet callback stddev, resampled frames, capture queue drops",
            "Confirms capture timing stability and whether hardware sample-rate mismatch affected the test.",
            "Use as validity checks, not as the headline result.",
        ),
    ]
    return [
        {
            "metric_group": group,
            "exported_metric": metric,
            "why_it_matters": why,
            "paper_use": use,
        }
        for group, metric, why, use in rows
    ]


def _metric_rows(rows: list[tuple[str, object, str, str]]) -> list[MetricRow]:
    return [
        {
            "metric": metric,
            "value": _format_metric_value(value),
            "unit": unit,
            "interpretation": interpretation,
        }
        for metric, value, unit, interpretation in rows
    ]


def _select_fields(row: Mapping[str, object], fields: list[str]) -> MetricRow:
    return {field: _clean_metric_value(row.get(field, 0)) for field in fields}


def _write_csv_atomic(path: Path, fields: list[str], rows: list[Mapping[str, object]]) -> None:
    temp_path = path.with_name(f"{path.stem}.tmp{path.suffix}")
    with temp_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})
    temp_path.replace(path)


def _write_matlab_plot_script(path: Path) -> None:
    temp_path = path.with_name(f"{path.stem}.tmp{path.suffix}")
    temp_path.write_text(MATLAB_PLOT_SCRIPT, encoding="utf-8")
    temp_path.replace(path)


MATLAB_PLOT_SCRIPT = r"""% Plot processed Secure Web Intercom measurement data.
% Run this script from MATLAB after the server has exported the CSV bundle.

clear; clc;
rootDir = fileparts(mfilename('fullpath'));
figureDir = fullfile(rootDir, 'figures');
if ~exist(figureDir, 'dir')
    mkdir(figureDir);
end

timePath = fullfile(rootDir, 'time_series.csv');
jitterPath = fullfile(rootDir, 'jitter_cdf.csv');

if isfile(timePath)
    T = readtable(timePath);
    if height(T) > 0
        x = T.uptime_seconds;

        if hasvars(T, {'browser_avg_estimated_owd_ms', 'browser_max_jitter_ms', 'browser_avg_playout_latency_ms'})
            fig = figure('Color', 'w', 'Name', 'QoS Delay Trend');
            plot(x, T.browser_avg_estimated_owd_ms, 'LineWidth', 1.8); hold on;
            plot(x, T.browser_max_jitter_ms, 'LineWidth', 1.8);
            plot(x, T.browser_avg_playout_latency_ms, 'LineWidth', 1.8);
            grid on; box on;
            xlabel('Time (s)');
            ylabel('Milliseconds');
            title('QoS Delay Trend');
            legend({'Estimated OWD', 'RFC3550 jitter', 'Playout latency'}, 'Location', 'best');
            save_chart(fig, figureDir, 'qos_delay_trend');
        end

        if hasvars(T, {'interval_rx_payload_kbps', 'interval_relayed_payload_kbps'})
            fig = figure('Color', 'w', 'Name', 'Payload Bitrate Trend');
            plot(x, T.interval_rx_payload_kbps, 'LineWidth', 1.8); hold on;
            plot(x, T.interval_relayed_payload_kbps, 'LineWidth', 1.8);
            grid on; box on;
            xlabel('Time (s)');
            ylabel('Payload bitrate (kbps)');
            title('PCM16 Payload Bitrate');
            legend({'Inbound payload', 'Relayed payload'}, 'Location', 'best');
            save_chart(fig, figureDir, 'payload_bitrate_trend');
        end

        if hasvars(T, {'browser_network_loss_packets', 'browser_late_dropped_packets', 'browser_buffer_underrun_events', 'relay_send_drops'})
            fig = figure('Color', 'w', 'Name', 'Impairment Counters');
            plot(x, T.browser_network_loss_packets, 'LineWidth', 1.8); hold on;
            plot(x, T.browser_late_dropped_packets, 'LineWidth', 1.8);
            plot(x, T.browser_buffer_underrun_events, 'LineWidth', 1.8);
            plot(x, T.relay_send_drops, 'LineWidth', 1.8);
            grid on; box on;
            xlabel('Time (s)');
            ylabel('Cumulative events');
            title('Late Delivery and Playout Impairments');
            legend({'Sequence gaps', 'Late drops', 'Underruns', 'Relay drops'}, 'Location', 'best');
            save_chart(fig, figureDir, 'impairment_counters');
        end

        if hasvars(T, {'browser_avg_estimated_mos', 'browser_min_estimated_mos'})
            fig = figure('Color', 'w', 'Name', 'MOS Trend');
            plot(x, T.browser_avg_estimated_mos, 'LineWidth', 1.8); hold on;
            plot(x, T.browser_min_estimated_mos, 'LineWidth', 1.8);
            yline(3.6, '--k', 'Fair threshold', 'LineWidth', 1.2);
            ylim([1 4.5]);
            grid on; box on;
            xlabel('Time (s)');
            ylabel('Estimated MOS');
            title('Estimated QoE Trend');
            legend({'Average MOS', 'Minimum MOS', 'MOS 3.6 threshold'}, 'Location', 'best');
            save_chart(fig, figureDir, 'mos_trend');
        end
    end
end

if isfile(jitterPath)
    J = readtable(jitterPath);
    if height(J) > 0 && hasvars(J, {'percentile', 'jitter_ms'})
        fig = figure('Color', 'w', 'Name', 'RFC3550 Jitter CDF');
        plot(J.jitter_ms, J.percentile, 'LineWidth', 1.8);
        grid on; box on;
        xlabel('RFC3550 jitter (ms)');
        ylabel('Percentile');
        title('RFC3550 Jitter CDF');
        save_chart(fig, figureDir, 'jitter_cdf');
    end
end

disp(['Figures written to: ', figureDir]);

function ok = hasvars(T, names)
ok = all(ismember(names, T.Properties.VariableNames));
end

function save_chart(fig, figureDir, baseName)
savefig(fig, fullfile(figureDir, [baseName, '.fig']));
try
    exportgraphics(fig, fullfile(figureDir, [baseName, '.png']), 'Resolution', 300);
catch
    saveas(fig, fullfile(figureDir, [baseName, '.png']));
end
end
"""


def _clean_metric_value(value: object) -> MetricValue:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, str)):
        return value
    return 0


def _format_metric_value(value: object) -> MetricValue:
    if isinstance(value, float):
        return round(value, 3)
    return _clean_metric_value(value)


def _as_float(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _as_int(value: object) -> int:
    return int(_as_float(value))


def _append_positive(values: list[float], value: object) -> None:
    number = _as_float(value)
    if number > 0:
        values.append(number)


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 3)


def _max_values(values: list[float]) -> float:
    if not values:
        return 0.0
    return round(max(values), 3)


def _min_values(values: list[float]) -> float:
    if not values:
        return 0.0
    return round(min(values), 3)


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    rank = (len(ordered) - 1) * max(0.0, min(100.0, percentile)) / 100
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 3)


def _client_sample_values(clients: list[ClientMetricState], field: str) -> list[float]:
    values: list[float] = []
    for client in clients:
        for sample in client.samples:
            value = _as_float(sample.get(field))
            if value > 0:
                values.append(value)
    return values


def _max(rows: list[MetricRow], field: str) -> float:
    if not rows:
        return 0.0
    return round(max(_as_float(row.get(field)) for row in rows), 3)
