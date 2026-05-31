"""Tailscale discovery helpers for tailnet-only deployments."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import socket
import subprocess


@dataclass(frozen=True)
class TailscaleConfig:
    hostname: str
    ip: str
    cert_path: Path | None
    key_path: Path | None


def discover_tailscale(
    cert_dir: str | Path | None = None,
    require_cert: bool = True,
) -> TailscaleConfig:
    data = _tailscale_status_json()
    self_info = data.get("Self")
    if not isinstance(self_info, dict):
        raise RuntimeError("tailscale status --json did not include Self information")

    hostname = str(self_info.get("DNSName") or "").rstrip(".")
    if not hostname:
        raise RuntimeError("Tailscale MagicDNS name is unavailable. Enable MagicDNS first.")

    ips = self_info.get("TailscaleIPs") or []
    ip = next((str(value) for value in ips if str(value).count(".") == 3), "")
    if not ip:
        ip = socket.gethostbyname(hostname)
    if not ip.startswith("100."):
        raise RuntimeError(f"resolved Tailscale IP does not look like a tailnet IPv4 address: {ip}")

    cert_path, key_path = find_tailscale_cert(hostname, cert_dir)
    if require_cert and (cert_path is None or key_path is None):
        raise FileNotFoundError(
            "Tailscale certificate was not found. Enable HTTPS certificates in the Tailscale admin console, "
            f"then run: tailscale cert {hostname}"
        )
    return TailscaleConfig(hostname=hostname, ip=ip, cert_path=cert_path, key_path=key_path)


def find_tailscale_cert(hostname: str, cert_dir: str | Path | None = None) -> tuple[Path | None, Path | None]:
    search_dirs: list[Path] = []
    if cert_dir:
        search_dirs.append(Path(cert_dir))
    search_dirs.extend(
        [
            Path.cwd(),
            Path("/var/lib/tailscale/certs"),
            Path("/Library/Tailscale/certs"),
            Path.home() / ".tailscale" / "certs",
        ]
    )
    for directory in search_dirs:
        cert_path = directory / f"{hostname}.crt"
        key_path = directory / f"{hostname}.key"
        if cert_path.exists() and key_path.exists():
            return cert_path, key_path
    return None, None


def _tailscale_status_json() -> dict[str, object]:
    try:
        result = subprocess.run(
            ["tailscale", "status", "--json"],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("tailscale CLI was not found. Install and log in to Tailscale first.") from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip()
        raise RuntimeError(f"tailscale status --json failed: {stderr}") from exc
    return json.loads(result.stdout)

