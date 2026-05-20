# Secure Web Voice Intercom

This is the browser-client version of the secure LAN voice intercom. Client machines do not need the Python project, a copied folder, or an executable. They only open the HTTPS URL served by the intercom server.

## How It Works

- The Python server serves a browser UI over HTTPS.
- Browsers connect through WebSocket Secure (`wss://`).
- Each browser captures microphone audio, converts it to 16 kHz PCM16, and sends it to the server.
- The server relays each client's audio to the other clients in the same room.
- Transport security comes from HTTPS/WSS. A room key is required before a client can join.

## Setup on the Server Machine

```powershell
cd "C:\Users\Khang\Documents\Codex\2026-05-20\secure-digital-voice-intercom-system-web"
python -m pip install -r requirements.txt
```

## Run

```powershell
$env:WEB_INTERCOM_KEY = "change-this-demo-secret"
python -m web_intercom.server --host 0.0.0.0 --port 8443
```

The server prints URLs such as:

```text
https://localhost:8443
https://192.168.3.202:8443
```

Open the LAN URL on client machines using Chrome or Edge. The first visit will show a self-signed certificate warning. Continue to the site, then allow microphone access.

## Client Usage

On each client machine:

1. Open `https://SERVER_IP:8443`.
2. Accept the browser certificate warning.
3. Enter a display name.
4. Keep room as `main`, or choose the same room name as the other users.
5. Enter the same room key configured on the server.
6. Click `Connect`.

Use headphones to reduce echo.

## Runtime Measurements and Excel Report

Run the server with `--metrics-xlsx` to generate a processed Excel workbook while clients are connected:

```powershell
$env:WEB_INTERCOM_KEY = "change-this-demo-secret"
python -m web_intercom.server --host 0.0.0.0 --port 8443 --stats-interval 5 --metrics-xlsx web_intercom_metrics.xlsx
```

The workbook is updated every reporting interval and contains:

- `Summary`: human-readable totals such as active clients, total audio MB, average/peak bitrate, failed room-key attempts, and dropped frames.
- `Assessment`: `OK` / `Review` checks with evidence and recommendations.
- `Client Summary`: latest browser-side capture/playback metrics per connected client.
- `Samples`: processed server-side measurement intervals, including bitrate and packet-rate columns.
- `Client Samples`: browser-reported metric samples over time.
- `Metric Guide`: plain-language definitions for every metric.

Browsers automatically send client-side measurement data to the server. You do not need to install anything on client machines.

Do not keep the workbook open in Excel while the server is running. Windows may lock the file; if that happens, close Excel and the next reporting interval will retry the update.

## Firewall

Allow inbound TCP port `8443` on the server machine. Run PowerShell as Administrator:

```powershell
New-NetFirewallRule -DisplayName "Secure Web Intercom HTTPS 8443" -Direction Inbound -Protocol TCP -LocalPort 8443 -Action Allow
```

## Notes and Limitations

- This version is for LAN demos, not public internet deployment.
- Browser microphone access requires HTTPS.
- The generated certificate is self-signed, so clients must accept the browser warning once.
- Audio is relayed by the server; the server can access the audio stream.
- Browser audio APIs introduce more latency than the native Python client.

## Tests

```powershell
python -m pytest
```
