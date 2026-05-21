# Agent Handoff Notes

This file is for future Codex/AI agents working on this repository.

## User Context

- The user usually speaks Vietnamese. Reply in Vietnamese unless the user asks otherwise.
- The user wants practical implementation, not only explanations.
- The user prefers changes to be committed and pushed to GitHub when the repo is already connected.
- The GitHub repository is `Khang2kruoi/secure-web-intercom`.
- Main branch is `main`.

## Project Scope

This repository is the browser-client version of the secure LAN voice intercom.

The goal is:

- Server runs on one machine in the LAN/Wi-Fi network.
- Client machines do not need to copy source code, install Python, or receive an executable.
- Clients open the server HTTPS URL in Chrome/Edge.
- Browser captures microphone audio and sends it to the Python server.
- Server relays audio to other clients in the same room.
- A shared room key controls who can join.
- Runtime measurements can be exported to a processed Excel workbook.

There is an older Python CLI/UDP implementation in a sibling folder. Treat it as backup. Do not edit that older project unless the user explicitly asks.

## Important Commands

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Run the server:

```powershell
$env:WEB_INTERCOM_KEY = "change-this-demo-secret"
python -m web_intercom.server --host 0.0.0.0 --port 8443
```

Run with Excel metrics:

```powershell
$env:WEB_INTERCOM_KEY = "change-this-demo-secret"
python -m web_intercom.server --host 0.0.0.0 --port 8443 --stats-interval 5 --metrics-xlsx web_intercom_metrics.xlsx
```

Run tests:

```powershell
python -m pytest
```

Allow server firewall on Windows:

```powershell
New-NetFirewallRule -DisplayName "Secure Web Intercom HTTPS 8443" -Direction Inbound -Protocol TCP -LocalPort 8443 -Action Allow
```

## Room Key Behavior

The room key is the shared password.

- Server reads it from `WEB_INTERCOM_KEY` or `--key`.
- Browser clients must enter the exact same value in the `Room key` field.
- Room name and room key are different:
  - Room name groups clients, for example `main`.
  - Room key authorizes entry.
- Anyone on the LAN who knows the room key can join, so use a stronger key for real testing.

## Current Architecture

Key files:

- `web_intercom/server.py`: aiohttp HTTPS/WSS server, room handling, audio relay, metrics endpoint.
- `web_intercom/certs.py`: self-signed certificate generation for LAN HTTPS.
- `web_intercom/metrics.py`: processed runtime metrics and Excel workbook generation.
- `static/index.html`: browser UI.
- `static/app.js`: browser client logic, WebSocket connection, audio capture/playback, client metrics.
- `static/audio-worklet.js`: microphone capture AudioWorklet processor.
- `tests/test_server.py`: server behavior tests.

The browser audio path already uses `AudioWorklet`, not deprecated `ScriptProcessorNode`.

The browser requests:

```javascript
new AudioContext({ sampleRate: 16000 })
```

This lets the browser/OS perform native resampling and anti-alias filtering instead of manual JavaScript downsampling.

## Known Limitations

- Media transport is still uncompressed PCM16 over WebSocket/TCP.
- TCP can introduce accumulated latency under Wi-Fi packet loss because of ordered delivery and retransmission.
- The server relays audio and can access the plaintext audio stream.
- The generated HTTPS certificate is self-signed, so browsers show a warning on first visit.
- This is for LAN demos and experiments, not public internet deployment.

## Roadmap Priority

If the user asks for technical upgrades, prioritize in this order:

1. Add browser-side Opus compression using WebCodecs to reduce bandwidth from raw PCM16 toward speech-codec bitrates.
2. Replace the media path with WebRTC DataChannel in unordered/unreliable mode, while keeping the Python server as signaling/room coordinator.
3. Add explicit one-way latency probes, jitter tracking, and packet-loss estimation to compare WebSocket/TCP against WebRTC/UDP.
4. Improve echo handling and playback mixing if multi-user demos expose feedback or clipping.

## Metrics Expectations

The user wants measurements to be understandable, not just raw data.

When changing metrics:

- Keep Excel output processed and human-readable.
- Preserve or improve sheets such as `Summary`, `Assessment`, `Client Summary`, `Samples`, `Client Samples`, and `Metric Guide`.
- Add derived metrics and recommendations when they help explain system behavior.
- Avoid dumping raw-only data without interpretation.

## Documentation Expectations

Keep `README.md` aligned with code behavior.

When changing setup, run commands, ports, key handling, metrics, or browser requirements, update the README in the same change.

The user previously worked on LaTeX paper content, but later asked to remove LaTeX-related files from this project. Do not add paper/LaTeX material back into this repo unless the user explicitly asks.

## Engineering Rules

- Prefer small, focused edits.
- Run `python -m pytest` after code changes.
- Do not revert user changes.
- Do not modify generated `certs/`, metrics output workbooks, or local caches unless directly requested.
- Keep the project usable on Windows with Python 3.12.
- Use clear Vietnamese explanations in final responses.
