# Secure Web Voice Intercom

This is the browser-client version of the secure LAN voice intercom. Client machines do not need the Python project, a copied folder, or an executable. They only open the HTTPS URL served by the intercom server.

## How It Works

- The Python server serves a browser UI over HTTPS.
- Browsers connect through WebSocket Secure (`wss://`).
- Each browser captures microphone audio with `AudioWorklet`, requests a 16 kHz `AudioContext`, resamples captured frames to 16 kHz if the hardware forces a different rate, converts frames to PCM16, and sends them to the server.
- Each browser keeps an independent playout clock per remote stream, so simultaneous talkers are mixed by the Web Audio graph instead of being serialized into one global queue.
- The server relays each client's audio to the other clients in the same room, with bounded per-recipient queues and send timeouts so one slow browser does not stall the whole room.
- Transport security comes from HTTPS/WSS with TLS 1.2 or newer. A room key is required before a client can join.
- The server limits total clients, open WebSocket connections, per-IP connections, and repeated failed room-key attempts.

## Setup on the Server Machine

Install Python 3.12, then clone the project and install the Python dependencies:

```powershell
git clone https://github.com/Khang2kruoi/secure-web-intercom.git
cd secure-web-intercom
python -m pip install -r requirements.txt
```

If you downloaded the project as a ZIP file instead of using Git, open a terminal in the extracted project folder and run the same `python -m pip install -r requirements.txt` command.

## Run

```powershell
$env:WEB_INTERCOM_KEY = "change-this-demo-secret"
python -m web_intercom.server --host 0.0.0.0 --port 8443
```

Optional resource limits are available for demos with many browsers:

```powershell
python -m web_intercom.server --host 0.0.0.0 --port 8443 --max-clients 20 --max-open-connections 40 --max-connections-per-ip 8 --max-auth-failures-per-ip 5
```

In this example, the room key is:

```text
change-this-demo-secret
```

The server prints URLs such as:

```text
https://localhost:8443
https://192.168.3.202:8443
```

Open the LAN URL on client machines using Chrome or Edge. The first visit will show a self-signed certificate warning. Continue to the site, then allow microphone access.

## Room Key

The room key is the shared password that clients must enter in the browser before they can join.

Set the room key on the server with an environment variable:

```powershell
$env:WEB_INTERCOM_KEY = "change-this-demo-secret"
python -m web_intercom.server --host 0.0.0.0 --port 8443
```

Or pass it directly with `--key`:

```powershell
python -m web_intercom.server --host 0.0.0.0 --port 8443 --key "change-this-demo-secret"
```

On each browser client, enter the exact same value in the `Room key` field:

```text
change-this-demo-secret
```

Use a stronger key for real testing. Anyone on the LAN who knows the room key can join the room.

## Client Usage

On each client machine:

1. Open `https://SERVER_IP:8443`.
2. Accept the browser certificate warning.
3. Enter a display name.
4. Keep room as `main`, or choose the same room name as the other users.
5. Enter the same room key configured on the server, for example `change-this-demo-secret`.
6. Click `Connect`.

Use headphones to reduce echo.

## Runtime Measurements and Excel Report

Run the server with `--metrics-xlsx` to generate a processed Excel workbook while clients are connected:

```powershell
$env:WEB_INTERCOM_KEY = "change-this-demo-secret"
python -m web_intercom.server --host 0.0.0.0 --port 8443 --stats-interval 5 --metrics-xlsx web_intercom_metrics.xlsx
```

The workbook is updated every reporting interval and contains:

- `Summary`: the selected paper-ready run metrics, including p95 delay, p95 jitter, late-drop rate, underrun duration, MOS, relay bitrate, relay drops, and browser timing stability.
- `QoS Summary`: application-level QoS indicators: estimated one-way delay, RFC 3550 inter-arrival jitter, sequence-gap rate, late-drop rate, underruns, relay-drop rate, and peak relay payload bitrate.
- `QoE Summary`: estimated R-factor and MOS values using a simplified ITU-T G.107 E-model.
- `Jitter CDF`: percentile table and chart for RFC 3550 jitter, intended as the main chart source for paper figures.
- `Client Summary`: latest per-browser QoS/QoE and validity metrics.
- `Time Series`: compact interval samples for plotting paper figures across LAN/Wi-Fi/congested scenarios.
- `Paper Metrics`: an explanation of which metrics are suitable for the paper and how to use them.

Browsers automatically send client-side measurement data to the server. You do not need to install anything on client machines.

Workbook generation runs in a separate process so report writing does not compete with the server event loop for Python's GIL during audio relay.
The workbook intentionally excludes raw byte dumps and routine implementation counters from the visible report. It focuses on metrics that can support an IEEE-style experimental section: delay, jitter, late delivery, playout continuity, MOS, relay scalability, and browser capture stability.

Do not keep the workbook open in Excel while the server is running. Windows may lock the file; if that happens, close Excel and the next reporting interval will retry the update.

### Measurement Notes

The audio packet format includes an application header with a random stream ID, a monotonically increasing sequence number, and an audio capture timestamp. Browser clients use these fields to estimate inter-arrival jitter with the RFC 3550 formula:

```text
D(i,j) = (Rj - Ri) - (Sj - Si)
J = J + (|D(i,j)| - J) / 16
```

The browser also sends WebSocket QoS pings. The workbook reports `estimated_owd_ms` as `RTT / 2`, which is a practical LAN approximation that avoids requiring synchronized clocks between client machines. Treat it as an estimate, not a hardware timestamp measurement.

Playback scheduling is tracked per remote stream. It flushes an overgrown stream queue instead of waiting for stale buffered audio to drain. When a stream queue is flushed, scheduled `AudioBufferSourceNode` instances for that stream are stopped so stale audio cannot overlap with newly scheduled packets. After silence or network gaps longer than 100 ms, the next received audio packet is treated as a fresh talk burst, so the beginning of speech is not discarded and silence is not counted as a buffer underrun.

Sequence gaps are reported as `network_loss_packets`, but only forward sequence jumps are counted. Reordered or stale packets do not move the stream baseline backward or add fake loss. Sequence gaps are not counted as late drops by themselves; `late_dropped_packets` only increases when the browser actually discards a packet because the per-stream playback queue budget was exceeded.

Presence messages include `active_stream_ids`, allowing browsers to remove state and scheduled audio nodes for streams that have left the room.

For IEEE-style experiments, run the same scenario under controlled network conditions and compare results. Useful scenarios include normal LAN, added delay, added packet loss, and congested Wi-Fi. The current media path is WebSocket/TCP with PCM16; it is intentionally measurable but can accumulate delay under loss because TCP preserves ordering.

## Firewall

Allow inbound TCP port `8443` on the server machine. Run PowerShell as Administrator:

```powershell
New-NetFirewallRule -DisplayName "Secure Web Intercom HTTPS 8443" -Direction Inbound -Protocol TCP -LocalPort 8443 -Action Allow
```

## Notes and Limitations

- This version is for LAN demos, not public internet deployment.
- Browser microphone access requires HTTPS.
- The generated certificate is self-signed, so clients must accept the browser warning once. The private key is written with restrictive file permissions where the operating system supports them.
- The HTTPS server enforces TLS 1.2 or newer.
- The server rejects excess connections and rate-limits repeated failed room-key attempts per IP.
- Audio is relayed by the server; the server can access the audio stream.
- Audio capture uses `AudioWorklet` instead of the deprecated `ScriptProcessorNode`, so capture work is isolated from the browser UI thread.
- The client requests `AudioContext({ sampleRate: 16000 })`. If the actual `AudioContext.sampleRate` differs, captured audio is resampled to 16 kHz with `OfflineAudioContext` before transmission. Received PCM16 is placed in a 16 kHz `AudioBuffer` so the browser performs playback resampling instead of nearest-neighbor JavaScript scaling.
- Audio packets carry stream ID, sequence number, and capture timestamp fields so browsers can measure jitter, late drops, buffer underruns, and callback stability.
- Server relay sends are isolated per recipient with a bounded 4-packet relay queue and short send timeout. If a recipient queue is full, new packets for that recipient are dropped immediately instead of allowing hidden backlog growth. Presence updates are also sent with bounded concurrent writes so one slow browser cannot delay the whole room update.
- Audio is still sent as uncompressed PCM16 over WebSocket/TCP. This is simple and measurable, but Wi-Fi loss or congestion can increase latency because TCP preserves order.

## Roadmap

Recommended next upgrades:

1. Add browser-side Opus encoding with WebCodecs to reduce audio bandwidth from raw PCM16 toward speech-codec bitrates.
2. Replace the WebSocket media path with WebRTC DataChannel in unordered/unreliable mode, using the Python server only for signaling.
3. Add explicit one-way latency probes and jitter statistics to compare WebSocket/TCP against a WebRTC/UDP media path.

## Tests

```powershell
python -m pytest
```
