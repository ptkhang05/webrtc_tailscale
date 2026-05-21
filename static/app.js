const TARGET_SAMPLE_RATE = 16000;
const AUDIO_PACKET_MAGIC = [0x53, 0x57, 0x49, 0x31]; // "SWI1"
const AUDIO_PACKET_HEADER_BYTES = 20;
const MIN_PLAYBACK_LEAD_SECONDS = 0.02;
const LATE_DROP_THRESHOLD_SECONDS = 0.12;
const MAX_PLAYBACK_QUEUE_SECONDS = 0.5;
const QOS_PING_INTERVAL_MS = 2000;

const form = document.querySelector("#join-form");
const leaveButton = document.querySelector("#leave-button");
const joinButton = document.querySelector("#join-button");
const statusEl = document.querySelector("#status");
const endpointEl = document.querySelector("#endpoint");
const clientCountEl = document.querySelector("#client-count");
const clientListEl = document.querySelector("#client-list");
const sentRateEl = document.querySelector("#sent-rate");
const rxRateEl = document.querySelector("#rx-rate");
const packetCountEl = document.querySelector("#packet-count");

let socket = null;
let audioContext = null;
let mediaStream = null;
let sourceNode = null;
let workletNode = null;
let silenceGainNode = null;
let nextPlaybackTime = 0;
let qosPingTimer = null;
let streamId = createStreamId();
let sequence = 0;
let remoteStreams = new Map();
let workletCallbackStats = createStats();
let workletMessageStats = createStats();
let lastWorkletMessageAtMs = null;
let metrics = createMetrics();

endpointEl.textContent = window.location.origin;

function createMetrics() {
  return {
    connectedAtMs: performance.now(),
    sentBytes: 0,
    sentPayloadBytes: 0,
    sentPackets: 0,
    receivedBytes: 0,
    receivedPayloadBytes: 0,
    receivedPackets: 0,
    capturedFrames: 0,
    playedPackets: 0,
    captureErrors: 0,
    malformedAudioPackets: 0,
    lateDroppedPackets: 0,
    queueOverflowDroppedPackets: 0,
    bufferUnderrunEvents: 0,
    bufferUnderrunSeconds: 0,
    maxBufferUnderrunMs: 0,
    packets: 0,
    lastSentBytes: 0,
    lastReceivedBytes: 0,
    lastSentKbps: 0,
    lastRxKbps: 0,
    rttMs: 0,
    estimatedOwdMs: 0,
    rfc3550JitterMs: 0,
  };
}

function createStats() {
  return {
    count: 0,
    mean: 0,
    m2: 0,
    max: 0,
  };
}

function addStat(stats, value) {
  if (!Number.isFinite(value) || value <= 0) {
    return;
  }
  stats.count += 1;
  const delta = value - stats.mean;
  stats.mean += delta / stats.count;
  stats.m2 += delta * (value - stats.mean);
  stats.max = Math.max(stats.max, value);
}

function statStddev(stats) {
  if (stats.count < 2) {
    return 0;
  }
  return Math.sqrt(stats.m2 / (stats.count - 1));
}

function createStreamId() {
  const values = new Uint32Array(1);
  crypto.getRandomValues(values);
  return values[0];
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  await connect();
});

leaveButton.addEventListener("click", () => {
  disconnect();
});

setInterval(() => {
  const sentDelta = metrics.sentBytes - metrics.lastSentBytes;
  const rxDelta = metrics.receivedBytes - metrics.lastReceivedBytes;
  metrics.lastSentBytes = metrics.sentBytes;
  metrics.lastReceivedBytes = metrics.receivedBytes;
  metrics.lastSentKbps = (sentDelta * 8) / 1000;
  metrics.lastRxKbps = (rxDelta * 8) / 1000;
  sentRateEl.textContent = `${metrics.lastSentKbps.toFixed(1)} kbps`;
  rxRateEl.textContent = `${metrics.lastRxKbps.toFixed(1)} kbps`;
  packetCountEl.textContent = String(metrics.packets);
}, 1000);

setInterval(() => {
  sendBrowserMetrics();
}, 5000);

async function connect() {
  if (socket) {
    return;
  }

  metrics = createMetrics();
  streamId = createStreamId();
  sequence = 0;
  remoteStreams = new Map();
  workletCallbackStats = createStats();
  workletMessageStats = createStats();
  lastWorkletMessageAtMs = null;
  setStatus("Connecting", false);
  audioContext = new AudioContext({ sampleRate: TARGET_SAMPLE_RATE });
  if (audioContext.audioWorklet === undefined) {
    metrics.captureErrors += 1;
    setStatus("AudioWorklet unavailable", false);
    throw new Error("AudioWorklet is not supported by this browser.");
  }
  await audioContext.audioWorklet.addModule("/static/audio-worklet.js");

  try {
    mediaStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        channelCount: 1,
        noiseSuppression: true,
        echoCancellation: true,
        autoGainControl: true,
      },
    });
  } catch (error) {
    metrics.captureErrors += 1;
    setStatus("Microphone blocked", false);
    throw error;
  }

  const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  socket = new WebSocket(`${wsProtocol}//${window.location.host}/ws`);
  socket.binaryType = "arraybuffer";

  socket.addEventListener("open", () => {
    const payload = {
      type: "join",
      name: document.querySelector("#name").value,
      room: document.querySelector("#room").value,
      key: document.querySelector("#key").value,
    };
    socket.send(JSON.stringify(payload));
  });

  socket.addEventListener("message", (event) => {
    if (typeof event.data === "string") {
      handleControlMessage(JSON.parse(event.data));
      return;
    }
    metrics.receivedBytes += event.data.byteLength;
    const packet = parseAudioPacket(event.data);
    if (!packet) {
      metrics.malformedAudioPackets += 1;
      return;
    }
    metrics.receivedPayloadBytes += packet.payload.byteLength;
    metrics.receivedPackets += 1;
    metrics.packets += 1;
    updateRfc3550Jitter(packet);
    playPcm16(packet.payload);
  });

  socket.addEventListener("close", () => {
    disconnect(false);
  });

  socket.addEventListener("error", () => {
    setStatus("Connection error", false);
  });
}

function startAudioCapture() {
  sourceNode = audioContext.createMediaStreamSource(mediaStream);
  workletNode = new AudioWorkletNode(audioContext, "intercom-capture-processor", {
    numberOfInputs: 1,
    numberOfOutputs: 1,
    outputChannelCount: [1],
  });
  silenceGainNode = audioContext.createGain();
  silenceGainNode.gain.value = 0;

  workletNode.port.onmessage = (event) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return;
    }
    const message = event.data;
    const buffer = message instanceof ArrayBuffer ? message : message.buffer;
    const captureTimeMs =
      message instanceof ArrayBuffer ? audioContext.currentTime * 1000 : Number(message.captureTimeMs || 0);
    const callbackIntervalMs =
      message instanceof ArrayBuffer ? 0 : Number(message.callbackIntervalMs || 0);
    const nowMs = performance.now();
    if (lastWorkletMessageAtMs !== null) {
      addStat(workletMessageStats, nowMs - lastWorkletMessageAtMs);
    }
    lastWorkletMessageAtMs = nowMs;
    addStat(workletCallbackStats, callbackIntervalMs);
    metrics.capturedFrames += 1;
    if (buffer.byteLength > 0) {
      try {
        const packet = buildAudioPacket(buffer, captureTimeMs);
        socket.send(packet);
        metrics.sentBytes += packet.byteLength;
        metrics.sentPayloadBytes += buffer.byteLength;
        metrics.sentPackets += 1;
      } catch (error) {
        metrics.captureErrors += 1;
      }
    }
  };

  sourceNode.connect(workletNode);
  workletNode.connect(silenceGainNode);
  silenceGainNode.connect(audioContext.destination);
}

function handleControlMessage(message) {
  if (message.type === "joined") {
    startAudioCapture();
    startQosProbes();
    joinButton.disabled = true;
    leaveButton.disabled = false;
    setStatus("Connected", true);
    return;
  }
  if (message.type === "qos_pong") {
    const sentAtMs = Number(message.client_time_ms || 0);
    if (sentAtMs > 0) {
      metrics.rttMs = Math.max(0, performance.now() - sentAtMs);
      metrics.estimatedOwdMs = metrics.rttMs / 2;
    }
    return;
  }
  if (message.type === "presence") {
    clientCountEl.textContent = String(message.count);
    clientListEl.innerHTML = "";
    for (const name of message.clients) {
      const item = document.createElement("li");
      item.textContent = name;
      clientListEl.appendChild(item);
    }
    return;
  }
  if (message.type === "error") {
    setStatus(message.message || "Error", false);
  }
}

function startQosProbes() {
  if (qosPingTimer) {
    clearInterval(qosPingTimer);
  }
  sendQosPing();
  qosPingTimer = setInterval(sendQosPing, QOS_PING_INTERVAL_MS);
}

function sendQosPing() {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }
  socket.send(
    JSON.stringify({
      type: "qos_ping",
      client_time_ms: performance.now(),
    }),
  );
}

function disconnect(closeSocket = true) {
  if (closeSocket && socket) {
    socket.close();
  }
  socket = null;
  if (qosPingTimer) {
    clearInterval(qosPingTimer);
    qosPingTimer = null;
  }

  if (workletNode) {
    workletNode.port.onmessage = null;
    workletNode.disconnect();
    workletNode = null;
  }
  if (silenceGainNode) {
    silenceGainNode.disconnect();
    silenceGainNode = null;
  }
  if (sourceNode) {
    sourceNode.disconnect();
    sourceNode = null;
  }
  if (mediaStream) {
    for (const track of mediaStream.getTracks()) {
      track.stop();
    }
    mediaStream = null;
  }
  if (audioContext) {
    audioContext.close();
    audioContext = null;
  }
  nextPlaybackTime = 0;
  joinButton.disabled = false;
  leaveButton.disabled = true;
  setStatus("Disconnected", false);
}

function buildAudioPacket(payloadBuffer, captureTimeMs) {
  const packet = new ArrayBuffer(AUDIO_PACKET_HEADER_BYTES + payloadBuffer.byteLength);
  const header = new DataView(packet);
  const bytes = new Uint8Array(packet);
  bytes.set(AUDIO_PACKET_MAGIC, 0);
  header.setUint32(4, streamId, false);
  header.setUint32(8, sequence, false);
  header.setFloat64(12, captureTimeMs, false);
  bytes.set(new Uint8Array(payloadBuffer), AUDIO_PACKET_HEADER_BYTES);
  sequence = (sequence + 1) >>> 0;
  return packet;
}

function parseAudioPacket(arrayBuffer) {
  if (arrayBuffer.byteLength <= AUDIO_PACKET_HEADER_BYTES) {
    return null;
  }
  const bytes = new Uint8Array(arrayBuffer);
  for (let index = 0; index < AUDIO_PACKET_MAGIC.length; index += 1) {
    if (bytes[index] !== AUDIO_PACKET_MAGIC[index]) {
      return null;
    }
  }
  const header = new DataView(arrayBuffer);
  const captureTimeMs = header.getFloat64(12, false);
  if (!Number.isFinite(captureTimeMs)) {
    return null;
  }
  return {
    streamId: header.getUint32(4, false),
    sequence: header.getUint32(8, false),
    captureTimeMs,
    payload: arrayBuffer.slice(AUDIO_PACKET_HEADER_BYTES),
  };
}

function updateRfc3550Jitter(packet) {
  const arrivalTimeMs = performance.now();
  let state = remoteStreams.get(packet.streamId);
  if (!state) {
    state = {
      previousTransitMs: null,
      jitterMs: 0,
      lastSequence: null,
    };
    remoteStreams.set(packet.streamId, state);
  }
  if (state.lastSequence !== null) {
    const expected = (state.lastSequence + 1) >>> 0;
    if (packet.sequence !== expected) {
      metrics.lateDroppedPackets += 1;
    }
  }
  state.lastSequence = packet.sequence;

  const transitMs = arrivalTimeMs - packet.captureTimeMs;
  if (state.previousTransitMs !== null) {
    const differenceMs = Math.abs(transitMs - state.previousTransitMs);
    state.jitterMs += (differenceMs - state.jitterMs) / 16;
  }
  state.previousTransitMs = transitMs;
  metrics.rfc3550JitterMs = Math.max(
    0,
    ...Array.from(remoteStreams.values(), (stream) => stream.jitterMs),
  );
}

function playPcm16(arrayBuffer) {
  if (!audioContext) {
    return;
  }
  const pcm = new Int16Array(arrayBuffer);
  const outputLength =
    audioContext.sampleRate === TARGET_SAMPLE_RATE
      ? pcm.length
      : Math.ceil((pcm.length * audioContext.sampleRate) / TARGET_SAMPLE_RATE);
  const audioBuffer = audioContext.createBuffer(1, outputLength, audioContext.sampleRate);
  const channel = audioBuffer.getChannelData(0);
  if (audioContext.sampleRate === TARGET_SAMPLE_RATE) {
    for (let i = 0; i < pcm.length; i += 1) {
      channel[i] = pcm[i] / 32768;
    }
  } else {
    const ratio = TARGET_SAMPLE_RATE / audioContext.sampleRate;
    for (let i = 0; i < outputLength; i += 1) {
      const sourceIndex = Math.min(Math.floor(i * ratio), pcm.length - 1);
      channel[i] = pcm[sourceIndex] / 32768;
    }
  }

  const node = audioContext.createBufferSource();
  node.buffer = audioBuffer;
  node.connect(audioContext.destination);
  const now = audioContext.currentTime;
  if (nextPlaybackTime > 0 && now > nextPlaybackTime) {
    const underrunSeconds = now - nextPlaybackTime;
    metrics.bufferUnderrunEvents += 1;
    metrics.bufferUnderrunSeconds += underrunSeconds;
    metrics.maxBufferUnderrunMs = Math.max(metrics.maxBufferUnderrunMs, underrunSeconds * 1000);
    nextPlaybackTime = now + MIN_PLAYBACK_LEAD_SECONDS;
    if (underrunSeconds > LATE_DROP_THRESHOLD_SECONDS) {
      metrics.lateDroppedPackets += 1;
      return;
    }
  }

  const queuedSeconds = Math.max(0, nextPlaybackTime - now);
  if (queuedSeconds > MAX_PLAYBACK_QUEUE_SECONDS) {
    metrics.queueOverflowDroppedPackets += 1;
    metrics.lateDroppedPackets += 1;
    return;
  }

  const startAt = Math.max(now + MIN_PLAYBACK_LEAD_SECONDS, nextPlaybackTime);
  node.start(startAt);
  nextPlaybackTime = startAt + audioBuffer.duration;
  metrics.playedPackets += 1;
}

function setStatus(text, connected) {
  statusEl.textContent = text;
  statusEl.classList.toggle("connected", connected);
}

function sendBrowserMetrics() {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }
  const playbackQueueSeconds = audioContext ? Math.max(0, nextPlaybackTime - audioContext.currentTime) : 0;
  const sessionDurationSeconds = Math.max(0, (performance.now() - metrics.connectedAtMs) / 1000);
  socket.send(
    JSON.stringify({
      type: "metrics",
      metrics: {
        session_duration_seconds: Number(sessionDurationSeconds.toFixed(3)),
        captured_frames: metrics.capturedFrames,
        sent_packets: metrics.sentPackets,
        sent_bytes: metrics.sentBytes,
        sent_payload_bytes: metrics.sentPayloadBytes,
        received_packets: metrics.receivedPackets,
        received_bytes: metrics.receivedBytes,
        received_payload_bytes: metrics.receivedPayloadBytes,
        played_packets: metrics.playedPackets,
        capture_errors: metrics.captureErrors,
        malformed_audio_packets: metrics.malformedAudioPackets,
        late_dropped_packets: metrics.lateDroppedPackets,
        queue_overflow_dropped_packets: metrics.queueOverflowDroppedPackets,
        buffer_underrun_events: metrics.bufferUnderrunEvents,
        buffer_underrun_seconds: Number(metrics.bufferUnderrunSeconds.toFixed(3)),
        max_buffer_underrun_ms: Number(metrics.maxBufferUnderrunMs.toFixed(3)),
        rtt_ms: Number(metrics.rttMs.toFixed(3)),
        estimated_owd_ms: Number(metrics.estimatedOwdMs.toFixed(3)),
        rfc3550_jitter_ms: Number(metrics.rfc3550JitterMs.toFixed(3)),
        callback_interval_mean_ms: Number(workletCallbackStats.mean.toFixed(3)),
        callback_interval_stddev_ms: Number(statStddev(workletCallbackStats).toFixed(3)),
        callback_interval_max_ms: Number(workletCallbackStats.max.toFixed(3)),
        worklet_message_interval_mean_ms: Number(workletMessageStats.mean.toFixed(3)),
        worklet_message_interval_stddev_ms: Number(statStddev(workletMessageStats).toFixed(3)),
        worklet_message_interval_max_ms: Number(workletMessageStats.max.toFixed(3)),
        last_sent_kbps: Number(metrics.lastSentKbps.toFixed(3)),
        last_rx_kbps: Number(metrics.lastRxKbps.toFixed(3)),
        playback_queue_seconds: Number(playbackQueueSeconds.toFixed(3)),
      },
    }),
  );
}
