const TARGET_SAMPLE_RATE = 16000;
const AUDIO_PACKET_MAGIC = [0x53, 0x57, 0x49, 0x31]; // "SWI1"
const AUDIO_PACKET_HEADER_BYTES = 20;
const MIN_PLAYBACK_LEAD_SECONDS = 0.02;
const TALK_BURST_RESET_SECONDS = 0.1;
const MAX_PLAYBACK_QUEUE_SECONDS = 0.5;
const QOS_PING_INTERVAL_MS = 2000;
const MAX_PENDING_CAPTURE_SENDS = 8;
const MEDIA_MODE_WEBRTC = "webrtc";
const MEDIA_MODE_RELAY = "relay";
const SPEAKING_RMS_THRESHOLD = 0.035;
const SPEAKING_HOLD_MS = 450;
const SPEAKER_METER_INTERVAL_MS = 80;

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
const mediaModeEl = document.querySelector("#media-mode");

let socket = null;
let audioContext = null;
let mediaStream = null;
let sourceNode = null;
let workletNode = null;
let silenceGainNode = null;
let qosPingTimer = null;
let streamId = createStreamId();
let sequence = 0;
let remoteStreams = new Map();
let peerConnections = new Map();
let remoteAudioElements = new Map();
let speakerStates = new Map();
let speakerMeters = new Map();
let streamOwners = new Map();
let speakerMeterContext = null;
let selfClientId = null;
let mediaMode = MEDIA_MODE_WEBRTC;
let workletCallbackStats = createStats();
let workletMessageStats = createStats();
let lastWorkletMessageAtMs = null;
let captureSendChain = Promise.resolve();
let pendingCaptureSends = 0;
let captureSessionId = 0;
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
    networkLossPackets: 0,
    resampledFrames: 0,
    captureQueueDroppedFrames: 0,
    audioContextSampleRate: 0,
    packets: 0,
    lastSentBytes: 0,
    lastReceivedBytes: 0,
    lastSentKbps: 0,
    lastRxKbps: 0,
    rttMs: 0,
    estimatedOwdMs: 0,
    rfc3550JitterMs: 0,
    websocketRttMs: 0,
    websocketEstimatedOwdMs: 0,
    webrtcPacketsLost: 0,
    webrtcPacketLossRatePercent: 0,
    webrtcJitterMs: 0,
    webrtcRttMs: 0,
    webrtcSelectedCandidatePair: "",
    webrtcLocalCandidateType: "",
    webrtcRemoteCandidateType: "",
    webrtcCandidateProtocol: "",
    webrtcCodec: "",
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
  if (mediaMode === MEDIA_MODE_WEBRTC && socket) {
    updateWebRtcStats().catch(() => {});
  }
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

setInterval(() => {
  refreshSpeakingIndicators();
}, SPEAKER_METER_INTERVAL_MS);

async function connect() {
  if (socket) {
    return;
  }

  resetRuntimeState();
  mediaMode = mediaModeEl ? mediaModeEl.value : MEDIA_MODE_WEBRTC;
  setStatus("Connecting", false);
  try {
    mediaStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        channelCount: 1,
        noiseSuppression: true,
        echoCancellation: true,
        autoGainControl: true,
      },
    });
    setupStreamSpeakerMeter("local", mediaStream, () => selfClientId);

    if (mediaMode === MEDIA_MODE_RELAY) {
      await setupRelayAudioContext();
    } else if (typeof RTCPeerConnection !== "function") {
      throw new Error("WebRTC is not supported by this browser.");
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
        media_mode: mediaMode,
      };
      socket.send(JSON.stringify(payload));
    });

    socket.addEventListener("message", (event) => {
      if (typeof event.data === "string") {
        handleControlMessage(JSON.parse(event.data));
        return;
      }
      if (mediaMode === MEDIA_MODE_RELAY) {
        handleRelayAudioPacket(event.data);
      }
    });

    socket.addEventListener("close", () => {
      disconnect(false);
    });

    socket.addEventListener("error", () => {
      setStatus("Connection error", false);
    });
  } catch (error) {
    cleanupFailedConnect();
    setStatus(error.message || "Connection failed", false);
    throw error;
  }
}

function resetRuntimeState() {
  metrics = createMetrics();
  streamId = createStreamId();
  sequence = 0;
  remoteStreams = new Map();
  peerConnections = new Map();
  remoteAudioElements = new Map();
  speakerStates = new Map();
  speakerMeters = new Map();
  streamOwners = new Map();
  selfClientId = null;
  workletCallbackStats = createStats();
  workletMessageStats = createStats();
  lastWorkletMessageAtMs = null;
  captureSendChain = Promise.resolve();
  pendingCaptureSends = 0;
  captureSessionId += 1;
}

async function setupRelayAudioContext() {
  audioContext = new AudioContext({ sampleRate: TARGET_SAMPLE_RATE });
  metrics.audioContextSampleRate = audioContext.sampleRate;
  if (audioContext.audioWorklet === undefined) {
    metrics.captureErrors += 1;
    throw new Error("AudioWorklet is not supported by this browser.");
  }
  await audioContext.audioWorklet.addModule("/static/audio-worklet.js");
}

function handleRelayAudioPacket(arrayBuffer) {
  metrics.receivedBytes += arrayBuffer.byteLength;
  const packet = parseAudioPacket(arrayBuffer);
  if (!packet) {
    metrics.malformedAudioPackets += 1;
    return;
  }
  metrics.receivedPayloadBytes += packet.payload.byteLength;
  metrics.receivedPackets += 1;
  metrics.packets += 1;
  updateRfc3550Jitter(packet);
  playPcm16(packet);
}

function startRelayAudioCapture() {
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
    const sourceSampleRate =
      message instanceof ArrayBuffer ? audioContext.sampleRate : Number(message.sampleRate || audioContext.sampleRate);
    const nowMs = performance.now();
    if (lastWorkletMessageAtMs !== null) {
      addStat(workletMessageStats, nowMs - lastWorkletMessageAtMs);
    }
    lastWorkletMessageAtMs = nowMs;
    addStat(workletCallbackStats, callbackIntervalMs);
    metrics.capturedFrames += 1;
    if (buffer.byteLength > 0) {
      enqueueCapturedAudio(buffer, captureTimeMs, sourceSampleRate);
    }
  };

  sourceNode.connect(workletNode);
  workletNode.connect(silenceGainNode);
  silenceGainNode.connect(audioContext.destination);
}

function handleControlMessage(message) {
  if (message.type === "joined") {
    selfClientId = message.client_id || null;
    if (mediaMode === MEDIA_MODE_RELAY) {
      startRelayAudioCapture();
    }
    startQosProbes();
    joinButton.disabled = true;
    leaveButton.disabled = false;
    setStatus(mediaMode === MEDIA_MODE_WEBRTC ? "Connected (WebRTC)" : "Connected (WSS relay)", true);
    return;
  }
  if (message.type === "qos_pong") {
    const sentAtMs = Number(message.client_time_ms || 0);
    if (sentAtMs > 0) {
      metrics.websocketRttMs = Math.max(0, performance.now() - sentAtMs);
      metrics.websocketEstimatedOwdMs = metrics.websocketRttMs / 2;
      if (mediaMode !== MEDIA_MODE_WEBRTC || metrics.webrtcRttMs <= 0) {
        metrics.rttMs = metrics.websocketRttMs;
        metrics.estimatedOwdMs = metrics.websocketEstimatedOwdMs;
      }
    }
    return;
  }
  if (message.type === "presence") {
    clientCountEl.textContent = String(message.count);
    renderClientList(message);
    updateRelayStreamOwners(message.active_streams);
    if (mediaMode === MEDIA_MODE_WEBRTC) {
      reconcileWebRtcPeers(message.peers).catch((error) => {
        console.error("WebRTC peer reconciliation failed", error);
      });
    } else {
      reconcileRemoteStreams(message.active_stream_ids);
    }
    return;
  }
  if (message.type === "webrtc_signal") {
    handleWebRtcSignal(message).catch((error) => {
      console.error("WebRTC signaling failed", error);
    });
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

async function reconcileWebRtcPeers(peers) {
  if (!Array.isArray(peers) || !selfClientId || !mediaStream) {
    return;
  }
  const activePeerIds = new Set(
    peers
      .map((peer) => String(peer.client_id || ""))
      .filter((peerId) => peerId && peerId !== selfClientId),
  );
  for (const peerId of peerConnections.keys()) {
    if (!activePeerIds.has(peerId)) {
      closePeerConnection(peerId);
    }
  }
  for (const peer of peers) {
    const peerId = String(peer.client_id || "");
    if (!peerId || peerId === selfClientId || peerConnections.has(peerId)) {
      continue;
    }
    createPeerConnection(peerId, String(peer.name || peerId));
    if (selfClientId < peerId) {
      await sendOffer(peerId);
    }
  }
}

function createPeerConnection(peerId, peerName) {
  const peerConnection = new RTCPeerConnection({ iceServers: [] });
  const state = {
    peerId,
    peerName,
    peerConnection,
  };
  peerConnections.set(peerId, state);

  for (const track of mediaStream.getTracks()) {
    peerConnection.addTrack(track, mediaStream);
  }

  peerConnection.onicecandidate = (event) => {
    if (event.candidate) {
      sendWebRtcSignal(peerId, {
        kind: "ice",
        candidate: iceCandidateToJson(event.candidate),
      });
    }
  };

  peerConnection.ontrack = (event) => {
    const [stream] = event.streams;
    if (stream) {
      attachRemoteAudio(peerId, stream);
    }
  };

  peerConnection.onconnectionstatechange = () => {
    const stateName = peerConnection.connectionState;
    if (stateName === "failed" || stateName === "closed" || stateName === "disconnected") {
      closePeerConnection(peerId);
    }
  };

  return state;
}

async function sendOffer(peerId) {
  const state = peerConnections.get(peerId);
  if (!state) {
    return;
  }
  const offer = await state.peerConnection.createOffer({
    offerToReceiveAudio: true,
  });
  await state.peerConnection.setLocalDescription(offer);
  sendWebRtcSignal(peerId, {
    kind: "offer",
    description: sessionDescriptionToJson(state.peerConnection.localDescription),
  });
}

async function handleWebRtcSignal(message) {
  if (mediaMode !== MEDIA_MODE_WEBRTC || !mediaStream) {
    return;
  }
  const peerId = String(message.from_client_id || "");
  const signal = message.signal || {};
  if (!peerId || peerId === selfClientId) {
    return;
  }
  let state = peerConnections.get(peerId);
  if (!state) {
    state = createPeerConnection(peerId, String(message.from_name || peerId));
  }
  const peerConnection = state.peerConnection;
  if (signal.kind === "offer" && signal.description) {
    await peerConnection.setRemoteDescription(signal.description);
    const answer = await peerConnection.createAnswer();
    await peerConnection.setLocalDescription(answer);
    sendWebRtcSignal(peerId, {
      kind: "answer",
      description: sessionDescriptionToJson(peerConnection.localDescription),
    });
  } else if (signal.kind === "answer" && signal.description) {
    await peerConnection.setRemoteDescription(signal.description);
  } else if (signal.kind === "ice" && signal.candidate) {
    try {
      await peerConnection.addIceCandidate(signal.candidate);
    } catch (error) {
      console.warn("Ignoring ICE candidate that could not be added", error);
    }
  }
}

function sessionDescriptionToJson(description) {
  return {
    type: description.type,
    sdp: description.sdp,
  };
}

function iceCandidateToJson(candidate) {
  return {
    candidate: candidate.candidate,
    sdpMid: candidate.sdpMid,
    sdpMLineIndex: candidate.sdpMLineIndex,
    usernameFragment: candidate.usernameFragment,
  };
}

function meanPositive(values) {
  const positives = values.filter((value) => Number.isFinite(value) && value > 0);
  if (positives.length === 0) {
    return 0;
  }
  return positives.reduce((total, value) => total + value, 0) / positives.length;
}

function maxPositive(values) {
  const positives = values.filter((value) => Number.isFinite(value) && value > 0);
  if (positives.length === 0) {
    return 0;
  }
  return Math.max(...positives);
}

function codecLabel(codec) {
  if (!codec) {
    return "";
  }
  const mimeType = String(codec.mimeType || "").replace(/^audio\//i, "");
  const clockRate = Number(codec.clockRate || 0);
  return clockRate > 0 ? `${mimeType}/${clockRate}` : mimeType;
}

function candidateTypeLabel(candidate) {
  if (!candidate) {
    return "";
  }
  return String(candidate.candidateType || candidate.type || "");
}

function candidateProtocolLabel(candidate) {
  if (!candidate) {
    return "";
  }
  return String(candidate.protocol || candidate.transport || "").toLowerCase();
}

function sendWebRtcSignal(peerId, signal) {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }
  socket.send(
    JSON.stringify({
      type: "webrtc_signal",
      target_client_id: peerId,
      signal,
    }),
  );
}

function attachRemoteAudio(peerId, stream) {
  let audio = remoteAudioElements.get(peerId);
  if (!audio) {
    audio = document.createElement("audio");
    audio.autoplay = true;
    audio.playsInline = true;
    audio.dataset.peerId = peerId;
    audio.style.display = "none";
    document.body.appendChild(audio);
    remoteAudioElements.set(peerId, audio);
  }
  audio.srcObject = stream;
  setupStreamSpeakerMeter(`peer:${peerId}`, stream, () => peerId);
  audio.play().catch(() => {});
}

function closePeerConnection(peerId) {
  const state = peerConnections.get(peerId);
  if (state) {
    state.peerConnection.close();
    peerConnections.delete(peerId);
  }
  const audio = remoteAudioElements.get(peerId);
  if (audio) {
    audio.srcObject = null;
    audio.remove();
    remoteAudioElements.delete(peerId);
  }
  removeSpeakerMeter(`peer:${peerId}`);
}

async function updateWebRtcStats() {
  let sentBytes = 0;
  let receivedBytes = 0;
  let sentPackets = 0;
  let receivedPackets = 0;
  let packetsLost = 0;
  const jitterValuesMs = [];
  const rttValuesMs = [];
  const candidatePairLabels = new Set();
  const localCandidateTypes = new Set();
  const remoteCandidateTypes = new Set();
  const candidateProtocols = new Set();
  const codecLabels = new Set();
  for (const state of peerConnections.values()) {
    const report = await state.peerConnection.getStats();
    let selectedPair = null;
    for (const item of report.values()) {
      const isAudioRtp = item.kind === "audio" || item.mediaType === "audio";
      if (item.type === "outbound-rtp" && isAudioRtp) {
        sentBytes += Number(item.bytesSent || 0);
        sentPackets += Number(item.packetsSent || 0);
        if (item.codecId) {
          const codec = report.get(item.codecId);
          const label = codecLabel(codec);
          if (label) {
            codecLabels.add(label);
          }
        }
      } else if (item.type === "inbound-rtp" && isAudioRtp) {
        receivedBytes += Number(item.bytesReceived || 0);
        receivedPackets += Number(item.packetsReceived || 0);
        packetsLost += Math.max(0, Number(item.packetsLost || 0));
        const jitterSeconds = Number(item.jitter || 0);
        if (jitterSeconds > 0) {
          jitterValuesMs.push(jitterSeconds * 1000);
        }
        const inboundRttSeconds = Number(item.roundTripTime || 0);
        if (inboundRttSeconds > 0) {
          rttValuesMs.push(inboundRttSeconds * 1000);
        }
        if (item.codecId) {
          const codec = report.get(item.codecId);
          const label = codecLabel(codec);
          if (label) {
            codecLabels.add(label);
          }
        }
      } else if (item.type === "remote-inbound-rtp" && isAudioRtp) {
        const remoteInboundRttSeconds = Number(item.roundTripTime || 0);
        if (remoteInboundRttSeconds > 0) {
          rttValuesMs.push(remoteInboundRttSeconds * 1000);
        }
      } else if (item.type === "candidate-pair") {
        const isSelected = item.selected === true || item.nominated === true;
        if (item.state === "succeeded" && (isSelected || selectedPair === null)) {
          selectedPair = item;
        }
      }
    }
    if (selectedPair) {
      const pairRttSeconds = Number(selectedPair.currentRoundTripTime || 0);
      if (pairRttSeconds > 0) {
        rttValuesMs.push(pairRttSeconds * 1000);
      }
      const localCandidate = report.get(selectedPair.localCandidateId);
      const remoteCandidate = report.get(selectedPair.remoteCandidateId);
      const localType = candidateTypeLabel(localCandidate);
      const remoteType = candidateTypeLabel(remoteCandidate);
      const protocol = candidateProtocolLabel(localCandidate) || candidateProtocolLabel(remoteCandidate);
      if (localType) {
        localCandidateTypes.add(localType);
      }
      if (remoteType) {
        remoteCandidateTypes.add(remoteType);
      }
      if (protocol) {
        candidateProtocols.add(protocol);
      }
      if (localType || remoteType || protocol) {
        candidatePairLabels.add(`${localType || "unknown"}-${remoteType || "unknown"}${protocol ? `/${protocol}` : ""}`);
      }
    }
  }
  metrics.sentBytes = sentBytes;
  metrics.sentPayloadBytes = sentBytes;
  metrics.sentPackets = sentPackets;
  metrics.receivedBytes = receivedBytes;
  metrics.receivedPayloadBytes = receivedBytes;
  metrics.receivedPackets = receivedPackets;
  metrics.playedPackets = receivedPackets;
  metrics.packets = sentPackets + receivedPackets;
  metrics.webrtcPacketsLost = packetsLost;
  metrics.webrtcPacketLossRatePercent =
    receivedPackets + packetsLost > 0 ? (packetsLost / (receivedPackets + packetsLost)) * 100 : 0;
  metrics.webrtcJitterMs = maxPositive(jitterValuesMs);
  metrics.webrtcRttMs = meanPositive(rttValuesMs);
  metrics.webrtcSelectedCandidatePair = Array.from(candidatePairLabels).sort().join("; ").slice(0, 160);
  metrics.webrtcLocalCandidateType = Array.from(localCandidateTypes).sort().join("; ").slice(0, 80);
  metrics.webrtcRemoteCandidateType = Array.from(remoteCandidateTypes).sort().join("; ").slice(0, 80);
  metrics.webrtcCandidateProtocol = Array.from(candidateProtocols).sort().join("; ").slice(0, 40);
  metrics.webrtcCodec = Array.from(codecLabels).sort().join("; ").slice(0, 120);
  metrics.networkLossPackets = packetsLost;
  metrics.rfc3550JitterMs = metrics.webrtcJitterMs;
  if (metrics.webrtcRttMs > 0) {
    metrics.rttMs = metrics.webrtcRttMs;
    metrics.estimatedOwdMs = metrics.webrtcRttMs / 2;
  }
}

function cleanupFailedConnect() {
  cleanupMediaAndPeers();
  joinButton.disabled = false;
  leaveButton.disabled = true;
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
  cleanupMediaAndPeers();
  joinButton.disabled = false;
  leaveButton.disabled = true;
  setStatus("Disconnected", false);
}

function cleanupMediaAndPeers() {
  for (const peerId of Array.from(peerConnections.keys())) {
    closePeerConnection(peerId);
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
  cleanupSpeakerMeters();
  for (const state of remoteStreams.values()) {
    stopActiveNodes(state);
  }
  if (audioContext) {
    audioContext.close();
    audioContext = null;
  }
  captureSendChain = Promise.resolve();
  pendingCaptureSends = 0;
  captureSessionId += 1;
  remoteStreams = new Map();
  streamOwners = new Map();
  speakerStates = new Map();
  clientListEl.innerHTML = "";
  clientCountEl.textContent = "0";
  selfClientId = null;
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
  const state = getRemoteStreamState(packet.streamId);
  const isForwardPacket = updateSequenceTracking(state, packet.sequence);
  if (!isForwardPacket) {
    return;
  }

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

function updateSequenceTracking(state, sequenceValue) {
  if (state.lastSequence !== null) {
    const expected = (state.lastSequence + 1) >>> 0;
    if (sequenceValue === expected) {
      state.lastSequence = sequenceValue;
      return true;
    }
    const forwardGap = (sequenceValue - expected) >>> 0;
    if (forwardGap > 0 && forwardGap < 0x80000000) {
      metrics.networkLossPackets += forwardGap;
      state.lastSequence = sequenceValue;
      return true;
    }
    return false;
  }
  state.lastSequence = sequenceValue;
  return true;
}

function getRemoteStreamState(remoteStreamId) {
  let state = remoteStreams.get(remoteStreamId);
  if (!state) {
    state = {
      previousTransitMs: null,
      jitterMs: 0,
      lastSequence: null,
      nextPlaybackTime: 0,
      activeNodes: [],
    };
    remoteStreams.set(remoteStreamId, state);
  }
  return state;
}

function renderClientList(message) {
  const peers = Array.isArray(message.peers) ? message.peers : [];
  const names = Array.isArray(message.clients) ? message.clients : [];
  const now = performance.now();
  const activeClientIds = new Set();
  clientListEl.innerHTML = "";

  if (peers.length > 0) {
    for (const peer of peers) {
      const clientId = String(peer.client_id || "");
      if (!clientId) {
        continue;
      }
      activeClientIds.add(clientId);
      const name = String(peer.name || clientId);
      const speaker = ensureSpeakerState(clientId, name, clientId === selfClientId);
      clientListEl.appendChild(createClientListItem(speaker, now));
    }
  } else {
    for (const name of names) {
      const clientId = `name:${name}`;
      activeClientIds.add(clientId);
      const speaker = ensureSpeakerState(clientId, String(name), false);
      clientListEl.appendChild(createClientListItem(speaker, now));
    }
  }

  for (const clientId of Array.from(speakerStates.keys())) {
    if (!activeClientIds.has(clientId)) {
      speakerStates.delete(clientId);
    }
  }
}

function createClientListItem(speaker, now) {
  const item = document.createElement("li");
  item.className = "client-chip";
  item.dataset.clientId = speaker.clientId;

  const indicator = document.createElement("span");
  indicator.className = "talk-indicator";
  indicator.setAttribute("aria-hidden", "true");

  const label = document.createElement("span");
  label.className = "client-name";
  label.textContent = speaker.isSelf ? `${speaker.name} (you)` : speaker.name;

  item.appendChild(indicator);
  item.appendChild(label);
  speaker.itemEl = item;
  speaker.indicatorEl = indicator;
  updateSpeakerIndicator(speaker, now);
  return item;
}

function ensureSpeakerState(clientId, name, isSelf) {
  let state = speakerStates.get(clientId);
  if (!state) {
    state = {
      clientId,
      name,
      isSelf,
      lastSpokeAtMs: 0,
      itemEl: null,
      indicatorEl: null,
    };
    speakerStates.set(clientId, state);
  }
  state.name = name || state.name;
  state.isSelf = Boolean(isSelf);
  return state;
}

function updateSpeakerIndicator(speaker, now) {
  const isSpeaking = speaker.lastSpokeAtMs > 0 && now - speaker.lastSpokeAtMs <= SPEAKING_HOLD_MS;
  if (speaker.itemEl) {
    speaker.itemEl.classList.toggle("speaking", isSpeaking);
    speaker.itemEl.title = isSpeaking ? `${speaker.name} is speaking` : `${speaker.name} is silent`;
  }
  if (speaker.indicatorEl) {
    speaker.indicatorEl.classList.toggle("speaking", isSpeaking);
  }
}

function updateRelayStreamOwners(activeStreams) {
  streamOwners = new Map();
  if (!Array.isArray(activeStreams)) {
    return;
  }
  for (const stream of activeStreams) {
    const streamId = Number(stream.stream_id);
    const clientId = String(stream.client_id || "");
    if (Number.isInteger(streamId) && streamId >= 0 && clientId) {
      streamOwners.set(streamId, clientId);
    }
  }
}

function setupStreamSpeakerMeter(meterKey, stream, getClientId) {
  if (typeof AudioContext !== "function") {
    return;
  }
  removeSpeakerMeter(meterKey);
  try {
    if (!speakerMeterContext) {
      speakerMeterContext = new AudioContext();
    }
    speakerMeterContext.resume().catch(() => {});
    const source = speakerMeterContext.createMediaStreamSource(stream);
    const analyser = speakerMeterContext.createAnalyser();
    analyser.fftSize = 512;
    analyser.smoothingTimeConstant = 0.2;
    source.connect(analyser);
    speakerMeters.set(meterKey, {
      source,
      analyser,
      data: new Uint8Array(analyser.fftSize),
      getClientId,
    });
  } catch (error) {
    console.warn("Speaker meter could not be created", error);
  }
}

function removeSpeakerMeter(meterKey) {
  const meter = speakerMeters.get(meterKey);
  if (!meter) {
    return;
  }
  try {
    meter.source.disconnect();
  } catch (error) {
    // The node may already be disconnected by the browser.
  }
  speakerMeters.delete(meterKey);
}

function cleanupSpeakerMeters() {
  for (const meterKey of Array.from(speakerMeters.keys())) {
    removeSpeakerMeter(meterKey);
  }
  if (speakerMeterContext) {
    speakerMeterContext.close().catch(() => {});
    speakerMeterContext = null;
  }
}

function refreshSpeakingIndicators() {
  sampleSpeakerMeters();
  const now = performance.now();
  for (const speaker of speakerStates.values()) {
    updateSpeakerIndicator(speaker, now);
  }
}

function sampleSpeakerMeters() {
  for (const meter of speakerMeters.values()) {
    const clientId = String(meter.getClientId() || "");
    if (!clientId) {
      continue;
    }
    meter.analyser.getByteTimeDomainData(meter.data);
    const rms = rmsFromByteTimeDomain(meter.data);
    if (rms >= SPEAKING_RMS_THRESHOLD) {
      markSpeaking(clientId);
    }
  }
}

function markSpeaking(clientId) {
  const speaker = speakerStates.get(clientId);
  if (!speaker) {
    return;
  }
  speaker.lastSpokeAtMs = performance.now();
}

function markRelayPacketSpeaking(remoteStreamId, pcm) {
  const clientId = streamOwners.get(remoteStreamId);
  if (!clientId) {
    return;
  }
  if (rmsFromInt16(pcm) >= SPEAKING_RMS_THRESHOLD) {
    markSpeaking(clientId);
  }
}

function rmsFromByteTimeDomain(data) {
  if (data.length === 0) {
    return 0;
  }
  let sumSquares = 0;
  for (let index = 0; index < data.length; index += 1) {
    const sample = (data[index] - 128) / 128;
    sumSquares += sample * sample;
  }
  return Math.sqrt(sumSquares / data.length);
}

function rmsFromInt16(pcm) {
  if (pcm.length === 0) {
    return 0;
  }
  let sumSquares = 0;
  for (let index = 0; index < pcm.length; index += 1) {
    const sample = pcm[index] / 32768;
    sumSquares += sample * sample;
  }
  return Math.sqrt(sumSquares / pcm.length);
}

function playPcm16(packet) {
  if (!audioContext) {
    return;
  }
  const state = getRemoteStreamState(packet.streamId);
  const pcm = new Int16Array(packet.payload);
  const audioBuffer = audioContext.createBuffer(1, pcm.length, TARGET_SAMPLE_RATE);
  const channel = audioBuffer.getChannelData(0);
  for (let i = 0; i < pcm.length; i += 1) {
    channel[i] = pcm[i] / 32768;
  }

  const now = audioContext.currentTime;
  pruneActiveNodes(state, now);
  if (state.nextPlaybackTime === 0 || now > state.nextPlaybackTime + TALK_BURST_RESET_SECONDS) {
    state.nextPlaybackTime = now + MIN_PLAYBACK_LEAD_SECONDS;
  } else if (now > state.nextPlaybackTime) {
    const underrunSeconds = now - state.nextPlaybackTime;
    metrics.bufferUnderrunEvents += 1;
    metrics.bufferUnderrunSeconds += underrunSeconds;
    metrics.maxBufferUnderrunMs = Math.max(metrics.maxBufferUnderrunMs, underrunSeconds * 1000);
    state.nextPlaybackTime = now + MIN_PLAYBACK_LEAD_SECONDS;
  }

  const queuedSeconds = Math.max(0, state.nextPlaybackTime - now);
  if (queuedSeconds > MAX_PLAYBACK_QUEUE_SECONDS) {
    metrics.queueOverflowDroppedPackets += 1;
    metrics.lateDroppedPackets += 1;
    stopActiveNodes(state);
    state.nextPlaybackTime = now + MIN_PLAYBACK_LEAD_SECONDS;
    return;
  }

  markRelayPacketSpeaking(packet.streamId, pcm);

  const node = audioContext.createBufferSource();
  node.buffer = audioBuffer;
  node.connect(audioContext.destination);
  const startAt = Math.max(now + MIN_PLAYBACK_LEAD_SECONDS, state.nextPlaybackTime);
  node.start(startAt);
  state.nextPlaybackTime = startAt + audioBuffer.duration;
  state.activeNodes.push({ node, endTime: state.nextPlaybackTime });
  node.addEventListener("ended", () => {
    state.activeNodes = state.activeNodes.filter((entry) => entry.node !== node);
  });
  metrics.playedPackets += 1;
}

function pruneActiveNodes(state, now) {
  state.activeNodes = state.activeNodes.filter((entry) => now < entry.endTime);
}

function stopActiveNodes(state) {
  for (const entry of state.activeNodes) {
    try {
      entry.node.stop();
    } catch (error) {
      // The node may already have ended or been stopped by the browser.
    }
  }
  state.activeNodes = [];
}

function setStatus(text, connected) {
  statusEl.textContent = text;
  statusEl.classList.toggle("connected", connected);
}

function sendBrowserMetrics() {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }
  const playbackQueueSeconds = currentPlaybackQueueSeconds();
  const sessionDurationSeconds = Math.max(0, (performance.now() - metrics.connectedAtMs) / 1000);
  const pathRttMs = mediaMode === MEDIA_MODE_WEBRTC && metrics.webrtcRttMs > 0 ? metrics.webrtcRttMs : metrics.rttMs;
  const pathEstimatedOwdMs =
    mediaMode === MEDIA_MODE_WEBRTC && metrics.webrtcRttMs > 0 ? metrics.webrtcRttMs / 2 : metrics.estimatedOwdMs;
  const pathJitterMs =
    mediaMode === MEDIA_MODE_WEBRTC && metrics.webrtcJitterMs > 0 ? metrics.webrtcJitterMs : metrics.rfc3550JitterMs;
  socket.send(
    JSON.stringify({
      type: "metrics",
      metrics: {
        media_mode: mediaMode,
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
        network_loss_packets: metrics.networkLossPackets,
        late_dropped_packets: metrics.lateDroppedPackets,
        queue_overflow_dropped_packets: metrics.queueOverflowDroppedPackets,
        buffer_underrun_events: metrics.bufferUnderrunEvents,
        buffer_underrun_seconds: Number(metrics.bufferUnderrunSeconds.toFixed(3)),
        max_buffer_underrun_ms: Number(metrics.maxBufferUnderrunMs.toFixed(3)),
        rtt_ms: Number(pathRttMs.toFixed(3)),
        estimated_owd_ms: Number(pathEstimatedOwdMs.toFixed(3)),
        rfc3550_jitter_ms: Number(pathJitterMs.toFixed(3)),
        websocket_rtt_ms: Number(metrics.websocketRttMs.toFixed(3)),
        webrtc_packets_lost: metrics.webrtcPacketsLost,
        webrtc_packet_loss_rate_percent: Number(metrics.webrtcPacketLossRatePercent.toFixed(3)),
        webrtc_jitter_ms: Number(metrics.webrtcJitterMs.toFixed(3)),
        webrtc_rtt_ms: Number(metrics.webrtcRttMs.toFixed(3)),
        webrtc_selected_candidate_pair: metrics.webrtcSelectedCandidatePair,
        webrtc_local_candidate_type: metrics.webrtcLocalCandidateType,
        webrtc_remote_candidate_type: metrics.webrtcRemoteCandidateType,
        webrtc_candidate_protocol: metrics.webrtcCandidateProtocol,
        webrtc_codec: metrics.webrtcCodec,
        callback_interval_mean_ms: Number(workletCallbackStats.mean.toFixed(3)),
        callback_interval_stddev_ms: Number(statStddev(workletCallbackStats).toFixed(3)),
        callback_interval_max_ms: Number(workletCallbackStats.max.toFixed(3)),
        worklet_message_interval_mean_ms: Number(workletMessageStats.mean.toFixed(3)),
        worklet_message_interval_stddev_ms: Number(statStddev(workletMessageStats).toFixed(3)),
        worklet_message_interval_max_ms: Number(workletMessageStats.max.toFixed(3)),
        audio_context_sample_rate: metrics.audioContextSampleRate,
        resampled_frames: metrics.resampledFrames,
        capture_queue_dropped_frames: metrics.captureQueueDroppedFrames,
        active_remote_streams: mediaMode === MEDIA_MODE_WEBRTC ? peerConnections.size : remoteStreams.size,
        last_sent_kbps: Number(metrics.lastSentKbps.toFixed(3)),
        last_rx_kbps: Number(metrics.lastRxKbps.toFixed(3)),
        playback_queue_seconds: Number(playbackQueueSeconds.toFixed(3)),
      },
    }),
  );
}

function currentPlaybackQueueSeconds() {
  if (!audioContext) {
    return 0;
  }
  const now = audioContext.currentTime;
  return Math.max(
    0,
    ...Array.from(remoteStreams.values(), (stream) => {
      pruneActiveNodes(stream, now);
      return Math.max(0, stream.nextPlaybackTime - now);
    }),
  );
}

function reconcileRemoteStreams(activeStreamIds) {
  if (!Array.isArray(activeStreamIds)) {
    return;
  }
  const active = new Set(
    activeStreamIds
      .map((value) => Number(value))
      .filter((value) => Number.isInteger(value) && value >= 0),
  );
  for (const [remoteStreamId, state] of remoteStreams.entries()) {
    if (!active.has(remoteStreamId)) {
      stopActiveNodes(state);
      remoteStreams.delete(remoteStreamId);
    }
  }
}

function enqueueCapturedAudio(buffer, captureTimeMs, sourceSampleRate) {
  if (pendingCaptureSends >= MAX_PENDING_CAPTURE_SENDS) {
    metrics.captureQueueDroppedFrames += 1;
    return;
  }
  pendingCaptureSends += 1;
  const sessionId = captureSessionId;
  const task = captureSendChain.then(() => sendCapturedAudio(buffer, captureTimeMs, sourceSampleRate, sessionId));
  captureSendChain = task.catch(() => {});
  task
    .catch(() => {
      if (sessionId === captureSessionId) {
        metrics.captureErrors += 1;
      }
    })
    .finally(() => {
      if (sessionId === captureSessionId) {
        pendingCaptureSends -= 1;
      }
    });
}

async function sendCapturedAudio(buffer, captureTimeMs, sourceSampleRate, sessionId) {
  if (sessionId !== captureSessionId || !socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }
  let payloadBuffer = buffer;
  const wasResampled = sourceSampleRate !== TARGET_SAMPLE_RATE;
  if (wasResampled) {
    payloadBuffer = await resamplePcm16Buffer(buffer, sourceSampleRate);
  }
  if (sessionId !== captureSessionId || !socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }
  if (wasResampled) {
    metrics.resampledFrames += 1;
  }
  const packet = buildAudioPacket(payloadBuffer, captureTimeMs);
  socket.send(packet);
  metrics.sentBytes += packet.byteLength;
  metrics.sentPayloadBytes += payloadBuffer.byteLength;
  metrics.sentPackets += 1;
}

async function resamplePcm16Buffer(buffer, sourceSampleRate) {
  if (!Number.isFinite(sourceSampleRate) || sourceSampleRate <= 0) {
    throw new Error("Invalid source sample rate.");
  }
  if (typeof OfflineAudioContext !== "function" || typeof AudioBuffer !== "function") {
    throw new Error("Browser audio resampling is unavailable.");
  }
  const pcm = new Int16Array(buffer);
  const targetLength = Math.max(1, Math.round((pcm.length * TARGET_SAMPLE_RATE) / sourceSampleRate));
  const inputBuffer = new AudioBuffer({
    length: pcm.length,
    numberOfChannels: 1,
    sampleRate: sourceSampleRate,
  });
  const inputChannel = inputBuffer.getChannelData(0);
  for (let i = 0; i < pcm.length; i += 1) {
    inputChannel[i] = pcm[i] / 32768;
  }
  const offlineContext = new OfflineAudioContext(1, targetLength, TARGET_SAMPLE_RATE);
  const source = offlineContext.createBufferSource();
  source.buffer = inputBuffer;
  source.connect(offlineContext.destination);
  source.start(0);
  const rendered = await offlineContext.startRendering();
  const renderedChannel = rendered.getChannelData(0);
  const output = new Int16Array(renderedChannel.length);
  for (let i = 0; i < renderedChannel.length; i += 1) {
    const sample = Math.max(-1, Math.min(1, renderedChannel[i]));
    output[i] = sample < 0 ? sample * 0x8000 : sample * 0x7fff;
  }
  return output.buffer;
}
