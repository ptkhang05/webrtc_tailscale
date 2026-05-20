const TARGET_SAMPLE_RATE = 16000;

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
let processorNode = null;
let nextPlaybackTime = 0;
let metrics = {
  sentBytes: 0,
  sentPackets: 0,
  receivedBytes: 0,
  receivedPackets: 0,
  capturedFrames: 0,
  playedPackets: 0,
  captureErrors: 0,
  packets: 0,
  lastSentBytes: 0,
  lastReceivedBytes: 0,
  lastSentKbps: 0,
  lastRxKbps: 0,
};

endpointEl.textContent = window.location.origin;

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

  setStatus("Connecting", false);
  audioContext = new AudioContext();
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
    metrics.receivedPackets += 1;
    metrics.packets += 1;
    playPcm16(event.data);
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
  processorNode = audioContext.createScriptProcessor(2048, 1, 1);
  processorNode.onaudioprocess = (event) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      return;
    }
    const input = event.inputBuffer.getChannelData(0);
    const pcm16 = downsampleToPcm16(input, audioContext.sampleRate, TARGET_SAMPLE_RATE);
    metrics.capturedFrames += 1;
    if (pcm16.byteLength > 0) {
      try {
        socket.send(pcm16.buffer);
        metrics.sentBytes += pcm16.byteLength;
        metrics.sentPackets += 1;
      } catch (error) {
        metrics.captureErrors += 1;
      }
    }
  };
  sourceNode.connect(processorNode);
  processorNode.connect(audioContext.destination);
}

function handleControlMessage(message) {
  if (message.type === "joined") {
    startAudioCapture();
    joinButton.disabled = true;
    leaveButton.disabled = false;
    setStatus("Connected", true);
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

function disconnect(closeSocket = true) {
  if (closeSocket && socket) {
    socket.close();
  }
  socket = null;

  if (processorNode) {
    processorNode.disconnect();
    processorNode = null;
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

function downsampleToPcm16(input, inputRate, outputRate) {
  if (inputRate === outputRate) {
    return floatToPcm16(input);
  }
  const ratio = inputRate / outputRate;
  const outputLength = Math.floor(input.length / ratio);
  const output = new Float32Array(outputLength);
  for (let i = 0; i < outputLength; i += 1) {
    const start = Math.floor(i * ratio);
    const end = Math.min(Math.floor((i + 1) * ratio), input.length);
    let sum = 0;
    let count = 0;
    for (let j = start; j < end; j += 1) {
      sum += input[j];
      count += 1;
    }
    output[i] = count ? sum / count : 0;
  }
  return floatToPcm16(output);
}

function floatToPcm16(float32) {
  const pcm = new Int16Array(float32.length);
  for (let i = 0; i < float32.length; i += 1) {
    const sample = Math.max(-1, Math.min(1, float32[i]));
    pcm[i] = sample < 0 ? sample * 0x8000 : sample * 0x7fff;
  }
  return pcm;
}

function playPcm16(arrayBuffer) {
  if (!audioContext) {
    return;
  }
  const pcm = new Int16Array(arrayBuffer);
  const outputLength = Math.ceil((pcm.length * audioContext.sampleRate) / TARGET_SAMPLE_RATE);
  const audioBuffer = audioContext.createBuffer(1, outputLength, audioContext.sampleRate);
  const channel = audioBuffer.getChannelData(0);
  const ratio = TARGET_SAMPLE_RATE / audioContext.sampleRate;
  for (let i = 0; i < outputLength; i += 1) {
    const sourceIndex = Math.min(Math.floor(i * ratio), pcm.length - 1);
    channel[i] = pcm[sourceIndex] / 32768;
  }

  const node = audioContext.createBufferSource();
  node.buffer = audioBuffer;
  node.connect(audioContext.destination);
  const startAt = Math.max(audioContext.currentTime + 0.02, nextPlaybackTime);
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
  socket.send(
    JSON.stringify({
      type: "metrics",
      metrics: {
        captured_frames: metrics.capturedFrames,
        sent_packets: metrics.sentPackets,
        sent_bytes: metrics.sentBytes,
        received_packets: metrics.receivedPackets,
        received_bytes: metrics.receivedBytes,
        played_packets: metrics.playedPackets,
        capture_errors: metrics.captureErrors,
        last_sent_kbps: Number(metrics.lastSentKbps.toFixed(3)),
        last_rx_kbps: Number(metrics.lastRxKbps.toFixed(3)),
        playback_queue_seconds: Number(playbackQueueSeconds.toFixed(3)),
      },
    }),
  );
}
