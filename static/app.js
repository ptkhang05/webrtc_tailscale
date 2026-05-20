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
let workletNode = null;
let silenceGainNode = null;
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
    metrics.capturedFrames += 1;
    const buffer = event.data;
    if (buffer.byteLength > 0) {
      try {
        socket.send(buffer);
        metrics.sentBytes += buffer.byteLength;
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
