class IntercomCaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.lastProcessTime = null;
  }

  process(inputs) {
    const input = inputs[0];
    if (!input || input.length === 0 || input[0].length === 0) {
      return true;
    }

    const channel = input[0];
    const callbackIntervalMs =
      this.lastProcessTime === null ? 0 : (currentTime - this.lastProcessTime) * 1000;
    this.lastProcessTime = currentTime;

    const pcm = new Int16Array(channel.length);
    for (let index = 0; index < channel.length; index += 1) {
      const sample = Math.max(-1, Math.min(1, channel[index]));
      pcm[index] = sample < 0 ? sample * 0x8000 : sample * 0x7fff;
    }
    this.port.postMessage(
      {
        type: "audio",
        buffer: pcm.buffer,
        captureTimeMs: currentTime * 1000,
        callbackIntervalMs,
        sampleCount: channel.length,
      },
      [pcm.buffer],
    );
    return true;
  }
}

registerProcessor("intercom-capture-processor", IntercomCaptureProcessor);
