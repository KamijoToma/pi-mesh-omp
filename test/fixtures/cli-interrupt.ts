// Test-only preload: Windows child.kill("SIGINT") cannot deliver Ctrl-C.
// Dispatch to the real CLI's own handler, without replacing CLI behavior.
process.on("message", (message: unknown) => {
  if (message !== "mesh-test-sigint") return;
  try {
    if (process.listenerCount("SIGINT") === 0) {
      throw new Error("CLI has not registered its SIGINT handler");
    }
    process.emit("SIGINT");
  } finally {
    if (process.connected) process.disconnect();
  }
});
// One-shot CLI commands must not stay alive solely for the control channel.
process.channel?.unref();

export {};
