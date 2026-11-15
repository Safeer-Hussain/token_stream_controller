## 0.1.0

- Initial release of `token_stream_controller`.
- Pure Dart zero-dependency asynchronous token streaming engine.
- Deterministic lifecycle phases: `idle`, `thinking`, `streaming`, `completed`, `cancelled`, and `failed`.
- Transient reasoning status tracking with automatic cleanup upon output token arrival or termination.
- Real-time throughput telemetry (`tokensPerSecond`) and elapsed duration tracking.
- Guaranteed try-finally loading-state cleanup across all completion, cancellation, error, and timeout paths.
- Deterministic text simulation via `simulateText` for offline copilots, widget tests, and demos.
- Robust chunk timeout handling with `TokenStreamException`.
- Explicit mid-stream cancellation with `cancel()`.
- State reset via `reset()` and controller lifecycle management via `dispose()`.
- Multi-turn stream accumulation support with `appendToExisting`.
