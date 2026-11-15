# token_stream_controller

[![pub package](https://img.shields.io/pub/v/token_stream_controller.svg)](https://pub.dev/packages/token_stream_controller)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A zero-dependency pure Dart asynchronous token stream controller engineered for Large Language Model (LLM) interfaces, generative AI copilots, and real-time streaming architectures. Features transient reasoning phases, throughput telemetry, explicit cancellation, and try-finally guaranteed loading-state cleanup.

## Features

- Pure Dart architecture with zero external dependencies (`dart:async` only), providing 100% platform compatibility across Android, iOS, Web, Windows, macOS, Linux, and WebAssembly (Wasm).
- Deterministic lifecycle state machine (`idle`, `thinking`, `streaming`, `completed`, `cancelled`, `failed`).
- Transient reasoning status tracking with guaranteed automatic clearing upon the arrival of the first output token.
- Real-time throughput metrics (`tokensPerSecond`) and elapsed duration tracking.
- Try-finally guaranteed resource and loading-state cleanup: `isActive` is guaranteed never to remain `true` after completion, cancellation, errors, or timeouts.
- Configurable chunk timeouts with automatic conversion into `TokenStreamException`.
- Explicit mid-stream cancellation with `cancel()`.
- Built-in deterministic tokenizer and simulator with `simulateText` for offline unit testing, widget tests, and interactive demos.
- Seamless compatibility with any Flutter state management solution (Riverpod, BLoC, Signals, GetX, or native `StreamBuilder`).

## Installation

Add `token_stream_controller` to your project using Dart or Flutter:

```bash
# Pure Dart projects
dart pub add token_stream_controller

# Flutter projects
flutter pub add token_stream_controller
```

Or add it directly to your `pubspec.yaml`:

```yaml
dependencies:
  token_stream_controller: ^0.1.0
```

## Lifecycle Phases

The controller operates across six deterministic lifecycle phases:

| Phase | Description | Active (`isActive`) | Terminal (`isTerminal`) |
| :--- | :--- | :--- | :--- |
| `TokenStreamPhase.idle` | Controller initialized or reset; awaiting input stream. | `false` | `false` |
| `TokenStreamPhase.thinking` | Transient reasoning or context-retrieval phase before output tokens arrive. | `true` | `false` |
| `TokenStreamPhase.streaming` | Actively receiving and concatenating output token chunks. | `true` | `false` |
| `TokenStreamPhase.completed` | Upstream token source completed normally. | `false` | `true` |
| `TokenStreamPhase.cancelled` | Execution aborted explicitly via `cancel()`. | `false` | `true` |
| `TokenStreamPhase.failed` | Upstream token source threw an error or exceeded chunk timeout. | `false` | `true` |

## Usage

### Basic Token Streaming with Reasoning Phase

```dart
import 'package:token_stream_controller/token_stream_controller.dart';

Future<void> main() async {
  final controller = TokenStreamController();

  // Listen to state transitions and snapshots
  controller.stream.listen((snapshot) {
    if (snapshot.isThinking) {
      print('Thinking: ${snapshot.reasoningStatus}');
    } else if (snapshot.isStreaming) {
      print('Streamed: ${snapshot.text} (${snapshot.tokensPerSecond.toStringAsFixed(1)} tok/s)');
    }
  });

  // Bind an upstream token source with an initial reasoning phase
  final result = await controller.bind(
    Stream.fromIterable(['Hello', ', ', 'world', '!']),
    initialReasoningStatus: 'Analyzing context and formulating response...',
  );

  print('Final text: ${result.text}');
  print('Total tokens: ${result.tokenCount}');
  print('Elapsed: ${result.elapsed.inMilliseconds} ms');

  await controller.dispose();
}
```

### Deterministic Text Simulation for Tests and Prototyping

```dart
import 'package:token_stream_controller/token_stream_controller.dart';

Future<void> main() async {
  final controller = TokenStreamController();

  final result = await controller.simulateText(
    'Simulating live streaming without an active backend.',
    tokenDelay: const Duration(milliseconds: 30),
    reasoningDelay: const Duration(milliseconds: 200),
    reasoningStatus: 'Retrieving vector embeddings...',
  );

  print(result.text);
  await controller.dispose();
}
```

### Mid-Stream Cancellation and Chunk Timeouts

```dart
import 'package:token_stream_controller/token_stream_controller.dart';

Future<void> main() async {
  final controller = TokenStreamController();

  // Cancel mid-stream
  final longTask = controller.bind(
    Stream.periodic(const Duration(milliseconds: 100), (i) => 'Chunk $i '),
  );

  await Future<void>.delayed(const Duration(milliseconds: 250));
  await controller.cancel();

  final cancelSnapshot = await longTask;
  print('Cancelled state: ${cancelSnapshot.phase}'); // TokenStreamPhase.cancelled
  print('Is active: ${cancelSnapshot.isActive}'); // false

  // Handle chunk timeout
  final timeoutTask = await controller.bind(
    Stream.periodic(const Duration(seconds: 2), (i) => 'Chunk $i'),
    chunkTimeout: const Duration(milliseconds: 500),
  );

  print('Failed state: ${timeoutTask.phase}'); // TokenStreamPhase.failed
  print('Error: ${timeoutTask.error}'); // TokenStreamException

  await controller.dispose();
}
```

### Integration with Flutter (StreamBuilder)

```dart
StreamBuilder<TokenStreamSnapshot>(
  stream: controller.stream,
  initialData: controller.current,
  builder: (context, snapshot) {
    final state = snapshot.requireData;

    if (state.isThinking) {
      return Text('Thinking: ${state.reasoningStatus}');
    }

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(state.text),
        if (state.isActive)
          Text('Speed: ${state.tokensPerSecond.toStringAsFixed(1)} tok/s'),
      ],
    );
  },
)
```

## Repository Links

- Repository: [https://github.com/Safeer-Hussain/token_stream_controller](https://github.com/Safeer-Hussain/token_stream_controller)
- Issue Tracker: [https://github.com/Safeer-Hussain/token_stream_controller/issues](https://github.com/Safeer-Hussain/token_stream_controller/issues)

## License

This package is licensed under the MIT License. See [LICENSE](LICENSE) for details.
