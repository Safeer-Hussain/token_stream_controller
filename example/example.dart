import 'dart:async';

import 'package:token_stream_controller/token_stream_controller.dart';

void main() async {
  print('================================================================');
  print('TOKEN STREAM CONTROLLER - STANDALONE CLI DEMO');
  print('================================================================');

  final controller = TokenStreamController();

  // Listen to all broadcast snapshot emissions.
  final subscription = controller.stream.listen((snapshot) {
    if (snapshot.isThinking) {
      print('[THINKING] Status: ${snapshot.reasoningStatus}');
    }
  });

  // 1. Transient reasoning phase and simulated token streaming.
  print('\n--- SCENARIO 1: Reasoning Phase and Throughput Telemetry ---');
  final result1 = await controller.simulateText(
    'TokenStreamController provides zero-dependency stream management with telemetry.',
    tokenDelay: const Duration(milliseconds: 25),
    reasoningDelay: const Duration(milliseconds: 150),
    reasoningStatus: 'Analyzing user request and retrieving prompt context...',
  );

  print('\n[COMPLETED] Accumulated output:');
  print(result1.text);
  print('Token count: ${result1.tokenCount}');
  print('Elapsed time: ${result1.elapsed.inMilliseconds} ms');
  print('Throughput: ${result1.tokensPerSecond.toStringAsFixed(2)} tokens/sec');
  print('Final phase: ${result1.phase}');

  // 2. Mid-stream cancellation demonstration.
  print('\n--- SCENARIO 2: Mid-Stream Cancellation ---');
  final infiniteSource = StreamController<String>();

  unawaited(() async {
    for (var i = 1; i <= 20; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 30));
      if (infiniteSource.isClosed) {
        break;
      }
      infiniteSource.add('Token-$i ');
    }
    await infiniteSource.close();
  }());

  final bindFuture = controller.bind(
    infiniteSource.stream,
    onChunk: (snapshot) {
      print(
          '[CHUNK] Received chunk #${snapshot.tokenCount}: "${snapshot.text}"');
    },
  );

  // Allow 3 tokens to stream, then cancel immediately.
  await Future<void>.delayed(const Duration(milliseconds: 100));
  print('[ACTION] Triggering mid-stream cancel...');
  await controller.cancel();

  final cancelResult = await bindFuture;
  print('[CANCELLED] Result phase: ${cancelResult.phase}');
  print('Text received before cancellation: "${cancelResult.text}"');
  print('Is active: ${cancelResult.isActive}');
  print('Is streaming: ${cancelResult.isStreaming}');

  // 3. Error recovery and chunk timeout.
  print('\n--- SCENARIO 3: Error Recovery and Chunk Timeout ---');
  final delayedSource = StreamController<String>();

  unawaited(() async {
    delayedSource.add('First immediate chunk... ');
    // Introduce a delay longer than the chunkTimeout.
    await Future<void>.delayed(const Duration(milliseconds: 200));
    if (!delayedSource.isClosed) {
      delayedSource.add('Late chunk that should not arrive.');
      await delayedSource.close();
    }
  }());

  final errorResult = await controller.bind(
    delayedSource.stream,
    chunkTimeout: const Duration(milliseconds: 80),
  );

  print('[FAILED] Result phase: ${errorResult.phase}');
  print('Recovered error: ${errorResult.error}');
  print('Is active: ${errorResult.isActive}');
  print('Is terminal: ${errorResult.isTerminal}');

  await delayedSource.close();

  // 4. Multi-turn text accumulation with appendToExisting.
  print('\n--- SCENARIO 4: Multi-Turn Stream Accumulation ---');
  controller.reset();

  await controller.bind(
    Stream.fromIterable(['Turn 1: Initial query response.\n']),
  );
  print('After Turn 1:\n${controller.current.text}');

  await controller.bind(
    Stream.fromIterable(['Turn 2: Follow-up response appended seamlessly.\n']),
    appendToExisting: true,
  );
  print('After Turn 2 (appended):\n${controller.current.text}');
  print('Total accumulated tokens: ${controller.current.tokenCount}');

  // Clean up resources.
  await subscription.cancel();
  await controller.dispose();
  print('\nController disposed safely.');
  print('================================================================');
}
