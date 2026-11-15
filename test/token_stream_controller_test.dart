import 'dart:async';

import 'package:test/test.dart';
import 'package:token_stream_controller/token_stream_controller.dart';

void main() {
  group('TokenStreamSnapshot', () {
    test('initial idle snapshot has default properties', () {
      const snapshot = TokenStreamSnapshot.idle();

      expect(snapshot.phase, equals(TokenStreamPhase.idle));
      expect(snapshot.text, isEmpty);
      expect(snapshot.reasoningStatus, isNull);
      expect(snapshot.tokenCount, equals(0));
      expect(snapshot.elapsed, equals(Duration.zero));
      expect(snapshot.error, isNull);
      expect(snapshot.stackTrace, isNull);
      expect(snapshot.isActive, isFalse);
      expect(snapshot.isThinking, isFalse);
      expect(snapshot.isStreaming, isFalse);
      expect(snapshot.isTerminal, isFalse);
      expect(snapshot.tokensPerSecond, equals(0.0));
    });

    test('tokensPerSecond calculates correctly for various elapsed durations',
        () {
      const zeroElapsed = TokenStreamSnapshot(
        phase: TokenStreamPhase.streaming,
        tokenCount: 10,
        elapsed: Duration.zero,
      );
      expect(zeroElapsed.tokensPerSecond, equals(0.0));

      const negativeElapsed = TokenStreamSnapshot(
        phase: TokenStreamPhase.streaming,
        tokenCount: 10,
        elapsed: Duration(milliseconds: -100),
      );
      expect(negativeElapsed.tokensPerSecond, equals(0.0));

      const standardRate = TokenStreamSnapshot(
        phase: TokenStreamPhase.streaming,
        tokenCount: 40,
        elapsed: Duration(milliseconds: 2000),
      );
      expect(standardRate.tokensPerSecond, closeTo(20.0, 0.001));

      const fastRate = TokenStreamSnapshot(
        phase: TokenStreamPhase.streaming,
        tokenCount: 100,
        elapsed: Duration(milliseconds: 500),
      );
      expect(fastRate.tokensPerSecond, closeTo(200.0, 0.001));
    });

    test('lifecycle helper getters reflect active and terminal phases', () {
      const thinking = TokenStreamSnapshot(
        phase: TokenStreamPhase.thinking,
        reasoningStatus: 'Analyzing request',
      );
      expect(thinking.isActive, isTrue);
      expect(thinking.isThinking, isTrue);
      expect(thinking.isStreaming, isFalse);
      expect(thinking.isTerminal, isFalse);

      const streaming = TokenStreamSnapshot(
        phase: TokenStreamPhase.streaming,
      );
      expect(streaming.isActive, isTrue);
      expect(streaming.isThinking, isFalse);
      expect(streaming.isStreaming, isTrue);
      expect(streaming.isTerminal, isFalse);

      const completed = TokenStreamSnapshot(
        phase: TokenStreamPhase.completed,
      );
      expect(completed.isActive, isFalse);
      expect(completed.isTerminal, isTrue);

      const cancelled = TokenStreamSnapshot(
        phase: TokenStreamPhase.cancelled,
      );
      expect(cancelled.isActive, isFalse);
      expect(cancelled.isTerminal, isTrue);

      const failed = TokenStreamSnapshot(
        phase: TokenStreamPhase.failed,
      );
      expect(failed.isActive, isFalse);
      expect(failed.isTerminal, isTrue);
    });

    test(
        'copyWith updates specified fields and supports clearing nullable fields',
        () {
      const initial = TokenStreamSnapshot(
        phase: TokenStreamPhase.thinking,
        text: 'Initial',
        reasoningStatus: 'Reasoning',
        tokenCount: 2,
        elapsed: Duration(seconds: 1),
        error: 'Err',
      );

      final updated = initial.copyWith(
        phase: TokenStreamPhase.streaming,
        text: 'Updated',
        clearReasoningStatus: true,
        tokenCount: 5,
        clearError: true,
      );

      expect(updated.phase, equals(TokenStreamPhase.streaming));
      expect(updated.text, equals('Updated'));
      expect(updated.reasoningStatus, isNull);
      expect(updated.tokenCount, equals(5));
      expect(updated.elapsed, equals(const Duration(seconds: 1)));
      expect(updated.error, isNull);
    });

    test('equality and hashCode compare field values correctly', () {
      const a = TokenStreamSnapshot(
        phase: TokenStreamPhase.streaming,
        text: 'test',
        tokenCount: 1,
      );
      const b = TokenStreamSnapshot(
        phase: TokenStreamPhase.streaming,
        text: 'test',
        tokenCount: 1,
      );
      const c = TokenStreamSnapshot(
        phase: TokenStreamPhase.streaming,
        text: 'different',
        tokenCount: 1,
      );

      expect(a, equals(b));
      expect(a.hashCode, equals(b.hashCode));
      expect(a, isNot(equals(c)));
      expect(a.toString(), contains('TokenStreamSnapshot'));
    });
  });

  group('TokenStreamException', () {
    test('formats message correctly with and without cause', () {
      const simple = TokenStreamException('Operation failed');
      expect(simple.message, equals('Operation failed'));
      expect(simple.cause, isNull);
      expect(
          simple.toString(), equals('TokenStreamException: Operation failed'));

      const cause = FormatException('Bad format');
      const withCause = TokenStreamException('Parse timeout', cause: cause);
      expect(withCause.message, equals('Parse timeout'));
      expect(withCause.cause, equals(cause));
      expect(
        withCause.toString(),
        equals(
            'TokenStreamException: Parse timeout (cause: FormatException: Bad format)'),
      );
    });
  });

  group('TokenStreamController', () {
    late TokenStreamController controller;

    setUp(() {
      controller = TokenStreamController();
    });

    tearDown(() async {
      await controller.dispose();
    });

    test('initializes with idle snapshot state', () {
      expect(controller.current.phase, equals(TokenStreamPhase.idle));
      expect(controller.current.text, isEmpty);
      expect(controller.isDisposed, isFalse);
    });

    test('starts streaming immediately when initialReasoningStatus is null',
        () async {
      final phases = <TokenStreamPhase>[];
      final subscription = controller.stream.listen((s) => phases.add(s.phase));

      final source = Stream.fromIterable(['Hello', ' ', 'World']);
      final finalSnapshot = await controller.bind(source);

      expect(finalSnapshot.phase, equals(TokenStreamPhase.completed));
      expect(finalSnapshot.text, equals('Hello World'));
      expect(finalSnapshot.tokenCount, equals(3));
      expect(phases.first, equals(TokenStreamPhase.streaming));
      expect(phases.last, equals(TokenStreamPhase.completed));

      await subscription.cancel();
    });

    test(
        'transitions through thinking phase and clears reasoningStatus on first token',
        () async {
      final emittedReasoning = <String?>[];
      final emittedPhases = <TokenStreamPhase>[];

      final subscription = controller.stream.listen((s) {
        emittedPhases.add(s.phase);
        emittedReasoning.add(s.reasoningStatus);
      });

      final streamController = StreamController<String>();
      final bindFuture = controller.bind(
        streamController.stream,
        initialReasoningStatus: 'Searching memory',
      );

      expect(controller.current.phase, equals(TokenStreamPhase.thinking));
      expect(controller.current.reasoningStatus, equals('Searching memory'));

      controller.updateReasoningStatus('Retrieving documents');
      expect(
          controller.current.reasoningStatus, equals('Retrieving documents'));

      streamController.add('Output token');
      await Future<void>.delayed(const Duration(milliseconds: 10));

      expect(controller.current.phase, equals(TokenStreamPhase.streaming));
      expect(controller.current.reasoningStatus, isNull);
      expect(controller.current.text, equals('Output token'));

      await streamController.close();
      final finalSnapshot = await bindFuture;

      expect(finalSnapshot.phase, equals(TokenStreamPhase.completed));
      expect(finalSnapshot.reasoningStatus, isNull);
      expect(emittedPhases, contains(TokenStreamPhase.thinking));
      expect(emittedPhases, contains(TokenStreamPhase.streaming));
      expect(emittedPhases.last, equals(TokenStreamPhase.completed));

      await subscription.cancel();
    });

    test('updateReasoningStatus does nothing outside thinking phase', () {
      controller.updateReasoningStatus('Should be ignored');
      expect(controller.current.reasoningStatus, isNull);
    });

    test('invokes onChunk callback for each incoming token chunk', () async {
      final chunks = <String>[];
      final source = Stream.fromIterable(['A', 'B', 'C']);

      await controller.bind(
        source,
        onChunk: (snapshot) {
          chunks.add(snapshot.text);
        },
      );

      expect(chunks, equals(['A', 'AB', 'ABC']));
    });

    test(
        'appendToExisting accumulates text and token counts across multiple binds',
        () async {
      final firstSource = Stream.fromIterable(['First', ' ']);
      final firstSnapshot = await controller.bind(firstSource);
      expect(firstSnapshot.text, equals('First '));
      expect(firstSnapshot.tokenCount, equals(2));

      final secondSource = Stream.fromIterable(['Second', ' Part']);
      final secondSnapshot = await controller.bind(
        secondSource,
        appendToExisting: true,
      );

      expect(secondSnapshot.text, equals('First Second Part'));
      expect(secondSnapshot.tokenCount, equals(4));
      expect(controller.current.text, equals('First Second Part'));
    });

    test(
        'guarantees cleanup and failed phase when upstream stream throws an error',
        () async {
      final streamController = StreamController<String>();
      final bindFuture = controller.bind(streamController.stream);

      streamController.add('Valid chunk');
      await Future<void>.delayed(const Duration(milliseconds: 10));

      streamController.addError(Exception('Network connection severed'));
      final result = await bindFuture;

      expect(result.phase, equals(TokenStreamPhase.failed));
      expect(result.isStreaming, isFalse);
      expect(result.isActive, isFalse);
      expect(result.text, equals('Valid chunk'));
      expect(result.error, isA<Exception>());
      expect(result.stackTrace, isNotNull);
      expect(result.reasoningStatus, isNull);

      await streamController.close();
    });

    test('chunkTimeout triggers TokenStreamException and transitions to failed',
        () async {
      final streamController = StreamController<String>();
      final bindFuture = controller.bind(
        streamController.stream,
        chunkTimeout: const Duration(milliseconds: 50),
      );

      streamController.add('Initial chunk');
      await Future<void>.delayed(const Duration(milliseconds: 20));

      // Wait longer than chunkTimeout without sending any chunk.
      await Future<void>.delayed(const Duration(milliseconds: 100));

      final result = await bindFuture;

      expect(result.phase, equals(TokenStreamPhase.failed));
      expect(result.isStreaming, isFalse);
      expect(result.isActive, isFalse);
      expect(result.error, isA<TokenStreamException>());
      expect(
        (result.error as TokenStreamException).message,
        contains('Chunk timeout of 50ms exceeded'),
      );

      await streamController.close();
    });

    test(
        'mid-stream cancel aborts remaining tokens and guarantees isStreaming is false',
        () async {
      final streamController = StreamController<String>();
      final bindFuture = controller.bind(streamController.stream);

      streamController.add('Token 1');
      await Future<void>.delayed(const Duration(milliseconds: 10));
      expect(controller.current.isStreaming, isTrue);

      await controller.cancel();

      expect(controller.current.phase, equals(TokenStreamPhase.cancelled));
      expect(controller.current.isStreaming, isFalse);
      expect(controller.current.isActive, isFalse);
      expect(controller.current.text, equals('Token 1'));

      // Pushing more tokens after cancel must not affect the controller.
      streamController.add('Ignored token');
      await Future<void>.delayed(const Duration(milliseconds: 10));

      expect(controller.current.text, equals('Token 1'));
      expect(controller.current.phase, equals(TokenStreamPhase.cancelled));

      final result = await bindFuture;
      expect(result.phase, equals(TokenStreamPhase.cancelled));

      await streamController.close();
    });

    test('cancel when idle or completed does not throw', () async {
      await expectLater(controller.cancel(), completes);

      await controller.bind(Stream.value('Done'));
      expect(controller.current.phase, equals(TokenStreamPhase.completed));

      await expectLater(controller.cancel(), completes);
      expect(controller.current.phase, equals(TokenStreamPhase.completed));
    });

    test('reset clears active state and returns controller to idle', () async {
      final streamController = StreamController<String>();
      unawaited(controller.bind(streamController.stream));

      streamController.add('In flight');
      await Future<void>.delayed(const Duration(milliseconds: 10));

      controller.reset();

      expect(controller.current.phase, equals(TokenStreamPhase.idle));
      expect(controller.current.text, isEmpty);
      expect(controller.current.tokenCount, equals(0));
      expect(controller.current.isActive, isFalse);

      await streamController.close();
    });

    test('dispose cancels subscriptions and forbids further operations',
        () async {
      await controller.dispose();
      expect(controller.isDisposed, isTrue);

      expect(
        () => controller.bind(Stream.value('Test')),
        throwsA(isA<StateError>()),
      );
      expect(
        () => controller.updateReasoningStatus('New status'),
        throwsA(isA<StateError>()),
      );
      expect(
        () => controller.cancel(),
        throwsA(isA<StateError>()),
      );
      expect(
        () => controller.reset(),
        throwsA(isA<StateError>()),
      );

      // Calling dispose a second time is safely idempotent.
      await expectLater(controller.dispose(), completes);
    });

    test('simulateText chunks words and streams deterministically', () async {
      const sampleText = 'Pure Dart asynchronous token stream controller';
      final result = await controller.simulateText(
        sampleText,
        tokenDelay: const Duration(milliseconds: 5),
      );

      expect(result.phase, equals(TokenStreamPhase.completed));
      expect(result.text, equals(sampleText));
      expect(result.tokenCount, equals(6));
      expect(result.tokensPerSecond, greaterThan(0.0));
      expect(result.isActive, isFalse);
    });

    test('simulateText handles empty string gracefully', () async {
      final result = await controller.simulateText(
        '',
        tokenDelay: const Duration(milliseconds: 5),
      );

      expect(result.phase, equals(TokenStreamPhase.completed));
      expect(result.text, isEmpty);
      expect(result.tokenCount, equals(0));
    });

    test('simulateText with reasoningDelay transitions through thinking phase',
        () async {
      final phases = <TokenStreamPhase>[];
      final sub = controller.stream.listen((s) => phases.add(s.phase));

      final result = await controller.simulateText(
        'Quick brown fox',
        tokenDelay: const Duration(milliseconds: 5),
        reasoningDelay: const Duration(milliseconds: 30),
        reasoningStatus: 'Reasoning about prompt',
      );

      expect(result.phase, equals(TokenStreamPhase.completed));
      expect(result.text, equals('Quick brown fox'));
      expect(phases, contains(TokenStreamPhase.thinking));
      expect(phases, contains(TokenStreamPhase.streaming));
      expect(phases.last, equals(TokenStreamPhase.completed));

      await sub.cancel();
    });

    test('simulateText with custom chunkPattern splits accordingly', () async {
      final result = await controller.simulateText(
        '12345',
        chunkPattern: RegExp(r'.'),
        tokenDelay: const Duration(milliseconds: 5),
      );

      expect(result.phase, equals(TokenStreamPhase.completed));
      expect(result.text, equals('12345'));
      expect(result.tokenCount, equals(5));
    });

    test('consecutive bind calls cancel previous active stream', () async {
      final stream1 = StreamController<String>();
      final stream2 = StreamController<String>();

      final future1 = controller.bind(stream1.stream);
      stream1.add('Stream1 chunk');
      await Future<void>.delayed(const Duration(milliseconds: 10));

      final future2 = controller.bind(stream2.stream);
      stream2.add('Stream2 chunk');
      await stream2.close();

      final result2 = await future2;
      expect(result2.phase, equals(TokenStreamPhase.completed));
      expect(result2.text, equals('Stream2 chunk'));

      final result1 = await future1;
      expect(result1.isActive, isFalse);

      await stream1.close();
    });
  });
}
