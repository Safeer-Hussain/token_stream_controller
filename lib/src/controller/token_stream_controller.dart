/// Core controller implementation for managing asynchronous token streams.
library;

import 'dart:async';

import '../errors/token_stream_exception.dart';
import '../models/token_stream_phase.dart';
import '../models/token_stream_snapshot.dart';

/// A pure Dart asynchronous controller for streaming tokens with transient reasoning phases,
/// throughput telemetry, cancellation, and guaranteed loading-state cleanup.
class TokenStreamController {
  final StreamController<TokenStreamSnapshot> _streamController;
  TokenStreamSnapshot _current;
  StreamSubscription<String>? _subscription;
  Completer<TokenStreamSnapshot>? _currentCompleter;
  Stopwatch? _stopwatch;
  bool _isDisposed = false;

  /// Creates a new [TokenStreamController] instance in the idle phase.
  TokenStreamController()
      : _streamController = StreamController<TokenStreamSnapshot>.broadcast(),
        _current = const TokenStreamSnapshot.idle();

  /// Broadcast stream emitting state snapshots on every phase transition or token chunk.
  Stream<TokenStreamSnapshot> get stream => _streamController.stream;

  /// The current immutable snapshot of the stream state.
  TokenStreamSnapshot get current => _current;

  /// Whether this controller has been disposed.
  bool get isDisposed => _isDisposed;

  /// Binds an asynchronous [tokenSource] stream to this controller.
  ///
  /// If [initialReasoningStatus] is provided, the controller transitions to
  /// [TokenStreamPhase.thinking] before output tokens arrive. Once the first
  /// token is received, the reasoning status is automatically cleared and the
  /// phase transitions to [TokenStreamPhase.streaming].
  ///
  /// If [chunkTimeout] is specified, a [TokenStreamException] will be raised if
  /// the duration between chunks exceeds this limit, cleanly transitioning the
  /// phase to [TokenStreamPhase.failed].
  ///
  /// If [appendToExisting] is `true`, newly received tokens and counts will be
  /// appended to the existing accumulated state rather than resetting to empty.
  ///
  /// Optional [onChunk] callback is triggered synchronously for each newly
  /// emitted token chunk snapshot.
  ///
  /// Throws a [StateError] if this controller has already been disposed.
  Future<TokenStreamSnapshot> bind(
    Stream<String> tokenSource, {
    String? initialReasoningStatus,
    Duration? chunkTimeout,
    bool appendToExisting = false,
    void Function(TokenStreamSnapshot snapshot)? onChunk,
  }) async {
    if (_isDisposed) {
      throw StateError('Cannot bind to a disposed TokenStreamController.');
    }

    _cleanupPriorActive();

    final completer = Completer<TokenStreamSnapshot>();
    _currentCompleter = completer;

    final stopwatch = Stopwatch()..start();
    _stopwatch = stopwatch;

    final initialText = appendToExisting ? _current.text : '';
    final initialTokenCount = appendToExisting ? _current.tokenCount : 0;
    var currentText = initialText;
    var currentTokenCount = initialTokenCount;

    if (initialReasoningStatus != null) {
      _emit(
        TokenStreamSnapshot(
          phase: TokenStreamPhase.thinking,
          text: currentText,
          reasoningStatus: initialReasoningStatus,
          tokenCount: currentTokenCount,
          elapsed: Duration.zero,
        ),
      );
    } else {
      _emit(
        TokenStreamSnapshot(
          phase: TokenStreamPhase.streaming,
          text: currentText,
          reasoningStatus: null,
          tokenCount: currentTokenCount,
          elapsed: Duration.zero,
        ),
      );
    }

    try {
      Stream<String> effectiveStream = tokenSource;
      if (chunkTimeout != null) {
        effectiveStream = effectiveStream.timeout(
          chunkTimeout,
          onTimeout: (sink) {
            sink.addError(
              TokenStreamException(
                'Chunk timeout of ${chunkTimeout.inMilliseconds}ms exceeded',
              ),
            );
          },
        );
      }

      _subscription = effectiveStream.listen(
        (chunk) {
          if (completer != _currentCompleter) {
            return;
          }
          currentText += chunk;
          currentTokenCount++;
          final snapshot = TokenStreamSnapshot(
            phase: TokenStreamPhase.streaming,
            text: currentText,
            reasoningStatus: null,
            tokenCount: currentTokenCount,
            elapsed: stopwatch.elapsed,
          );
          _emit(snapshot);
          if (onChunk != null) {
            try {
              onChunk(snapshot);
            } catch (chunkError, chunkStackTrace) {
              final failureSnapshot = TokenStreamSnapshot(
                phase: TokenStreamPhase.failed,
                text: currentText,
                reasoningStatus: null,
                tokenCount: currentTokenCount,
                elapsed: stopwatch.elapsed,
                error: chunkError,
                stackTrace: chunkStackTrace,
              );
              _emit(failureSnapshot);
              if (!completer.isCompleted) {
                completer.complete(failureSnapshot);
              }
              _cleanupPriorActive();
            }
          }
        },
        onError: (Object error, StackTrace stackTrace) {
          if (completer != _currentCompleter) {
            return;
          }
          final effectiveError = error is TimeoutException
              ? TokenStreamException(
                  'Chunk timeout of ${chunkTimeout?.inMilliseconds}ms exceeded',
                  cause: error,
                )
              : error;
          final snapshot = TokenStreamSnapshot(
            phase: TokenStreamPhase.failed,
            text: currentText,
            reasoningStatus: null,
            tokenCount: currentTokenCount,
            elapsed: stopwatch.elapsed,
            error: effectiveError,
            stackTrace: stackTrace,
          );
          _emit(snapshot);
          if (!completer.isCompleted) {
            completer.complete(snapshot);
          }
        },
        onDone: () {
          if (completer != _currentCompleter) {
            return;
          }
          if (_current.phase != TokenStreamPhase.cancelled &&
              _current.phase != TokenStreamPhase.failed) {
            final snapshot = TokenStreamSnapshot(
              phase: TokenStreamPhase.completed,
              text: currentText,
              reasoningStatus: null,
              tokenCount: currentTokenCount,
              elapsed: stopwatch.elapsed,
            );
            _emit(snapshot);
          }
          if (!completer.isCompleted) {
            completer.complete(_current);
          }
        },
        cancelOnError: true,
      );

      return await completer.future;
    } catch (error, stackTrace) {
      final effectiveError = error is TimeoutException
          ? TokenStreamException(
              'Chunk timeout of ${chunkTimeout?.inMilliseconds}ms exceeded',
              cause: error,
            )
          : error;
      final snapshot = TokenStreamSnapshot(
        phase: TokenStreamPhase.failed,
        text: currentText,
        reasoningStatus: null,
        tokenCount: currentTokenCount,
        elapsed: stopwatch.elapsed,
        error: effectiveError,
        stackTrace: stackTrace,
      );
      _emit(snapshot);
      return snapshot;
    } finally {
      stopwatch.stop();
      if (_currentCompleter == completer) {
        _currentCompleter = null;
        final sub = _subscription;
        _subscription = null;
        if (sub != null) {
          await sub.cancel();
        }
      }
      if (_current.isActive) {
        _emit(
          _current.copyWith(
            phase: TokenStreamPhase.completed,
            clearReasoningStatus: true,
          ),
        );
      } else if (_current.reasoningStatus != null) {
        _emit(
          _current.copyWith(
            clearReasoningStatus: true,
          ),
        );
      }
    }
  }

  /// Deterministically simulates token streaming from a static text string.
  ///
  /// Useful for local unit testing, user interface prototyping, and offline demos.
  ///
  /// If [reasoningDelay] is supplied and greater than zero, the controller will
  /// remain in [TokenStreamPhase.thinking] with [reasoningStatus] until that delay
  /// elapses before the first token is emitted.
  ///
  /// Text is chunked according to [chunkPattern], defaulting to whitespace-delimited words.
  Future<TokenStreamSnapshot> simulateText(
    String fullText, {
    Duration tokenDelay = const Duration(milliseconds: 20),
    Duration? reasoningDelay,
    String? reasoningStatus,
    RegExp? chunkPattern,
  }) {
    final List<String> tokens;
    if (fullText.isEmpty) {
      tokens = const <String>[];
    } else if (chunkPattern != null) {
      final matches = chunkPattern.allMatches(fullText);
      tokens = matches.isNotEmpty
          ? matches.map((m) => m.group(0)!).toList()
          : <String>[fullText];
    } else {
      final pattern = RegExp(r'\S+\s*|\s+');
      final matches = pattern.allMatches(fullText);
      tokens = matches.isNotEmpty
          ? matches.map((m) => m.group(0)!).toList()
          : <String>[fullText];
    }

    Stream<String> generateTokenStream() async* {
      if (reasoningDelay != null && reasoningDelay > Duration.zero) {
        await Future<void>.delayed(reasoningDelay);
      }
      for (final token in tokens) {
        if (tokenDelay > Duration.zero) {
          await Future<void>.delayed(tokenDelay);
        }
        yield token;
      }
    }

    return bind(
      generateTokenStream(),
      initialReasoningStatus: reasoningStatus,
    );
  }

  /// Updates the reasoning status message while in the [TokenStreamPhase.thinking] phase.
  ///
  /// Has no effect if the controller is not currently in the thinking phase.
  /// Throws a [StateError] if this controller has already been disposed.
  void updateReasoningStatus(String status) {
    if (_isDisposed) {
      throw StateError(
        'Cannot update reasoning status on a disposed TokenStreamController.',
      );
    }
    if (_current.phase == TokenStreamPhase.thinking) {
      final snapshot = _current.copyWith(reasoningStatus: status);
      _emit(snapshot);
    }
  }

  /// Aborts the active token stream, transitioning immediately to [TokenStreamPhase.cancelled].
  ///
  /// Active subscriptions are cancelled and loading states are guaranteed to be cleaned up.
  /// Throws a [StateError] if this controller has already been disposed.
  Future<void> cancel() async {
    if (_isDisposed) {
      throw StateError('Cannot cancel a disposed TokenStreamController.');
    }
    if (!_current.isActive &&
        _subscription == null &&
        (_currentCompleter == null || _currentCompleter!.isCompleted)) {
      return;
    }

    final sub = _subscription;
    _subscription = null;
    if (sub != null) {
      await sub.cancel();
    }
    _stopwatch?.stop();

    final snapshot = _current.copyWith(
      phase: TokenStreamPhase.cancelled,
      clearReasoningStatus: true,
    );
    _emit(snapshot);

    if (_currentCompleter != null && !_currentCompleter!.isCompleted) {
      _currentCompleter!.complete(snapshot);
    }
    _currentCompleter = null;
  }

  /// Cancels any active stream and resets the state to [TokenStreamSnapshot.idle].
  ///
  /// Throws a [StateError] if this controller has already been disposed.
  void reset() {
    if (_isDisposed) {
      throw StateError('Cannot reset a disposed TokenStreamController.');
    }
    final sub = _subscription;
    _subscription = null;
    if (sub != null) {
      unawaited(sub.cancel());
    }
    _stopwatch?.stop();
    _stopwatch = null;

    if (_currentCompleter != null && !_currentCompleter!.isCompleted) {
      _currentCompleter!.complete(const TokenStreamSnapshot.idle());
    }
    _currentCompleter = null;

    _emit(const TokenStreamSnapshot.idle());
  }

  /// Disposes this controller and closes its broadcast stream.
  ///
  /// Subsequent calls to [bind], [updateReasoningStatus], [cancel], or [reset]
  /// will throw a [StateError].
  Future<void> dispose() async {
    if (_isDisposed) {
      return;
    }
    _isDisposed = true;

    final sub = _subscription;
    _subscription = null;
    if (sub != null) {
      await sub.cancel();
    }

    _stopwatch?.stop();
    _stopwatch = null;

    if (_currentCompleter != null && !_currentCompleter!.isCompleted) {
      _currentCompleter!.complete(_current);
    }
    _currentCompleter = null;

    await _streamController.close();
  }

  void _cleanupPriorActive() {
    final sub = _subscription;
    _subscription = null;
    if (sub != null) {
      unawaited(sub.cancel());
    }
    _stopwatch?.stop();
    if (_currentCompleter != null && !_currentCompleter!.isCompleted) {
      final cancelledSnapshot = _current.copyWith(
        phase: TokenStreamPhase.cancelled,
        clearReasoningStatus: true,
      );
      _currentCompleter!.complete(cancelledSnapshot);
    }
    _currentCompleter = null;
  }

  void _emit(TokenStreamSnapshot snapshot) {
    _current = snapshot;
    if (!_streamController.isClosed) {
      _streamController.add(snapshot);
    }
  }
}
