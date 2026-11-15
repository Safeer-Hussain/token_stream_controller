/// Exceptions thrown by the token stream controller.
library;

/// Exception thrown when a token stream operation fails or times out.
class TokenStreamException implements Exception {
  /// The descriptive error message.
  final String message;

  /// The underlying error or cause, if any.
  final Object? cause;

  /// Creates a new [TokenStreamException] with [message] and an optional [cause].
  const TokenStreamException(this.message, {this.cause});

  @override
  String toString() {
    if (cause != null) {
      return 'TokenStreamException: $message (cause: $cause)';
    }
    return 'TokenStreamException: $message';
  }
}
