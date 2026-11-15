/// Lifecycle phases for token stream processing.
library;

/// Represents the deterministic lifecycle phases of a token stream.
enum TokenStreamPhase {
  /// Controller is initialized or reset, awaiting input.
  idle,

  /// Transient reasoning or context-retrieval phase prior to the first output token.
  thinking,

  /// Actively receiving and concatenating output token chunks.
  streaming,

  /// Upstream token source closed normally.
  completed,

  /// Execution aborted explicitly via cancellation.
  cancelled,

  /// Upstream token source threw an error or exceeded chunk timeout.
  failed,
}
