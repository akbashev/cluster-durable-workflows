import DistributedCluster
import Foundation

/// Pure run-level decisions, extracted from `WorkflowActor` so they can be
/// tested without booting a cluster. The actor's job is only to journal and
/// perform what these functions decide.

/// What should happen after a workflow run throws.
enum RunFailureDecision: Equatable, Sendable {
  /// Schedule attempt N at an absolute wall-clock deadline.
  case retry(attempt: Int, deadline: Date)
  /// Go terminal with this message.
  case fail(message: String)
}

/// Retries are possible when a policy is in place, the error is retryable
/// (`ApplicationError.typed` marked non-retryable, or whose type is listed in
/// the policy, fails immediately), and attempts remain (`maximumAttempts`
/// counts the first run). The Nth retry uses the Nth backoff interval; the
/// journaled deadline — not this recomputation — is the source of truth once
/// emitted. Jitter is rolled here, once, live; replay never re-rolls.
func decideOnRunFailure(
  error: Error,
  retry: RetryState?,
  now: Date
) -> RunFailureDecision {
  let message: String
  let isRetryable: Bool
  if case .typed(let m, let type, let nonRetryable) = error as? ApplicationError {
    message = m
    isRetryable =
      !nonRetryable
      && !(retry?.policy.nonRetryableErrorTypes.contains(type) ?? false)
  } else {
    message = error.localizedDescription
    isRetryable = true
  }

  guard let retry,
    isRetryable,
    retry.attempt + 1 < retry.policy.maximumAttempts
  else {
    return .fail(message: message)
  }

  let attempt = retry.attempt + 1
  var backoff = retry.policy.backoffStrategy()
  for _ in 1..<attempt { _ = backoff.next() }
  guard let interval = backoff.next() else {
    return .fail(message: message)
  }
  return .retry(attempt: attempt, deadline: now.addingTimeInterval(TimeInterval(interval)))
}

/// Activity outcomes replayable in the CURRENT run: all successes, plus
/// failures recorded after the latest `executionStarted`. Failures from
/// older runs are excluded so a new attempt re-dispatches the activity;
/// within a resumed run the journaled failure must rethrow, because the
/// workflow may have caught it and branched (see `WorkflowContext.executeActivity`).
func currentRunCachedOutcomes(
  events: [WorkflowEvent],
  outcomes: [ActivityKey: ActivityOutcomeRecord]
) -> [ActivityKey: ActivityOutcomeRecord] {
  var currentRunFailures: Set<ActivityKey> = []
  for event in events.reversed() {
    if case .executionStarted = event { break }
    if case .activityFailed(let key, _) = event { currentRunFailures.insert(key) }
  }
  return outcomes.filter { key, outcome in
    switch outcome {
    case .success: true
    case .failure: currentRunFailures.contains(key)
    }
  }
}
