import Foundation
import Testing

@testable import DurableWorkflows

/// Table-driven tests for the pure run decisions — no cluster, no journal.
@Suite
struct RunDecisionTests {

  private let now = Date()

  private func policy(
    initialInterval: Duration = .milliseconds(200),
    maximumAttempts: Int = 3,
    nonRetryableErrorTypes: [String] = []
  ) -> RetryPolicy {
    RetryPolicy(
      initialInterval: initialInterval,
      maximumAttempts: maximumAttempts,
      nonRetryableErrorTypes: nonRetryableErrorTypes
    )
  }

  @Test
  func noPolicyFailsImmediately() {
    struct Boom: Error {}
    let decision = decideOnRunFailure(error: Boom(), retry: nil, now: self.now)
    guard case .fail = decision else {
      Issue.record("expected fail, got \(decision)")
      return
    }
  }

  @Test
  func typedErrorMessageIsUsedForFailure() {
    let error = ApplicationError.typed(message: "boom", type: "Boom", isNonRetryable: false)
    let decision = decideOnRunFailure(error: error, retry: nil, now: self.now)
    #expect(decision == .fail(message: "boom"))
  }

  @Test
  func retrySchedulesNextAttemptWithinJitterBounds() {
    let retry = RetryState(policy: self.policy(), attempt: 0)
    struct Boom: Error {}
    let decision = decideOnRunFailure(error: Boom(), retry: retry, now: self.now)
    guard case .retry(let attempt, let deadline) = decision else {
      Issue.record("expected retry, got \(decision)")
      return
    }
    #expect(attempt == 1)
    // initialInterval 200ms with randomFactor 0.25 → 150…250ms out.
    let delay = deadline.timeIntervalSince(self.now)
    #expect(delay >= 0.15 && delay <= 0.25)
  }

  @Test
  func backoffGrowsWithAttemptNumber() {
    var retry = RetryState(policy: self.policy(maximumAttempts: 5), attempt: 2)
    struct Boom: Error {}
    guard
      case .retry(let attempt, let deadline) = decideOnRunFailure(
        error: Boom(),
        retry: retry,
        now: self.now
      )
    else {
      Issue.record("expected retry")
      return
    }
    #expect(attempt == 3)
    // Third interval: 200ms * 1.5^2 = 450ms, ±25% jitter → 337.5…562.5ms.
    let delay = deadline.timeIntervalSince(self.now)
    #expect(delay >= 0.33 && delay <= 0.57)
    retry.attempt = attempt
    _ = retry
  }

  @Test
  func exhaustedAttemptsFail() {
    // maximumAttempts counts the first run: attempt 2 of 2 → no retry left.
    let retry = RetryState(policy: self.policy(maximumAttempts: 2), attempt: 1)
    let error = ApplicationError.typed(message: "boom", type: "Boom", isNonRetryable: false)
    #expect(decideOnRunFailure(error: error, retry: retry, now: self.now) == .fail(message: "boom"))
  }

  @Test
  func nonRetryableFlagFailsEvenWithAttemptsRemaining() {
    let retry = RetryState(policy: self.policy(), attempt: 0)
    let error = ApplicationError.typed(message: "nope", type: "Fatal", isNonRetryable: true)
    #expect(decideOnRunFailure(error: error, retry: retry, now: self.now) == .fail(message: "nope"))
  }

  @Test
  func listedErrorTypeFailsEvenWithAttemptsRemaining() {
    let retry = RetryState(
      policy: self.policy(nonRetryableErrorTypes: ["ValidationError"]),
      attempt: 0
    )
    let error = ApplicationError.typed(message: "bad input", type: "ValidationError", isNonRetryable: false)
    #expect(decideOnRunFailure(error: error, retry: retry, now: self.now) == .fail(message: "bad input"))
  }
}

@Suite
struct CurrentRunCachedOutcomesTests {

  private func key(_ name: String) -> ActivityKey {
    ActivityKey(name: name, inputData: Data(name.utf8))
  }

  private func failure(_ message: String) -> ActivityFailurePayload {
    ActivityFailurePayload(message: message, type: "Boom", isNonRetryable: false)
  }

  @Test
  func failuresFromOlderRunsAreDroppedSuccessesKept() {
    let old = self.key("old")
    let recent = self.key("recent")
    let succeeded = self.key("succeeded")
    let events: [WorkflowEvent] = [
      .executionStarted(inputData: Data()),
      .activityFailed(key: old, failure: self.failure("old run failure")),
      .activitySucceeded(key: succeeded, outputData: Data()),
      .executionFailed(message: "old run failure"),
      // New run boundary — everything above is history.
      .executionStarted(inputData: Data()),
      .activityFailed(key: recent, failure: self.failure("this run failure")),
    ]
    let outcomes: [ActivityKey: ActivityOutcomeRecord] = [
      old: .failure(self.failure("old run failure")),
      recent: .failure(self.failure("this run failure")),
      succeeded: .success(outputData: Data()),
    ]

    let cached = currentRunCachedOutcomes(events: events, outcomes: outcomes)

    // Old failure: dropped → a new attempt re-dispatches the activity.
    #expect(cached[old] == nil)
    // This run's failure: kept → replay rethrows it (the workflow may have
    // caught it and branched).
    guard case .failure = cached[recent] else {
      Issue.record("expected recent failure to be cached")
      return
    }
    // Successes survive run boundaries — that's what makes retries cheap.
    guard case .success = cached[succeeded] else {
      Issue.record("expected success to be cached")
      return
    }
  }
}
