import DistributedCluster
import DurableWorkflows
import Foundation
import Testing

/// Automatic retry semantics over a real single-node cluster: a failed run is
/// retried with journaled backoff, and the retry re-dispatches only the
/// activity that never completed.
///
/// Serialized: `FlakySwitch`/`InvocationCounter` are process-global, and two
/// tests here drive `FlakyWorkflow` concurrently otherwise.
@Suite(.timeLimit(.minutes(1)), .serialized)
struct RetryWorkflowTests {

  @Test
  func failureRetriesAutomaticallyAndRedispatchesOnlyFailedActivity() async throws {
    let (system, node, worker) = try await makeFlakyWorkflowSystem(
      name: "flaky-retry",
      port: 4604,
      store: InMemoryEventStore()
    )
    let options = WorkflowOptions(
      id: "wf-flaky",
      retryPolicy: RetryPolicy(initialInterval: .milliseconds(50), maximumAttempts: 2)
    )

    // The activity fails once, then succeeds — the policy absorbs the failure.
    InvocationCounter.shared.reset("flaky")
    FlakySwitch.shared.failuresRemaining = 1
    defer { FlakySwitch.shared.failuresRemaining = 0 }
    let output = try await system.workflows.execute(
      type: FlakyWorkflow.self,
      options: options,
      input: .init()
    )
    #expect(output == "ok")
    // Dispatched twice live: the failed attempt and the retried one.
    #expect(InvocationCounter.shared.count("flaky") == 2)

    let info = try await system.workflows.getStatus(type: FlakyWorkflow.self, options: options)
    guard case .completed = info.status else {
      Issue.record("expected completed, got \(info.status)")
      return
    }
    let kinds: [String] = info.events.map { event in
      switch event {
      case .executionStarted: return "started"
      case .retryPolicyConfigured: return "policyConfigured"
      case .retryScheduled: return "retryScheduled"
      case .activityFailed: return "activityFailed"
      case .activitySucceeded: return "activitySucceeded"
      case .executionFailed: return "failed"
      case .executionCompleted: return "completed"
      default: return "other"
      }
    }
    #expect(
      kinds == [
        "started", "policyConfigured", "activityFailed", "retryScheduled",
        "started", "activitySucceeded", "completed",
      ]
    )

    _ = node
    _ = worker
  }

  @Test
  func retriesExhaustedFailsTheRun() async throws {
    let (system, node, worker) = try await makeFlakyWorkflowSystem(
      name: "flaky-exhausted",
      port: 4605,
      store: InMemoryEventStore()
    )
    let options = WorkflowOptions(
      id: "wf-flaky-exhausted",
      retryPolicy: RetryPolicy(initialInterval: .milliseconds(50), maximumAttempts: 2)
    )

    // Fails more often than the policy allows: attempt 1 + retry 1, then the
    // run goes terminal `.failed`.
    InvocationCounter.shared.reset("flaky")
    FlakySwitch.shared.failuresRemaining = 5
    defer { FlakySwitch.shared.failuresRemaining = 0 }
    await #expect(throws: ApplicationError.self) {
      try await system.workflows.execute(
        type: FlakyWorkflow.self,
        options: options,
        input: .init()
      )
    }
    #expect(InvocationCounter.shared.count("flaky") == 2)

    let info = try await system.workflows.getStatus(type: FlakyWorkflow.self, options: options)
    guard case .failed = info.status else {
      Issue.record("expected failed, got \(info.status)")
      return
    }

    _ = node
    _ = worker
  }

  /// A crash while parked in retry backoff must lose nothing: the journaled
  /// `retryScheduled` deadline survives, resume waits out only the remainder,
  /// and the retried attempt re-dispatches the failed activity exactly once.
  @Test
  func crashDuringRetryBackoffResumesAndRetries() async throws {
    let store = InMemoryEventStore()
    let (system, node, worker) = try await makeFlakyWorkflowSystem(
      name: "flaky-backoff",
      port: 4607,
      store: store
    )
    let options = WorkflowOptions(id: "wf-backoff")

    // Seed the journal exactly as a crashed node left it: attempt 1 failed,
    // retry #1 was scheduled 0.3s into the future, then the node died.
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    let activityKey = ActivityKey(name: "flaky", inputData: try encoder.encode("attempt"))
    let persistenceID = "flaky-wf-backoff"

    try await store.persistEvent(
      WorkflowEvent.executionStarted(inputData: try encoder.encode(FlakyWorkflow.Input())),
      id: persistenceID,
      sequenceNumber: 1
    )
    try await store.persistEvent(
      WorkflowEvent.retryPolicyConfigured(
        RetryPolicy(initialInterval: .milliseconds(50), maximumAttempts: 2)
      ),
      id: persistenceID,
      sequenceNumber: 2
    )
    try await store.persistEvent(
      WorkflowEvent.activityFailed(
        key: activityKey,
        failure: ActivityFailurePayload(
          message: "flaky activity failed",
          type: "FlakyError",
          isNonRetryable: false
        )
      ),
      id: persistenceID,
      sequenceNumber: 3
    )
    try await store.persistEvent(
      WorkflowEvent.retryScheduled(attempt: 1, deadline: Date(timeIntervalSinceNow: 0.3)),
      id: persistenceID,
      sequenceNumber: 4
    )

    // Activation folds the journal and resumes into the pending backoff.
    InvocationCounter.shared.reset("flaky")
    FlakySwitch.shared.failuresRemaining = 0
    let started = ContinuousClock.now
    try await eventually(interval: .milliseconds(100)) {
      let info = try await system.workflows.getStatus(type: FlakyWorkflow.self, options: options)
      if case .completed = info.status { return true }
      return false
    }
    #expect(ContinuousClock.now - started < .seconds(5))

    // The retry re-dispatched the failed activity — exactly once.
    #expect(InvocationCounter.shared.count("flaky") == 1)

    let info = try await system.workflows.getStatus(type: FlakyWorkflow.self, options: options)
    guard case .completed(let outputData) = info.status else {
      Issue.record("expected completed, got \(info.status)")
      return
    }
    #expect(try JSONDecoder().decode(String.self, from: outputData) == "ok")

    _ = node
    _ = worker
  }

  /// Cancelling a run parked in retry backoff must be prompt: the wait lives
  /// in the execution task that `cancel()` cancels — the caller must not sit
  /// out the remaining backoff.
  @Test
  func cancelDuringRetryBackoffIsPrompt() async throws {
    let (system, node, worker) = try await makeFlakyWorkflowSystem(
      name: "flaky-cancel-backoff",
      port: 4608,
      store: InMemoryEventStore()
    )
    let options = WorkflowOptions(
      id: "wf-cancel-backoff",
      retryPolicy: RetryPolicy(initialInterval: .seconds(30), maximumAttempts: 2)
    )
    InvocationCounter.shared.reset("flaky")
    FlakySwitch.shared.failuresRemaining = 1
    defer { FlakySwitch.shared.failuresRemaining = 0 }

    let execution = Task {
      try await system.workflows.execute(type: FlakyWorkflow.self, options: options, input: .init())
    }

    // Wait until the failed attempt has journaled its retry — the run is now
    // parked in a 30-second backoff.
    try await eventually(interval: .milliseconds(50)) {
      let info = try await system.workflows.getStatus(type: FlakyWorkflow.self, options: options)
      return info.events.contains { event in
        if case .retryScheduled = event { return true }
        return false
      }
    }

    let started = ContinuousClock.now
    try await system.workflows.cancel(type: FlakyWorkflow.self, options: options)

    await #expect(throws: CancellationError.self) {
      try await execution.value
    }
    #expect(ContinuousClock.now - started < .seconds(5))
    // The failed attempt ran once; the retry never fired.
    #expect(InvocationCounter.shared.count("flaky") == 1)

    let info = try await system.workflows.getStatus(type: FlakyWorkflow.self, options: options)
    guard case .cancelled = info.status else {
      Issue.record("expected cancelled, got \(info.status)")
      return
    }

    _ = node
    _ = worker
  }

  /// Two concurrent `execute` calls for the same workflow id must produce ONE
  /// run: the second joins the in-flight start instead of journaling a
  /// sibling `executionStarted` and re-dispatching every activity.
  @Test
  func concurrentExecuteStartsOnlyOneRun() async throws {
    let store = InMemoryEventStore()
    let gate = GatedEventStore(inner: store)
    let (system, node, worker) = try await makeFlakyWorkflowSystem(
      name: "flaky-race",
      port: 4609,
      store: gate
    )
    let options = WorkflowOptions(id: "wf-race")
    InvocationCounter.shared.reset("flaky")
    FlakySwitch.shared.failuresRemaining = 0

    // The first execute's `executionStarted` emit parks behind the gate; the
    // second arrives while the first is suspended mid-start.
    async let resultA = system.workflows.execute(
      type: FlakyWorkflow.self,
      options: options,
      input: .init()
    )
    try await Task.sleep(for: .milliseconds(100))
    async let resultB = system.workflows.execute(
      type: FlakyWorkflow.self,
      options: options,
      input: .init()
    )
    try await Task.sleep(for: .milliseconds(100))
    gate.open()

    let (outputA, outputB) = try await (resultA, resultB)
    #expect(outputA == "ok")
    #expect(outputB == "ok")

    let info = try await system.workflows.getStatus(type: FlakyWorkflow.self, options: options)
    let starts = info.events.filter { event in
      if case .executionStarted = event { return true }
      return false
    }
    #expect(starts.count == 1)
    #expect(InvocationCounter.shared.count("flaky") == 1)

    _ = node
    _ = worker
  }

  /// A cancelled CALLER (SSE disconnect, closed tab) must not kill the run:
  /// the workflow belongs to the journal, not to whichever task happened to
  /// start it. Regression test for the demo failure where closing the stream
  /// mid-backoff left the workflow `.running` with no live task — parked
  /// forever.
  @Test
  func callerCancellationDoesNotKillTheRun() async throws {
    let (system, node, worker) = try await makeFlakyWorkflowSystem(
      name: "flaky-caller-cancel",
      port: 4611,
      store: InMemoryEventStore()
    )
    let options = WorkflowOptions(
      id: "wf-caller-cancel",
      retryPolicy: RetryPolicy(initialInterval: .milliseconds(100), maximumAttempts: 2)
    )
    InvocationCounter.shared.reset("flaky")
    FlakySwitch.shared.failuresRemaining = 1
    defer { FlakySwitch.shared.failuresRemaining = 0 }

    let execution = Task {
      try await system.workflows.execute(type: FlakyWorkflow.self, options: options, input: .init())
    }

    // Wait until the failed attempt has journaled its retry — the run is now
    // parked in backoff — then cancel the CALLER, not the workflow.
    try await eventually(interval: .milliseconds(50)) {
      let info = try await system.workflows.getStatus(type: FlakyWorkflow.self, options: options)
      return info.events.contains { event in
        if case .retryScheduled = event { return true }
        return false
      }
    }
    execution.cancel()

    // The run survives its caller: the backoff fires, the retry re-dispatches,
    // and the workflow completes.
    try await eventually(interval: .milliseconds(100)) {
      let info = try await system.workflows.getStatus(type: FlakyWorkflow.self, options: options)
      if case .completed = info.status { return true }
      return false
    }
    #expect(InvocationCounter.shared.count("flaky") == 2)

    _ = node
    _ = worker
  }

  /// Pressing "start again" on a failed workflow — with a NEW input, as any
  /// real caller has (the demo's Connection actor differs per submission) —
  /// must start a fresh run, not throw `workflowInputMismatch`. The fresh run
  /// also gets a fresh retry budget: `retryPolicyConfigured` marks a
  /// caller-initiated start and resets the exhausted attempt counter.
  @Test
  func executeFromFailedWithNewInputStartsFreshRunWithFreshRetryBudget() async throws {
    let (system, node, worker) = try await makeFlakyWorkflowSystem(
      name: "flaky-restart",
      port: 4610,
      store: InMemoryEventStore()
    )
    let options = WorkflowOptions(
      id: "wf-restart",
      retryPolicy: RetryPolicy(initialInterval: .milliseconds(50), maximumAttempts: 2)
    )

    // Exhaust the policy: attempt 1 + retry 1 both fail, run goes terminal.
    InvocationCounter.shared.reset("flaky")
    FlakySwitch.shared.failuresRemaining = 10
    await #expect(throws: ApplicationError.self) {
      try await system.workflows.execute(
        type: FlakyWorkflow.self,
        options: options,
        input: .init(label: "v1")
      )
    }
    #expect(InvocationCounter.shared.count("flaky") == 2)
    let failed = try await system.workflows.getStatus(type: FlakyWorkflow.self, options: options)
    guard case .failed = failed.status else {
      Issue.record("expected failed, got \(failed.status)")
      return
    }

    // Re-execute with NEW input and one failure left: the fresh budget turns
    // it into fail-then-succeed. Without the budget reset this would fail
    // terminally on the first error; before the input-guard fix it threw
    // `workflowInputMismatch` instead of running at all.
    FlakySwitch.shared.failuresRemaining = 1
    defer { FlakySwitch.shared.failuresRemaining = 0 }
    let output = try await system.workflows.execute(
      type: FlakyWorkflow.self,
      options: options,
      input: .init(label: "v2")
    )
    #expect(output == "ok")
    #expect(InvocationCounter.shared.count("flaky") == 4)

    let info = try await system.workflows.getStatus(type: FlakyWorkflow.self, options: options)
    guard case .completed = info.status else {
      Issue.record("expected completed, got \(info.status)")
      return
    }
    let starts = info.events.filter {
      if case .executionStarted = $0 { return true }
      return false
    }
    let policies = info.events.filter {
      if case .retryPolicyConfigured = $0 { return true }
      return false
    }
    #expect(starts.count == 4)  // two attempts per run, two runs
    #expect(policies.count == 2)  // one per caller-initiated run

    _ = node
    _ = worker
  }

  /// The other side of the failure-replay contract: resuming the SAME run
  /// that caught an activity failure and continued must rethrow the journaled
  /// failure — never re-dispatch — or the replay can take a different branch
  /// than history recorded. (Only a NEW run, after a fresh executionStarted,
  /// may re-dispatch a failed activity.)
  @Test
  func resumeRethrowsJournaledFailureInsteadOfRedispatching() async throws {
    let store = InMemoryEventStore()
    let (system, node, worker) = try await makeCompensatingWorkflowSystem(
      name: "compensating-replay",
      port: 4606,
      store: store
    )
    let options = WorkflowOptions(id: "wf-compensated")

    // Seed the journal exactly as a crashed node left it: `primary` failed,
    // the workflow caught it, `fallback` succeeded, then it parked in sleep.
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    let activityInput = try encoder.encode("live")
    let primaryKey = ActivityKey(name: "primary", inputData: activityInput)
    let fallbackKey = ActivityKey(name: "fallback", inputData: activityInput)
    let persistenceID = "compensating-wf-compensated"

    try await store.persistEvent(
      WorkflowEvent.executionStarted(inputData: try encoder.encode(CompensatingWorkflow.Input())),
      id: persistenceID,
      sequenceNumber: 1
    )
    try await store.persistEvent(
      WorkflowEvent.activityFailed(
        key: primaryKey,
        failure: ActivityFailurePayload(
          message: "primary failed",
          type: "PrimaryError",
          isNonRetryable: false
        )
      ),
      id: persistenceID,
      sequenceNumber: 2
    )
    try await store.persistEvent(
      WorkflowEvent.activitySucceeded(
        key: fallbackKey,
        outputData: try encoder.encode("fallback live")
      ),
      id: persistenceID,
      sequenceNumber: 3
    )
    try await store.persistEvent(
      WorkflowEvent.timerScheduled(
        sequence: 0,
        duration: .seconds(30),
        deadline: Date(timeIntervalSinceNow: 0.3),
        summary: "post-compensation window"
      ),
      id: persistenceID,
      sequenceNumber: 4
    )

    // Activation folds the journal and resumes the run parked in the sleep.
    let started = ContinuousClock.now
    try await eventually(interval: .milliseconds(100)) {
      let info = try await system.workflows.getStatus(type: CompensatingWorkflow.self, options: options)
      if case .completed = info.status { return true }
      return false
    }
    #expect(ContinuousClock.now - started < .seconds(5))

    // Nothing was re-dispatched live — even though `primary` would succeed
    // now, replay rethrew its journaled failure and took the fallback branch.
    #expect(InvocationCounter.shared.count("primary") == 0)
    #expect(InvocationCounter.shared.count("fallback") == 0)

    let info = try await system.workflows.getStatus(type: CompensatingWorkflow.self, options: options)
    guard case .completed(let outputData) = info.status else {
      Issue.record("expected completed, got \(info.status)")
      return
    }
    #expect(try JSONDecoder().decode(String.self, from: outputData) == "fallback live")

    _ = node
    _ = worker
  }
}
