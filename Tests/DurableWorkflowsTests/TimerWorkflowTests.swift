import DistributedCluster
import Foundation
import Testing

@testable import DurableWorkflows

/// End-to-end timer behavior over a real single-node cluster with an
/// in-memory journal.
@Suite(.timeLimit(.minutes(2)))
struct TimerWorkflowTests {

  @Test
  func sleepCompletesAndJournalsTimerEvents() async throws {
    let (system, node, worker) = try await makeWorkflowSystem(
      name: "timers-basic", port: 4600, store: InMemoryEventStore()
    )
    let options = WorkflowOptions(id: "wf-basic")

    let started = ContinuousClock.now
    let output = try await system.workflows.execute(
      type: TimerWorkflow.self,
      options: options,
      input: .init(delayMillis: 200)
    )
    #expect(output == "hello world")
    #expect(ContinuousClock.now - started >= .milliseconds(200))

    let info = try await system.workflows.getStatus(type: TimerWorkflow.self, options: options)
    guard case .completed = info.status else {
      Issue.record("expected completed, got \(info.status)")
      return
    }

    let scheduled = info.events.compactMap { event -> (Int, Duration, String?)? in
      guard case .timerScheduled(let sequence, let duration, _, let summary) = event else { return nil }
      return (sequence, duration, summary)
    }
    #expect(scheduled.count == 1)
    #expect(scheduled.first?.0 == 0)
    #expect(scheduled.first?.1 == .milliseconds(200))
    #expect(scheduled.first?.2 == "test timer")

    let fired = info.events.contains { event in
      if case .timerFired(let sequence) = event { return sequence == 0 }
      return false
    }
    #expect(fired)

    _ = node
    _ = worker
  }

  @Test
  func cancelDuringSleepEndsAsCancelledNotFailed() async throws {
    let (system, node, worker) = try await makeWorkflowSystem(
      name: "timers-cancel", port: 4601, store: InMemoryEventStore()
    )
    let options = WorkflowOptions(id: "wf-cancel")

    let execution = Task {
      try await system.workflows.execute(
        type: TimerWorkflow.self,
        options: options,
        input: .init(delayMillis: 3_600_000)
      )
    }

    // Wait until the workflow is actually parked in the timer.
    try await eventually {
      let info = try await system.workflows.getStatus(type: TimerWorkflow.self, options: options)
      return info.events.contains { if case .timerScheduled = $0 { return true } else { return false } }
    }

    try await system.workflows.cancel(type: TimerWorkflow.self, options: options)

    await #expect(throws: CancellationError.self) {
      try await execution.value
    }

    let info = try await system.workflows.getStatus(type: TimerWorkflow.self, options: options)
    #expect(info.status == .cancelled)
    // The cancellation must not be turned into a failure by `_run`'s catch.
    let hasFailure = info.events.contains { if case .executionFailed = $0 { return true } else { return false } }
    #expect(!hasFailure)
    // The sleep's `.timerCancelled` is emitted after `.executionCancelled`,
    // and `handleEvent` drops post-terminal events by design — a cancelled
    // workflow never replays, so the timer record would be cosmetic. A
    // *child-task* cancellation (workflow keeps running) does record it.

    _ = node
    _ = worker
  }

  /// Crash-and-restore: seed the journal exactly as a crashed node would
  /// have left it — execution started, a 10s timer scheduled with only
  /// 300ms left to its absolute deadline. Activation of the workflow actor
  /// restores the journal and auto-resumes the run; it must wait only the
  /// remaining ~300ms, not the full 10s, and must not journal a second
  /// `timerScheduled`.
  @Test
  func crashDuringSleepResumesWithRemainingTimeOnly() async throws {
    let store = InMemoryEventStore()
    let (system, node, worker) = try await makeWorkflowSystem(
      name: "timers-crash", port: 4603, store: store
    )
    let options = WorkflowOptions(id: "wf-crash")

    // The crashed node's journal: `persistenceID` is "\(workflowName)-\(id)",
    // and the @Workflow macro names `TimerWorkflow` "timer".
    let inputData = try JSONEncoder().encode(TimerWorkflow.Input(delayMillis: 10_000))
    let persistenceID = "timer-wf-crash"
    try await store.persistEvent(
      WorkflowEvent.executionStarted(inputData: inputData),
      id: persistenceID,
      sequenceNumber: 1
    )
    try await store.persistEvent(
      WorkflowEvent.timerScheduled(
        sequence: 0,
        duration: .seconds(10),
        deadline: Date(timeIntervalSinceNow: 0.3),
        summary: "test timer"
      ),
      id: persistenceID,
      sequenceNumber: 2
    )

    let started = ContinuousClock.now
    try await eventually(interval: .milliseconds(100)) {
      let info = try await system.workflows.getStatus(type: TimerWorkflow.self, options: options)
      if case .completed = info.status { return true }
      return false
    }
    // The full duration was 10s; finishing well under that proves the resume
    // waited only until the persisted deadline.
    #expect(ContinuousClock.now - started < .seconds(5))

    let info = try await system.workflows.getStatus(type: TimerWorkflow.self, options: options)
    guard case .completed(let outputData) = info.status else {
      Issue.record("expected completed, got \(info.status)")
      return
    }
    #expect(try JSONDecoder().decode(String.self, from: outputData) == "hello world")

    // The replayed run must not schedule a new timer.
    let scheduledCount = info.events.filter {
      if case .timerScheduled = $0 { return true } else { return false }
    }.count
    #expect(scheduledCount == 1)
    let fired = info.events.contains { event in
      if case .timerFired(let sequence) = event { return sequence == 0 }
      return false
    }
    #expect(fired)

    _ = node
    _ = worker
  }

  @Test
  func negativeDurationFailsTheRun() async throws {
    let (system, node, worker) = try await makeWorkflowSystem(
      name: "timers-negative", port: 4602, store: InMemoryEventStore()
    )
    let options = WorkflowOptions(id: "wf-negative")

    await #expect(throws: WorkflowRuntimeError.invalidTimerDuration) {
      try await system.workflows.execute(
        type: TimerWorkflow.self,
        options: options,
        input: .init(delayMillis: -5)
      )
    }

    let info = try await system.workflows.getStatus(type: TimerWorkflow.self, options: options)
    guard case .failed = info.status else {
      Issue.record("expected failed, got \(info.status)")
      return
    }

    _ = node
    _ = worker
  }
}
