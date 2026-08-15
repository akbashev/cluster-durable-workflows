import DistributedCluster
import Foundation
import Synchronization
import Testing

@testable import DurableWorkflows

/// Replay logic of `WorkflowContext.sleep`, exercised directly against
/// cached timer states — no cluster, no journal.
@Suite(.timeLimit(.minutes(1)))
struct TimerReplayTests {

  final class EventRecorder: Sendable {
    private let recorded = Mutex<[WorkflowEvent]>([])

    func record(_ event: WorkflowEvent) {
      self.recorded.withLock { $0.append(event) }
    }

    var events: [WorkflowEvent] {
      self.recorded.withLock { $0 }
    }
  }

  /// Test double for the runtime's per-run timer sequence allocation (in
  /// production this lives on `WorkflowActor`, serialized by isolation).
  final class SequenceCursor: Sendable {
    private let value = Mutex(0)

    func next() -> Int {
      self.value.withLock {
        defer { $0 += 1 }
        return $0
      }
    }
  }

  /// Systems must bind a real (> 0) port; tests run in parallel, so each
  /// context gets its own.
  private static let nextPort = Mutex(4700)

  private func makeContext(
    timers: [Int: TimerState]
  ) async throws -> (WorkflowContext, EventRecorder) {
    let port = Self.nextPort.withLock { defer { $0 += 1 }; return $0 }
    let system = await ClusterSystem("timer-replay-\(port)") {
      $0.bindPort = port  // the context never actually uses the system
    }
    let recorder = EventRecorder()
    let cursor = SequenceCursor()
    let context = WorkflowContext(
      cachedOutcomes: [:],
      cachedTimers: timers,
      workflowID: "replay-test",
      system: system,
      dispatch: { _, _, _ in fatalError("no activities in timer tests") },
      recordTimerEvent: { event in recorder.record(event) },
      allocateTimerSequence: { cursor.next() }
    )
    return (context, recorder)
  }

  @Test
  func firedTimerReturnsImmediately() async throws {
    let (context, recorder) = try await makeContext(timers: [0: .fired])
    let started = ContinuousClock.now
    try await context.sleep(for: .seconds(5))
    #expect(ContinuousClock.now - started < .milliseconds(100))
    #expect(recorder.events.isEmpty)
  }

  @Test
  func cancelledTimerRethrowsCancellation() async throws {
    let (context, recorder) = try await makeContext(timers: [0: .cancelled])
    await #expect(throws: CancellationError.self) {
      try await context.sleep(for: .seconds(5))
    }
    #expect(recorder.events.isEmpty)
  }

  @Test
  func scheduledTimerWithPastDeadlineEmitsOnlyFired() async throws {
    let deadline = Date(timeIntervalSinceNow: -1)
    let (context, recorder) = try await makeContext(timers: [
      0: .scheduled(duration: .seconds(5), deadline: deadline, summary: nil)
    ])
    try await context.sleep(for: .seconds(5))
    guard recorder.events.count == 1, case .timerFired(let sequence) = recorder.events[0] else {
      Issue.record("expected exactly [.timerFired(0)], got \(recorder.events)")
      return
    }
    #expect(sequence == 0)
  }

  @Test
  func scheduledTimerWithMismatchedDurationThrows() async throws {
    let deadline = Date(timeIntervalSinceNow: 60)
    let (context, _) = try await makeContext(timers: [
      0: .scheduled(duration: .seconds(5), deadline: deadline, summary: nil)
    ])
    await #expect(
      throws: WorkflowRuntimeError.nondeterministicTimer(
        sequence: 0, expected: .seconds(5), actual: .seconds(6)
      )
    ) {
      try await context.sleep(for: .seconds(6))
    }
  }

  @Test
  func freshSleepJournalsScheduleThenFire() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    try await context.sleep(for: .milliseconds(10), summary: "nap")
    guard recorder.events.count == 2 else {
      Issue.record("expected 2 events, got \(recorder.events)")
      return
    }
    guard case .timerScheduled(let sequence, let duration, let deadline, let summary) = recorder.events[0] else {
      Issue.record("expected timerScheduled, got \(recorder.events[0])")
      return
    }
    #expect(sequence == 0)
    #expect(duration == .milliseconds(10))
    #expect(summary == "nap")
    #expect(deadline > Date(timeIntervalSinceNow: -1))
    guard case .timerFired(let firedSequence) = recorder.events[1] else {
      Issue.record("expected timerFired, got \(recorder.events[1])")
      return
    }
    #expect(firedSequence == 0)
  }

  @Test
  func zeroDurationIsNormalizedToMinimalTimer() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    try await context.sleep(for: .zero)
    guard case .timerScheduled(_, let duration, _, _) = recorder.events.first else {
      Issue.record("expected timerScheduled, got \(recorder.events)")
      return
    }
    #expect(duration == .milliseconds(1))
  }

  @Test
  func negativeDurationThrows() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    await #expect(throws: WorkflowRuntimeError.invalidTimerDuration) {
      try await context.sleep(for: .seconds(-1))
    }
    #expect(recorder.events.isEmpty)
  }

  @Test
  func sequentialSleepsAllocateIncreasingSequences() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    try await context.sleep(for: .milliseconds(1))
    try await context.sleep(for: .milliseconds(1))
    var scheduled: [Int] = []
    var fired: [Int] = []
    for event in recorder.events {
      switch event {
      case .timerScheduled(let sequence, _, _, _): scheduled.append(sequence)
      case .timerFired(let sequence): fired.append(sequence)
      default: break
      }
    }
    #expect(scheduled == [0, 1])
    #expect(fired == [0, 1])
  }
}
