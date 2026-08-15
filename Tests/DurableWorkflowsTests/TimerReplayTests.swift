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
    timers: [Int: TimerState],
    timestamps: [Int: ContinuousClock.Instant] = [:]
  ) async throws -> (WorkflowContext, EventRecorder) {
    let port = Self.nextPort.withLock {
      defer { $0 += 1 }
      return $0
    }
    let system = await ClusterSystem("timer-replay-\(port)") {
      $0.bindPort = port  // the context never actually uses the system
    }
    let recorder = EventRecorder()
    let cursor = SequenceCursor()
    let context = WorkflowContext(
      cachedOutcomes: [:],
      cachedTimers: timers,
      cachedTimestamps: timestamps,
      workflowID: "replay-test",
      system: system,
      dispatch: { _, _, _ in fatalError("no activities in timer tests") },
      recordEvent: { event in recorder.record(event) },
      allocateSequence: { cursor.next() }
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
    // Measured a hair after `.now + duration` — just under, never over.
    #expect(duration > .milliseconds(9) && duration <= .milliseconds(10))
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
  func negativeDurationIsNormalizedToMinimalTimer() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    try await context.sleep(for: .seconds(-1))
    guard case .timerScheduled(_, let duration, _, _) = recorder.events.first else {
      Issue.record("expected timerScheduled, got \(recorder.events)")
      return
    }
    #expect(duration == .milliseconds(1))
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

  @Test
  func nowJournalsWallClockAndReplaysCachedInstant() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    let before = ContinuousClock.now
    let live = try await context.now
    #expect(live >= before && live <= ContinuousClock.now)
    // The journal holds the wall-clock Date — the exact, durable form.
    guard recorder.events.count == 1,
      case .timestampRecorded(let sequence, let date) = recorder.events[0]
    else {
      Issue.record("expected exactly [.timestampRecorded], got \(recorder.events)")
      return
    }
    #expect(sequence == 0)
    #expect(abs(date.timeIntervalSinceNow) < 1)

    // Replay: the cached (derived-at-fold) instant comes back verbatim,
    // no new journal write.
    let cached = ContinuousClock.now - .seconds(5)
    let (replayed, replayRecorder) = try await makeContext(timers: [:], timestamps: [0: cached])
    let value = try await replayed.now
    #expect(value == cached)
    #expect(replayRecorder.events.isEmpty)
  }

  @Test
  func timeoutReturnsBodyResultAndCancelsTimer() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    let value = try await context.timeout(for: .seconds(60)) {
      try await Task.sleep(for: .milliseconds(10))
      return "done"
    }
    #expect(value == "done")
    var sawScheduled = false
    var sawCancelled = false
    for event in recorder.events {
      switch event {
      case .timerScheduled: sawScheduled = true
      case .timerCancelled: sawCancelled = true
      default: break
      }
    }
    #expect(sawScheduled)
    #expect(sawCancelled)
  }

  @Test
  func timeoutThrowsWhenBodyIsSlower() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    await #expect(throws: WorkflowRuntimeError.timeoutExceeded) {
      try await context.timeout(for: .milliseconds(20)) {
        try await Task.sleep(for: .seconds(60))
        return "late"
      }
    }
    #expect(
      recorder.events.contains { event in
        if case .timerFired = event { return true }
        return false
      }
    )
  }

  @Test
  func mixedOperationsShareOneSequenceSpace() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    try await context.sleep(for: .milliseconds(1))
    _ = try await context.now
    var timerSeqs: [Int] = []
    var timestampSeqs: [Int] = []
    for event in recorder.events {
      switch event {
      case .timerScheduled(let sequence, _, _, _): timerSeqs.append(sequence)
      case .timestampRecorded(let sequence, _): timestampSeqs.append(sequence)
      default: break
      }
    }
    #expect(timerSeqs == [0])
    #expect(timestampSeqs == [1])
  }

  @Test
  func sleepOnRecordedTimestampPositionThrows() async throws {
    let (context, recorder) = try await makeContext(timers: [:], timestamps: [0: .now])
    await #expect(
      throws: WorkflowRuntimeError.nondeterministicOperation(
        sequence: 0,
        expected: "timer",
        actual: "timestamp"
      )
    ) {
      try await context.sleep(for: .seconds(1))
    }
    #expect(recorder.events.isEmpty)
  }

  @Test
  func sleepUntilPastDeadlineJournalsMinimalTimerAndFiresImmediately() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    let deadline = Date(timeIntervalSinceNow: -1)
    let started = ContinuousClock.now
    try await context.sleep(until: deadline)
    #expect(ContinuousClock.now - started < .milliseconds(100))
    // A past deadline is normalized to a minimal positive duration; the
    // Date overload still journals the caller's exact date.
    guard recorder.events.count == 2,
      case .timerScheduled(_, let duration, let recordedDeadline, _) = recorder.events[0],
      case .timerFired(let firedSequence) = recorder.events[1]
    else {
      Issue.record("expected [.timerScheduled, .timerFired], got \(recorder.events)")
      return
    }
    #expect(duration == .milliseconds(1))
    #expect(recordedDeadline == deadline)
    #expect(firedSequence == 0)
  }

  @Test
  func sleepUntilFutureDateJournalsTheExactDate() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    let deadline = Date(timeIntervalSinceNow: 0.05)
    let started = ContinuousClock.now
    try await context.sleep(until: deadline)
    #expect(ContinuousClock.now - started >= .milliseconds(40))
    guard case .timerScheduled(_, _, let recordedDeadline, _) = recorder.events.first else {
      Issue.record("expected timerScheduled, got \(recorder.events)")
      return
    }
    // The Date overload journals the caller's exact date, not a
    // double-converted approximation.
    #expect(recordedDeadline == deadline)
  }

  @Test
  func sleepUntilInstantWaitsMonotonically() async throws {
    let (context, recorder) = try await makeContext(timers: [:])
    let started = ContinuousClock.now
    try await context.sleep(until: .now + .milliseconds(30))
    #expect(ContinuousClock.now - started >= .milliseconds(25))
    #expect(recorder.events.count == 2)
  }

  @Test
  func sleepUntilReplayWaitsTheStoredDeadline() async throws {
    // Replay of the same call: the code passes the identical (journaled)
    // date, validation passes, and the stored deadline — not a freshly
    // computed one — governs the wait. Here it is already past, so the
    // timer fires immediately.
    let stored = Date(timeIntervalSinceNow: -1)
    let (context, recorder) = try await makeContext(timers: [
      0: .scheduled(duration: .seconds(3600), deadline: stored, summary: "renewal")
    ])
    try await context.sleep(until: stored)
    guard recorder.events.count == 1, case .timerFired = recorder.events[0] else {
      Issue.record("expected exactly [.timerFired], got \(recorder.events)")
      return
    }
  }

  @Test
  func sleepUntilMismatchedDateThrows() async throws {
    // The Date overload is value-validated on replay: a re-run passing a
    // different date than history recorded fails loudly.
    let stored = Date(timeIntervalSinceNow: 60)
    let requested = Date(timeIntervalSinceNow: 3600)
    let (context, recorder) = try await makeContext(timers: [
      0: .scheduled(duration: .seconds(60), deadline: stored, summary: nil)
    ])
    await #expect(
      throws: WorkflowRuntimeError.nondeterministicDeadline(
        sequence: 0,
        expected: stored,
        actual: requested
      )
    ) {
      try await context.sleep(until: requested)
    }
    #expect(recorder.events.isEmpty)
  }

  @Test
  func sleepUntilInstantReplayIsNotValueValidated() async throws {
    // Instants are per-boot and not reproducible, so the Instant overload
    // cannot compare values on replay: whatever the code passes, the stored
    // deadline governs. (Per-boot drift, not redeploys, is the expected
    // source of difference here.)
    let stored = Date(timeIntervalSinceNow: -1)
    let (context, recorder) = try await makeContext(timers: [
      0: .scheduled(duration: .seconds(3600), deadline: stored, summary: nil)
    ])
    try await context.sleep(until: .now + .seconds(3600))
    guard recorder.events.count == 1, case .timerFired = recorder.events[0] else {
      Issue.record("expected exactly [.timerFired], got \(recorder.events)")
      return
    }
  }
}
