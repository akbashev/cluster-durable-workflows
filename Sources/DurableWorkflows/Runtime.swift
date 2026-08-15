import Distributed
import DistributedCluster
import EventSourcing
import Foundation
import ServiceLifecycle
import Synchronization
import VirtualActors

public enum WorkflowRuntimeError: Error, Sendable, Equatable {
  case activityContainerNotRegistered(String)
  case unknownActivityFailure(String)
  case workflowAlreadyRunning
  case workflowInputMismatch
  case workflowCancelled
  case workflowNotRunning
  /// Replay found a different operation kind at this sequence than the
  /// workflow code requested (e.g. a timer where history recorded a
  /// timestamp) — the workflow code changed incompatibly, or concurrent
  /// operations were scheduled in a different order than recorded.
  case nondeterministicOperation(sequence: Int, expected: String, actual: String)
  /// Replay of `sleep(until: Date)` found a different deadline at this
  /// sequence than history recorded — the workflow code changed
  /// incompatibly over a live execution. Version the workflow instead.
  /// (The `Instant` overload is not validated — per-boot values are not
  /// reproducible.)
  case nondeterministicDeadline(sequence: Int, expected: Date, actual: Date)
  /// `WorkflowContext.timeout(for:body:)` elapsed before the body finished.
  case timeoutExceeded
}

public enum ApplicationError: Error, Codable, Sendable {
  case typed(message: String, type: String, isNonRetryable: Bool)
}

public struct ActivityFailurePayload: Codable, Sendable {
  public let message: String
  public let type: String
  public let isNonRetryable: Bool

  public init(message: String, type: String, isNonRetryable: Bool) {
    self.message = message
    self.type = type
    self.isNonRetryable = isNonRetryable
  }
}

public struct ActivityInvocation: Codable, Sendable {
  public let name: String
  public let inputData: Data
  public let workflowID: String

  public init(name: String, inputData: Data, workflowID: String) {
    self.name = name
    self.inputData = inputData
    self.workflowID = workflowID
  }
}

public enum ActivityInvocationResult: Codable, Sendable {
  case success(outputData: Data)
  case failure(ActivityFailurePayload)
}

public struct ActivityContext: Sendable {
  public let workflowID: String
  public let activityName: String
  public let system: ClusterSystem

  public init(workflowID: String, activityName: String, system: ClusterSystem) {
    self.workflowID = workflowID
    self.activityName = activityName
    self.system = system
  }
}

public distributed actor DurableActivityDispatchWorker<WorkflowType: WorkflowProtocol>: DistributedWorker {
  public typealias ActorSystem = ClusterSystem
  public typealias WorkItem = ActivityInvocation
  public typealias WorkResult = ActivityInvocationResult

  private let container: WorkflowType.Activities
  private var recoverTask: Task<Void, Never>?

  public init(actorSystem: ClusterSystem) async {
    self.actorSystem = actorSystem
    self.container = WorkflowType.Activities()
    await self.actorSystem.receptionist.checkIn(self, with: .durableWorkers(for: WorkflowType.self))
    self.checkRecover()
  }

  distributed public func submit(work: ActivityInvocation) async throws -> ActivityInvocationResult {
    do {
      let output = try await container.handle(invocation: work, on: self.actorSystem)
      return .success(outputData: output)
    } catch let applicationError as ApplicationError {
      switch applicationError {
      case .typed(let message, let type, let isNonRetryable):
        return .failure(.init(message: message, type: type, isNonRetryable: isNonRetryable))
      }
    } catch {
      return .failure(.init(message: error.localizedDescription, type: "ActivityError", isNonRetryable: false))
    }
  }

  private func checkRecover() {
    self.recoverTask = Task {
      defer {
        self.recoverTask = nil
      }

      for await event in self.actorSystem.cluster.events {
        if case .membershipChange(let change) = event {
          guard change.node == self.actorSystem.cluster.node else {
            continue
          }
          guard change.isUp else {
            continue
          }
          try? await self.actorSystem.workflows.recoverAll(ofType: WorkflowType.self)
          return
        }
      }
    }
  }
}

extension DistributedReception.Key {
  public static func durableWorkers<W: WorkflowProtocol>(for type: W.Type) -> DistributedReception.Key<DurableActivityDispatchWorker<W>> {
    .init(id: "durable-workers-\(String(describing: W.Activities.self))")
  }
}

public enum ActivityOutcomeRecord: Codable, Sendable {
  case success(outputData: Data)
  case failure(ActivityFailurePayload)
}

public struct WorkflowContext: Sendable {
  private let cachedOutcomes: [String: ActivityOutcomeRecord]
  private let cachedTimers: [Int: TimerState]
  private let cachedTimestamps: [Int: ContinuousClock.Instant]
  private let workflowID: String
  public let system: ClusterSystem
  private let dispatch: @Sendable (String, ActivityInvocation, ActivityOptions) async throws -> Data
  private let recordEvent: @Sendable (WorkflowEvent) async throws -> Void
  /// Per-run sequence allocation for ALL journaled operations (`sleep`,
  /// `now`), owned by the runtime (`WorkflowActor`) — the same place the
  /// journaled events live. One ordinal space across operation kinds makes
  /// cross-kind reordering detectable on replay. The context itself holds
  /// no mutable bookkeeping.
  private let allocateSequence: @Sendable () async -> Int
  private let decoder: JSONDecoder
  private let encoder: JSONEncoder

  init(
    cachedOutcomes: [String: ActivityOutcomeRecord],
    cachedTimers: [Int: TimerState],
    cachedTimestamps: [Int: ContinuousClock.Instant],
    workflowID: String,
    system: ClusterSystem,
    dispatch: @escaping @Sendable (String, ActivityInvocation, ActivityOptions) async throws -> Data,
    recordEvent: @escaping @Sendable (WorkflowEvent) async throws -> Void,
    allocateSequence: @escaping @Sendable () async -> Int
  ) {
    self.cachedOutcomes = cachedOutcomes
    self.cachedTimers = cachedTimers
    self.cachedTimestamps = cachedTimestamps
    self.workflowID = workflowID
    self.system = system
    self.dispatch = dispatch
    self.recordEvent = recordEvent
    self.allocateSequence = allocateSequence

    let decoder = JSONDecoder()
    decoder.userInfo[.actorSystemKey] = system
    self.decoder = decoder

    let encoder = JSONEncoder()
    encoder.userInfo[.actorSystemKey] = system
    self.encoder = encoder
  }

  public func getActor<ActorType: VirtualActor>(
    identifiedBy id: VirtualActorID,
    dependency: any Sendable & Codable
  ) async throws -> ActorType {
    try await self.system.virtualActors.getActor(identifiedBy: id, dependency: dependency)
  }

  @discardableResult
  public func executeActivity<ActivityType: ActivityReference>(
    _ activity: ActivityType.Type,
    options: ActivityOptions = .init(),
    input: ActivityType.Input
  ) async throws -> ActivityType.Output {
    try Task.checkCancellation()

    let inputData = try encoder.encode(input)
    let key = "\(ActivityType.name):\(inputData.base64EncodedString())"

    if let cached = cachedOutcomes[key] {
      switch cached {
      case .success(let outputData):
        return try decoder.decode(ActivityType.Output.self, from: outputData)
      case .failure(let failure):
        throw ApplicationError.typed(
          message: failure.message,
          type: failure.type,
          isNonRetryable: failure.isNonRetryable
        )
      }
    }

    let invocation = ActivityInvocation(
      name: ActivityType.name,
      inputData: inputData,
      workflowID: workflowID
    )

    let outputData = try await dispatch(key, invocation, options)
    return try decoder.decode(ActivityType.Output.self, from: outputData)
  }

  /// Durably sleeps until a monotonic-clock instant.
  ///
  /// The timer is journaled (`timerScheduled`, then `timerFired`) with an
  /// absolute wall-clock deadline: after a crash or passivation the resumed
  /// run replays instantly to this point and waits only the *remaining*
  /// time, regardless of reboots or which node resumes it. A timer already
  /// fired in history returns immediately; a cancelled one rethrows
  /// `CancellationError`.
  ///
  /// The wait itself runs entirely on `ContinuousClock`. `Date` appears ONLY
  /// at the storage boundary: the journaled wall-clock deadline (which stays
  /// meaningful across reboots and nodes) is translated once into the
  /// monotonic domain on the way in.
  ///
  /// - Parameters:
  ///   - instant: a `ContinuousClock.Instant`; a past instant still journals
  ///     a timer (normalized to a minimal positive duration) and returns
  ///     immediately.
  ///   - summary: diagnostic metadata recorded in history — NOT the timer's
  ///     identity. Timers are identified by a per-run sequence number shared
  ///     with every other journaled operation (`now`).
  ///
  /// - Warning: sequence numbers are allocated by the runtime per call, in
  ///   arrival order — so sequential sleeps replay deterministically, but
  ///   concurrent sleeps (task groups) may be numbered in a different order
  ///   across replays. A timer at a sequence recorded as a different
  ///   operation kind is caught and throws
  ///   ``WorkflowRuntimeError/nondeterministicOperation(sequence:expected:actual:)``;
  ///   concurrent timers can swap sequence numbers undetected.
  public func sleep(until instant: ContinuousClock.Instant, summary: String? = nil) async throws {
    try await self._sleep(until: instant, summary: summary, wallDeadline: nil)
  }

  /// Wall-clock convenience overload of `sleep(until:summary:)` for
  /// deadlines arriving from the outside world (a subscription renewal date,
  /// a campaign end). Converts to the monotonic domain at the boundary and
  /// delegates; the journal records the exact date.
  ///
  /// Unlike the `Instant` overload, this one IS value-validated on replay:
  /// a `Date` argument is reproducible (it should come from journaled state),
  /// so a re-run passing a different date throws
  /// ``WorkflowRuntimeError/nondeterministicDeadline(sequence:expected:actual:)``.
  public func sleep(until date: Date, summary: String? = nil) async throws {
    try await self._sleep(until: date.continuousClockInstant, summary: summary, wallDeadline: date)
  }

  /// Duration convenience overload of `sleep(until:summary:)` —
  /// the equivalent of Temporal's `Workflow.sleep(for:summary:)`. Resolves
  /// to an instant on the monotonic clock and delegates.
  public func sleep(for duration: Duration, summary: String? = nil) async throws {
    try await self.sleep(until: .now + duration, summary: summary)
  }

  private func _sleep(
    until instant: ContinuousClock.Instant,
    summary: String?,
    wallDeadline: Date?
  ) async throws {
    try Task.checkCancellation()
    let sequence = await self.allocateSequence()
    // One ordinal space across operation kinds — shared with `now`.
    if self.cachedTimestamps[sequence] != nil {
      throw WorkflowRuntimeError.nondeterministicOperation(
        sequence: sequence,
        expected: "timer",
        actual: "timestamp"
      )
    }

    switch self.cachedTimers[sequence] {
    case .fired:
      return
    case .cancelled:
      throw CancellationError()
    case .scheduled(_, let storedDeadline, _):
      // Validated exactly when the caller's value is reproducible across
      // replays (the Date overload). JSON round-trips Date at sub-microsecond
      // precision; allow slack for encoding differences rather than
      // demanding bit-equality.
      if let wallDeadline, abs(storedDeadline.timeIntervalSince(wallDeadline)) >= 0.001 {
        throw WorkflowRuntimeError.nondeterministicDeadline(
          sequence: sequence,
          expected: storedDeadline,
          actual: wallDeadline
        )
      }
      // Wait out the stored wall-clock deadline, translated once into the
      // monotonic domain.
      try await self.waitForTimer(sequence: sequence, fireAt: storedDeadline.continuousClockInstant)
    case nil:
      // Normalize to a minimal positive duration so a past instant still
      // journals a timer.
      let normalized = max(ContinuousClock.Instant.now.duration(to: instant), .milliseconds(1))
      try await self.recordEvent(
        .timerScheduled(
          sequence: sequence,
          duration: normalized,
          deadline: wallDeadline ?? Date(after: normalized),
          summary: summary
        )
      )
      try await self.waitForTimer(sequence: sequence, fireAt: instant)
    }
  }

  /// Shared wait tail of `sleep(until:)`: sleep on the
  /// monotonic clock, journal cancellation or firing.
  private func waitForTimer(sequence: Int, fireAt: ContinuousClock.Instant) async throws {
    do {
      // A past instant returns immediately — an overdue timer fires as soon
      // as the run resumes.
      try await Task.sleep(until: fireAt, clock: .continuous)
    } catch is CancellationError {
      // Persist the cancellation: replay must rethrow instead of
      // recreating the sleep.
      try? await self.recordEvent(.timerCancelled(sequence: sequence))
      throw CancellationError()
    }
    try await self.recordEvent(.timerFired(sequence: sequence))
  }

  /// Deterministic workflow time as a monotonic instant, for Clock/Duration
  /// arithmetic in workflow code.
  ///
  /// Each call is journaled (`timestampRecorded`) as a wall-clock `Date` —
  /// the only representation that survives reboots and nodes exactly. Live
  /// calls return a fresh `ContinuousClock.Instant`; on replay the journaled
  /// date is translated once, at journal-fold time, into the monotonic
  /// domain (`Date.continuousClockInstant`), so all replayed values in one
  /// run share a single anchor and their *differences* are exact.
  ///
  /// - Warning: a replayed value approximates the recorded moment up to
  ///   wall↔monotonic drift between runs (e.g. NTP steps). Never feed a
  ///   `now`-derived value into an activity input or anything else that
  ///   lands in a content-addressed journal key — those require byte-exact
  ///   replay. For calendar facts from the outside world (renewal dates,
  ///   deadlines), prefer ``sleep(until:summary:)``, which binds by the
  ///   exact journaled deadline.
  ///
  /// Every call costs a journal write — keep `now` out of hot loops.
  public var now: ContinuousClock.Instant {
    get async throws {
      try Task.checkCancellation()
      let sequence = await self.allocateSequence()
      // One ordinal space across operation kinds — see `sleep`.
      if self.cachedTimers[sequence] != nil {
        throw WorkflowRuntimeError.nondeterministicOperation(
          sequence: sequence,
          expected: "timestamp",
          actual: "timer"
        )
      }
      if let recorded = self.cachedTimestamps[sequence] { return recorded }
      let now = Date.now
      try await self.recordEvent(.timestampRecorded(sequence: sequence, date: now))
      return now.continuousClockInstant
    }
  }

  /// Runs `body` and throws ``WorkflowRuntimeError/timeoutExceeded`` if it
  /// does not finish within `duration` — built on ``sleep(until:summary:)``,
  /// like Temporal's `Workflow.timeout`.
  ///
  /// The losing side is cancelled: if the timer fires first, `body` is
  /// cancelled (and its already-journaled steps replay normally); if `body`
  /// finishes first, the timer is cancelled and a `timerCancelled` event is
  /// journaled so replay does not recreate it.
  ///
  /// - Warning: `body` runs concurrently with the timer task, so if `body`
  ///   itself calls ``sleep(until:summary:)`` the two allocate timer sequences
  ///   in nondeterministic order across replays — see the warning on
  ///   ``sleep(until:summary:)``.
  public func timeout<T: Sendable>(
    for duration: Duration,
    body: @escaping @Sendable () async throws -> T
  ) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
      group.addTask { try await body() }
      group.addTask {
        try await self.sleep(until: .now + duration)
        throw WorkflowRuntimeError.timeoutExceeded
      }
      defer { group.cancelAll() }
      guard let first = try await group.next() else {
        throw WorkflowRuntimeError.timeoutExceeded
      }
      return first
    }
  }
}

public enum WorkflowEvent: Codable, Sendable {
  case executionStarted(inputData: Data)
  case activitySucceeded(key: String, name: String, outputData: Data)
  case activityFailed(key: String, name: String, failure: ActivityFailurePayload)
  case timerScheduled(sequence: Int, duration: Duration, deadline: Date, summary: String?)
  case timerFired(sequence: Int)
  case timerCancelled(sequence: Int)
  case timestampRecorded(sequence: Int, date: Date)
  case executionCompleted(outputData: Data)
  case executionCancelled
  case executionFailed(message: String)
}

/// Reconstructed timer state, folded from the journal by `handleEvent`.
public enum TimerState: Codable, Sendable, Equatable {
  case scheduled(duration: Duration, deadline: Date, summary: String?)
  case fired
  case cancelled
}

public enum WorkflowStatus: Codable, Sendable, Equatable {
  case idle
  case running
  case completed(data: Data)
  case cancelled
  case failed(error: String)

  public var name: String {
    switch self {
    case .idle: "idle"
    case .running: "running"
    case .completed: "completed"
    case .cancelled: "cancelled"
    case .failed: "failed"
    }
  }
}

public struct WorkflowStatusInfo: Codable, Sendable {
  public let status: WorkflowStatus
  public let events: [WorkflowEvent]
}

@EventSourced
@VirtualActor
public distributed actor WorkflowActor<WorkflowType: WorkflowProtocol> {
  public typealias ActorSystem = ClusterSystem
  public typealias Event = WorkflowEvent

  public struct Dependency: Codable, Sendable {
    public let workflowID: String

    public init(workflowID: String) {
      self.workflowID = workflowID
    }
  }

  public struct State: Codable, Sendable {
    var status: WorkflowStatus = .idle
    var inputData: Data?
    var activityOutcomes: [String: ActivityOutcomeRecord] = [:]
    var timers: [Int: TimerState] = [:]
    var timestamps: [Int: ContinuousClock.Instant] = [:]
    var events: [WorkflowEvent] = []
    var error: String?
  }

  public var state = State()
  private let encoder: JSONEncoder
  private let decoder: JSONDecoder

  private let persistenceID: String
  private let workflowID: String
  private let activityPool: WorkerPool<DurableActivityDispatchWorker<WorkflowType>>
  private var currentExecutionTask: Task<WorkflowResult<WorkflowType.Output>, Error>?

  /// Per-run operation cursor, reset at the start of every `_run`: each run —
  /// fresh or replayed — allocates 0, 1, 2… in call order across ALL journaled
  /// operations (`sleep`, `now`), so a replayed call binds to the same
  /// journaled entry it created. One ordinal space for every operation kind
  /// means cross-kind reordering is detectable (a `sleep` landing on a
  /// recorded timestamp position fails loudly instead of duplicating).
  /// Actor isolation makes allocation atomic; concurrent calls are numbered
  /// in message-arrival order, which is nondeterministic across replays
  /// (see `WorkflowContext.sleep`).
  private var operationSequenceCursor = 0

  private func nextOperationSequence() -> Int {
    defer { self.operationSequenceCursor += 1 }
    return self.operationSequenceCursor
  }

  public init(actorSystem: ClusterSystem, dependency: Dependency) async throws {
    self.actorSystem = actorSystem
    self.persistenceID = "\(WorkflowType.name)-\(dependency.workflowID)"
    self.workflowID = dependency.workflowID

    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    encoder.userInfo[.actorSystemKey] = actorSystem
    self.encoder = encoder

    let decoder = JSONDecoder()
    decoder.userInfo[.actorSystemKey] = actorSystem
    self.decoder = decoder

    let poolSettings = WorkerPoolSettings<DurableActivityDispatchWorker<WorkflowType>>(
      selector: .dynamic(.durableWorkers(for: WorkflowType.self)),
      strategy: .simpleRoundRobin
    )
    self.activityPool = try await WorkerPool(settings: poolSettings, actorSystem: actorSystem)

    try await actorSystem.journal.register(actor: self, with: self.persistenceID)

    if case .running = self.state.status {
      Task { try? await self.resume() }
    }
  }

  distributed public func getStatus() async throws -> WorkflowStatusInfo {
    WorkflowStatusInfo(
      status: self.state.status,
      events: self.state.events
    )
  }

  distributed public func cancel() async throws {
    guard case .running = self.state.status else { return }
    try await self.emit(event: .executionCancelled)
    self.currentExecutionTask?.cancel()
    self.currentExecutionTask = nil
  }

  /// A running workflow never agrees to deactivate: its execution is an
  /// in-memory task (e.g. parked in a timer sleep) that passivation would
  /// orphan — nothing would wake the actor at the timer's deadline.
  public func shouldDeactivate() async -> Bool {
    if case .running = self.state.status { return false }
    return true
  }

  @discardableResult
  distributed public func resume() async throws -> WorkflowResult<WorkflowType.Output> {
    switch self.state.status {
    case .running:
      if let task = self.currentExecutionTask { return try await task.value }
      guard let inputData = self.state.inputData else {
        throw WorkflowRuntimeError.workflowInputMismatch
      }
      let input = try self.decoder.decode(WorkflowType.Input.self, from: inputData)
      return try await self._run(input: input)
    case .completed(let outputData):
      let output = try self.decoder.decode(WorkflowType.Output.self, from: outputData)
      return WorkflowResult(output: output)
    case .cancelled:
      throw WorkflowRuntimeError.workflowCancelled
    case .idle, .failed:
      throw WorkflowRuntimeError.workflowNotRunning
    }
  }

  @discardableResult
  distributed public func execute(input: WorkflowType.Input) async throws -> WorkflowResult<WorkflowType.Output> {
    let inputData = try self.encoder.encode(input)
    if let previousInputData = self.state.inputData, previousInputData != inputData {
      throw WorkflowRuntimeError.workflowInputMismatch
    }

    switch self.state.status {
    case .completed(let outputData):
      if let output = try? self.decoder.decode(WorkflowType.Output.self, from: outputData) {
        return WorkflowResult(output: output)
      }
    case .running:
      if let task = self.currentExecutionTask { return try await task.value }
      return try await self._run(input: input)
    case .cancelled:
      throw WorkflowRuntimeError.workflowCancelled
    case .idle, .failed:
      break
    }

    try await self.emit(event: .executionStarted(inputData: inputData))
    return try await self._run(input: input)
  }

  private func _run(input: WorkflowType.Input) async throws -> WorkflowResult<WorkflowType.Output> {
    // A run — fresh or replayed — numbers its journaled operations from 0
    // in call order.
    self.operationSequenceCursor = 0
    let workflowTask = Task {
      let workflow = WorkflowType()
      let context = WorkflowContext(
        cachedOutcomes: self.state.activityOutcomes,
        cachedTimers: self.state.timers,
        cachedTimestamps: self.state.timestamps,
        workflowID: self.workflowID,
        system: self.actorSystem,
        dispatch: { key, invocation, _ in
          let result = try await self.activityPool.submit(work: invocation)
          switch result {
          case .success(let outputData):
            try await self.emit(
              event: .activitySucceeded(
                key: key,
                name: invocation.name,
                outputData: outputData
              )
            )
            return outputData
          case .failure(let failure):
            try await self.emit(
              event: .activityFailed(
                key: key,
                name: invocation.name,
                failure: failure
              )
            )
            throw ApplicationError.typed(
              message: failure.message,
              type: failure.type,
              isNonRetryable: failure.isNonRetryable
            )
          }
        },
        recordEvent: { event in
          try await self.emit(event: event)
        },
        allocateSequence: { await self.nextOperationSequence() }
      )

      do {
        let output = try await workflow.run(input: input, context: context)
        let outputData = try self.encoder.encode(output)
        try await self.emit(event: .executionCompleted(outputData: outputData))
        return WorkflowResult(output: output)
      } catch is CancellationError {
        // `cancel()` already emitted `.executionCancelled` — a cancelled
        // workflow must not be turned into a failed one here.
        throw CancellationError()
      } catch let appError as ApplicationError {
        if case .typed(let message, _, _) = appError {
          try await self.emit(event: .executionFailed(message: message))
        }
        throw appError
      } catch {
        try await self.emit(event: .executionFailed(message: error.localizedDescription))
        throw error
      }
    }

    self.currentExecutionTask = workflowTask
    defer {
      self.currentExecutionTask = nil
      // A terminal workflow is pure journal from here on — evict the actor
      // rather than holding it resident. Detached so the in-flight
      // distributed call's reply is delivered before the actor disappears;
      // reactivation on the next call just replays the journal.
      switch self.state.status {
      case .completed, .cancelled:
        Task { try? await self.actorSystem.virtualActors.deactivate(self) }
      case .idle, .running, .failed:
        break
      }
    }

    return try await withTaskCancellationHandler {
      try await workflowTask.value
    } onCancel: {
      workflowTask.cancel()
    }
  }

  distributed public func history() -> [WorkflowEvent] {
    self.state.events
  }

  distributed public func handleEvent(_ event: WorkflowEvent) {
    // Terminal states are absorbing: a late event (a timer cancellation
    // racing completion, a failure surfacing after cancel) must not move the
    // workflow out of them. `.failed` is deliberately NOT terminal —
    // `execute` may start a new run from it.
    switch self.state.status {
    case .completed, .cancelled:
      return
    case .idle, .running, .failed:
      break
    }

    self.state.events.append(event)

    switch event {
    case .executionStarted(let inputData):
      self.state.status = .running
      self.state.inputData = inputData
      self.state.error = nil
    case .activitySucceeded(let key, _, let outputData):
      self.state.activityOutcomes[key] = .success(outputData: outputData)
    case .activityFailed(let key, _, let failure):
      self.state.activityOutcomes[key] = .failure(failure)
    case .timerScheduled(let sequence, let duration, let deadline, let summary):
      self.state.timers[sequence] = .scheduled(duration: duration, deadline: deadline, summary: summary)
    case .timerFired(let sequence):
      self.state.timers[sequence] = .fired
    case .timerCancelled(let sequence):
      self.state.timers[sequence] = .cancelled
    case .timestampRecorded(let sequence, let date):
      self.state.timestamps[sequence] = date.continuousClockInstant
    case .executionCompleted(let outputData):
      self.state.status = .completed(data: outputData)
      self.state.error = nil
    case .executionFailed(let message):
      self.state.status = .failed(error: message)
      self.state.error = message
    case .executionCancelled:
      self.state.status = .cancelled
    }
  }
}

extension Date {
  /// Converts this wall-clock date into an approximate monotonic instant.
  ///
  /// The conversion is anchored at the moment this property is accessed.
  var continuousClockInstant: ContinuousClock.Instant {
    ContinuousClock.Instant.now.advanced(
      by: .seconds(self.timeIntervalSinceNow)
    )
  }
}
