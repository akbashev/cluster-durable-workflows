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
  /// `WorkflowContext.sleep` was called with a negative duration.
  case invalidTimerDuration
  /// Replay found a different duration at this timer sequence than the
  /// workflow code requested — the workflow code changed incompatibly, or
  /// concurrent timers were scheduled in a different order than recorded.
  case nondeterministicTimer(sequence: Int, expected: Duration, actual: Duration)
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
  private let workflowID: String
  public let system: ClusterSystem
  private let dispatch: @Sendable (String, ActivityInvocation, ActivityOptions) async throws -> Data
  private let recordTimerEvent: @Sendable (WorkflowEvent) async throws -> Void
  /// Per-run timer sequence allocation, owned by the runtime (`WorkflowActor`)
  /// — the same place the journaled timer events live. The context itself
  /// holds no mutable bookkeeping.
  private let allocateTimerSequence: @Sendable () async -> Int
  private let decoder: JSONDecoder
  private let encoder: JSONEncoder

  init(
    cachedOutcomes: [String: ActivityOutcomeRecord],
    cachedTimers: [Int: TimerState],
    workflowID: String,
    system: ClusterSystem,
    dispatch: @escaping @Sendable (String, ActivityInvocation, ActivityOptions) async throws -> Data,
    recordTimerEvent: @escaping @Sendable (WorkflowEvent) async throws -> Void,
    allocateTimerSequence: @escaping @Sendable () async -> Int
  ) {
    self.cachedOutcomes = cachedOutcomes
    self.cachedTimers = cachedTimers
    self.workflowID = workflowID
    self.system = system
    self.dispatch = dispatch
    self.recordTimerEvent = recordTimerEvent
    self.allocateTimerSequence = allocateTimerSequence

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

  /// Durably sleeps for `duration` — the equivalent of Temporal's
  /// `Workflow.sleep(for:summary:)`.
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
  /// monotonic domain on the way in, and produced once on the way out.
  ///
  /// - Parameters:
  ///   - duration: must be non-negative; zero is normalized to a minimal
  ///     positive duration (a timer is still journaled).
  ///   - summary: diagnostic metadata recorded in history — NOT the timer's
  ///     identity. Timers are identified by a per-run sequence number.
  ///
  /// - Warning: sequence numbers are allocated by the runtime per call, in
  ///   arrival order — so sequential sleeps replay deterministically, but
  ///   concurrent sleeps (task groups) may be numbered in a different order
  ///   across replays. A mismatched duration at a recorded sequence is caught
  ///   and throws
  ///   ``WorkflowRuntimeError/nondeterministicTimer(sequence:expected:actual:)``;
  ///   concurrent timers with *identical* durations can swap undetected.
  public func sleep(for duration: Duration, summary: String? = nil) async throws {
    try Task.checkCancellation()
    guard duration >= .zero else { throw WorkflowRuntimeError.invalidTimerDuration }
    // Temporal behavior: a zero-duration timer still exists in history.
    let normalized = max(duration, .milliseconds(1))
    let sequence = await self.allocateTimerSequence()

    let fireAt: ContinuousClock.Instant
    switch self.cachedTimers[sequence] {
    case .fired:
      return
    case .cancelled:
      throw CancellationError()
    case .scheduled(let recordedDuration, let storedDeadline, _):
      guard recordedDuration == normalized else {
        throw WorkflowRuntimeError.nondeterministicTimer(
          sequence: sequence, expected: recordedDuration, actual: normalized
        )
      }
      // Storage boundary, read side: wall clock → monotonic, once. A
      // deadline in the past maps to a past instant — "fire immediately".
      fireAt = .now + .seconds(storedDeadline.timeIntervalSinceNow)
    case nil:
      fireAt = .now + normalized
      // Storage boundary, write side: monotonic → wall clock, once.
      try await self.recordTimerEvent(
        .timerScheduled(
          sequence: sequence,
          duration: normalized,
          deadline: Date(after: normalized),
          summary: summary
        )
      )
    }

    do {
      // A past instant returns immediately — an overdue timer fires as soon
      // as the run resumes.
      try await Task.sleep(until: fireAt, clock: .continuous)
    } catch is CancellationError {
      // Persist the cancellation: replay must rethrow instead of
      // recreating the sleep.
      try? await self.recordTimerEvent(.timerCancelled(sequence: sequence))
      throw CancellationError()
    }
    try await self.recordTimerEvent(.timerFired(sequence: sequence))
  }
}

public enum WorkflowEvent: Codable, Sendable {
  case executionStarted(inputData: Data)
  case activitySucceeded(key: String, name: String, outputData: Data)
  case activityFailed(key: String, name: String, failure: ActivityFailurePayload)
  case timerScheduled(sequence: Int, duration: Duration, deadline: Date, summary: String?)
  case timerFired(sequence: Int)
  case timerCancelled(sequence: Int)
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

  /// Per-run timer sequence cursor, reset at the start of every `_run`: each
  /// run — fresh or replayed — allocates 0, 1, 2… in call order, so a
  /// replayed `sleep` binds to the same journaled timer it created. Actor
  /// isolation makes allocation atomic; concurrent `sleep` calls are
  /// numbered in message-arrival order, which is nondeterministic across
  /// replays (see `WorkflowContext.sleep`).
  private var timerSequenceCursor = 0

  private func nextTimerSequence() -> Int {
    defer { self.timerSequenceCursor += 1 }
    return self.timerSequenceCursor
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
    // A run — fresh or replayed — numbers its timers from 0 in call order.
    self.timerSequenceCursor = 0
    let workflowTask = Task {
      let workflow = WorkflowType()
      let context = WorkflowContext(
        cachedOutcomes: self.state.activityOutcomes,
        cachedTimers: self.state.timers,
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
        recordTimerEvent: { event in
          try await self.emit(event: event)
        },
        allocateTimerSequence: { await self.nextTimerSequence() }
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
    defer { self.currentExecutionTask = nil }

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
