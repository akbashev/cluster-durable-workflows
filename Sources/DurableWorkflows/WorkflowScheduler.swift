import Distributed
import DistributedCluster
import EventSourcing
import Foundation
import VirtualActors

public struct SchedulePersistenceID: Hashable, Codable, Sendable, CustomStringConvertible {
  public let rawValue: String

  public init(workflowType: String, scheduleID: ScheduleID) {
    self.rawValue = "\(workflowType)-schedule-\(scheduleID)"
  }

  public var description: String { rawValue }
}

public struct ScheduleID: Hashable, Codable, Sendable, CustomStringConvertible, ExpressibleByStringLiteral {
  public let rawValue: String
  public init(rawValue: String) { self.rawValue = rawValue }
  public init(stringLiteral value: String) { self.rawValue = value }
  public var description: String { rawValue }
}

public struct ScheduledWorkflowRecord: Codable, Sendable {
  public let id: ScheduleID
  public let recurrence: Calendar.RecurrenceRule
  public let startedAt: Date
  public let workflowType: String
  public let inputData: Data
  public let retryPolicy: RetryPolicy?
  public var lastFiredAt: Date?
  public var overlapPolicy: ScheduleOverlapPolicy

  public init(
    id: ScheduleID,
    recurrence: Calendar.RecurrenceRule,
    startedAt: Date,
    workflowType: String,
    inputData: Data,
    retryPolicy: RetryPolicy?,
    lastFiredAt: Date?,
    overlapPolicy: ScheduleOverlapPolicy = .allow
  ) {
    self.id = id
    self.recurrence = recurrence
    self.startedAt = startedAt
    self.workflowType = workflowType
    self.inputData = inputData
    self.retryPolicy = retryPolicy
    self.lastFiredAt = lastFiredAt
    self.overlapPolicy = overlapPolicy
  }
}

public enum ScheduleOverlapPolicy: Codable, Sendable {
  case allow
  case skip
}

public struct ScheduleOccurrenceID: Hashable, Codable, Sendable, CustomStringConvertible {
  public let scheduleID: ScheduleID
  public let scheduledAt: Date
  public let triggerID: UUID?

  public init(
    scheduleID: ScheduleID,
    scheduledAt: Date,
    triggerID: UUID? = nil
  ) {
    self.scheduleID = scheduleID
    self.scheduledAt = scheduledAt
    self.triggerID = triggerID
  }

  public var description: String {
    if let triggerID {
      return "\(scheduleID)-trigger-\(triggerID.uuidString)"
    }
    let milliseconds = Int64((scheduledAt.timeIntervalSince1970 * 1_000).rounded())
    return "\(scheduleID)-\(milliseconds)"
  }
}

public struct ScheduledWorkflowOccurrence: Codable, Sendable {
  public let id: ScheduleOccurrenceID
  public let scheduleID: ScheduleID
  public let workflowType: String
  public let inputData: Data
  public let retryPolicy: RetryPolicy?
  public let scheduledAt: Date

  public var workflowID: WorkflowID {
    WorkflowID(rawValue: "schedule-\(id.description)")
  }
}

public struct ScheduleDescription: Codable, Sendable {
  public let id: ScheduleID
  public let recurrence: Calendar.RecurrenceRule
  public let workflowType: String
  public let inputData: Data
  public let createdAt: Date
  public let lastFiredAt: Date?
  public let nextFireAt: Date?
  public let nextFireIn: Duration?
  public let paused: Bool
  public let executions: [ScheduleExecutionState]
}

public enum ScheduleTriggerOutcome: Codable, Sendable, Equatable {
  case triggered
  case skipped
}

public enum ScheduleRuntimeError: Error, Codable, Sendable, Equatable {
  case scheduleNotFound(ScheduleID)
}

public enum ScheduleExecutionState: Codable, Sendable, Equatable {
  case idle
  case pending
  case running
}

extension ScheduleExecutionState {
  init(_ execution: ScheduleExecution) {
    switch execution {
    case .idle: self = .idle
    case .pending: self = .pending
    case .running: self = .running
    }
  }
}

public enum ScheduleExecution: Codable, Sendable {
  case idle
  case pending(ScheduledWorkflowOccurrence)
  case running(ScheduledWorkflowOccurrence)
}

public enum WorkflowSchedulerEvent: Codable, Sendable {
  case scheduled(ScheduledWorkflowRecord)
  case cancelled
  case paused(reason: String?)
  case unpaused(reason: String?)
  case occurrenceClaimed(ScheduledWorkflowOccurrence)
  case occurrenceSkipped(ScheduledWorkflowOccurrence)
  case occurrenceTriggered(ScheduledWorkflowOccurrence)
  case occurrenceStarted(ScheduledWorkflowOccurrence)
  case occurrenceFinished(id: ScheduleOccurrenceID)
}

/// Durable schedule state. One virtual actor owns one schedule, so schedules
/// are independently placed, passivated, and reactivated across the cluster.
@EventSourced
@VirtualActor
/// `Input` is `WorkflowType.Input`, as the actor's own generic parameter: a distributed method can't use an
/// associated type (swiftlang/swift#74769).
public distributed actor WorkflowScheduler<WorkflowType: WorkflowProtocol, Input>
where WorkflowType.Input == Input {
  public typealias ActorSystem = ClusterSystem
  public typealias Event = WorkflowSchedulerEvent

  public struct Dependency: Codable, Sendable {
    public let scheduleID: ScheduleID
    public init(scheduleID: ScheduleID) { self.scheduleID = scheduleID }
  }

  public struct State: Codable, Sendable {
    public var schedule: ScheduledWorkflowRecord?
    public var paused: Bool = false
    public var executions: [ScheduleOccurrenceID: ScheduleExecution] = [:]
  }

  public var state = State()
  private let scheduleID: ScheduleID
  private let encoder: JSONEncoder
  private let decoder: JSONDecoder
  private var timerTask: Task<Void, Never>?

  public init(actorSystem: ClusterSystem, dependency: Dependency) async throws {
    self.actorSystem = actorSystem
    self.scheduleID = dependency.scheduleID
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    encoder.userInfo[.actorSystemKey] = actorSystem
    self.encoder = encoder
    let decoder = JSONDecoder()
    decoder.userInfo[.actorSystemKey] = actorSystem
    self.decoder = decoder
    try await actorSystem.journal.register(
      actor: self,
      with: SchedulePersistenceID(
        workflowType: WorkflowType.name,
        scheduleID: dependency.scheduleID
      ).rawValue
    )
    self.rearm()
    for execution in state.executions.values {
      switch execution {
      case .pending(let occurrence), .running(let occurrence):
        Task { await self.dispatch(occurrence) }
      case .idle:
        break
      }
    }
  }

  distributed public func schedule(
    recurrence: Calendar.RecurrenceRule,
    input: Input,
    retryPolicy: RetryPolicy?,
    overlapPolicy: ScheduleOverlapPolicy = .allow
  ) async throws {
    guard state.schedule == nil else {
      rearm()
      return
    }
    let inputData = try encoder.encode(input)
    try await emit(
      event: .scheduled(
        ScheduledWorkflowRecord(
          id: scheduleID,
          recurrence: recurrence,
          startedAt: .now,
          workflowType: WorkflowType.name,
          inputData: inputData,
          retryPolicy: retryPolicy,
          lastFiredAt: nil,
          overlapPolicy: overlapPolicy
        )
      )
    )
    rearm()
  }

  distributed public func cancel() async throws {
    guard state.schedule != nil else { return }
    try await emit(event: .cancelled)
    timerTask?.cancel()
    timerTask = nil
  }

  /// Stops automatic future occurrences while keeping the schedule durable.
  /// Existing occurrence executions are allowed to finish.
  distributed public func pause(reason: String? = nil) async throws {
    guard state.schedule != nil, !state.paused else { return }
    try await emit(event: .paused(reason: reason))
    timerTask?.cancel()
    timerTask = nil
  }

  /// Resumes automatic occurrences from the existing recurrence cursor.
  distributed public func unpause(reason: String? = nil) async throws {
    guard state.schedule != nil, state.paused else { return }
    try await emit(event: .unpaused(reason: reason))
    rearm()
  }

  /// Runs one occurrence immediately without advancing the recurring
  /// schedule. The next calendar occurrence remains the next calendar
  /// occurrence after this call.
  distributed public func trigger() async throws -> ScheduleTriggerOutcome {
    guard let schedule = state.schedule else {
      throw ScheduleRuntimeError.scheduleNotFound(scheduleID)
    }

    let occurrence = Self.occurrence(
      for: schedule,
      at: .now,
      triggerID: UUID()
    )
    guard schedule.overlapPolicy != .skip || state.executions.isEmpty else {
      return .skipped
    }

    // This is scheduler state, not workflow history. It records that the
    // manual occurrence was accepted so a crash after this point can recover
    // and dispatch it from the scheduler journal.
    try await emit(event: .occurrenceTriggered(occurrence))
    await dispatch(occurrence)
    return .triggered
  }

  distributed public func describe() -> ScheduleDescription? {
    guard let schedule = state.schedule else { return nil }
    return ScheduleDescription(
      id: schedule.id,
      recurrence: schedule.recurrence,
      workflowType: schedule.workflowType,
      inputData: schedule.inputData,
      createdAt: schedule.startedAt,
      lastFiredAt: schedule.lastFiredAt,
      nextFireAt: Self.nextFireDate(for: schedule),
      nextFireIn: Self.nextFireDate(for: schedule).map { .milliseconds(Int64(max(0, $0.timeIntervalSinceNow * 1_000))) },
      paused: state.paused,
      executions: state.executions.values.map(ScheduleExecutionState.init)
    )
  }

  public func shouldDeactivate() async -> Bool {
    state.schedule == nil && state.executions.isEmpty && timerTask == nil
  }

  private func rearm() {
    timerTask?.cancel()
    timerTask = nil
    guard !state.paused else { return }
    guard let schedule = state.schedule, let deadline = Self.nextFireDate(for: schedule) else { return }
    timerTask = Task {
      do {
        try await Task.sleep(until: deadline.continuousClockInstant, clock: .continuous)
        try Task.checkCancellation()
        await self.deadlineReached()
      } catch {}
    }
  }

  private func deadlineReached() async {
    timerTask = nil
    guard let schedule = state.schedule, let scheduledAt = Self.nextFireDate(for: schedule), scheduledAt <= .now else {
      rearm()
      return
    }
    let occurrence = Self.occurrence(for: schedule, at: scheduledAt)
    if schedule.overlapPolicy == .skip && !state.executions.isEmpty {
      try? await emit(event: .occurrenceSkipped(occurrence))
      rearm()
      return
    }
    do {
      try await emit(event: .occurrenceClaimed(occurrence))
      rearm()
      await dispatch(occurrence)
    } catch { rearm() }
  }

  private func dispatch(_ occurrence: ScheduledWorkflowOccurrence) async {
    guard state.executions[occurrence.id] != nil else { return }
    do {
      if case .pending = state.executions[occurrence.id] {
        try await emit(event: .occurrenceStarted(occurrence))
      }
      let input = try decoder.decode(WorkflowType.Input.self, from: occurrence.inputData)
      _ = try await actorSystem.workflows.execute(
        type: WorkflowType.self,
        options: WorkflowOptions(id: occurrence.workflowID, retryPolicy: occurrence.retryPolicy),
        input: input
      )
      try await emit(event: .occurrenceFinished(id: occurrence.id))
    } catch {
      try? await emit(event: .occurrenceFinished(id: occurrence.id))
    }
  }

  distributed public func handleEvent(_ event: WorkflowSchedulerEvent) {
    state = Self.reduce(state: state, event: event)
  }
}

extension WorkflowScheduler {
  /// The next occurrence: strictly after the last one, and the first strictly after the schedule
  /// was made, so a schedule is never due at once.
  static func nextFireDate(for schedule: ScheduledWorkflowRecord) -> Date? {
    let lowerBound = (schedule.lastFiredAt ?? schedule.startedAt).addingTimeInterval(0.001)
    return schedule.recurrence.recurrences(of: schedule.startedAt, in: lowerBound...).first { _ in true }
  }

  static func occurrence(
    for schedule: ScheduledWorkflowRecord,
    at date: Date,
    triggerID: UUID? = nil
  ) -> ScheduledWorkflowOccurrence {
    ScheduledWorkflowOccurrence(
      id: ScheduleOccurrenceID(
        scheduleID: schedule.id,
        scheduledAt: date,
        triggerID: triggerID
      ),
      scheduleID: schedule.id,
      workflowType: schedule.workflowType,
      inputData: schedule.inputData,
      retryPolicy: schedule.retryPolicy,
      scheduledAt: date
    )
  }

  static func reduce(state: State, event: WorkflowSchedulerEvent) -> State {
    var state = state
    switch event {
    case .scheduled(let schedule): state.schedule = schedule
    case .cancelled:
      state.schedule = nil
      state.paused = false
      state.executions = state.executions.filter {
        if case .running = $0.value { return true }
        return false
      }
    case .paused:
      state.paused = true
    case .unpaused:
      state.paused = false
    case .occurrenceClaimed(let occurrence):
      state.schedule?.lastFiredAt = occurrence.scheduledAt
      state.executions[occurrence.id] = .pending(occurrence)
    case .occurrenceSkipped(let occurrence):
      state.schedule?.lastFiredAt = occurrence.scheduledAt
    case .occurrenceTriggered(let occurrence):
      state.executions[occurrence.id] = .pending(occurrence)
    case .occurrenceStarted(let occurrence):
      state.executions[occurrence.id] = .running(occurrence)
    case .occurrenceFinished(let id):
      state.executions.removeValue(forKey: id)
    }
    return state
  }
}
