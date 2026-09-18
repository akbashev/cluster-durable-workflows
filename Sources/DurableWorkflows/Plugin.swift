import Distributed
import DistributedCluster
import Foundation
import VirtualActors

public struct WorkflowDescription: Codable, Sendable {
  public let id: WorkflowID
  public let status: WorkflowStatus
  public let inputData: Data?

  public init(id: WorkflowID, status: WorkflowStatus, inputData: Data?) {
    self.id = id
    self.status = status
    self.inputData = inputData
  }
}

public actor DurableWorkflowsPlugin: Plugin {
  public static let pluginKey: Key = "$durableWorkflows"

  public nonisolated var key: Key {
    Self.pluginKey
  }

  public var actorSystem: ClusterSystem!
  private var registry: WorkflowRegistry!

  public init() {
    self.actorSystem = nil
  }

  public func start(_ system: ClusterSystem) async throws {
    self.actorSystem = system
    self.registry = try await self.actorSystem.singleton.host(name: "workflow-registry") { actorSystem in
      try await WorkflowRegistry(actorSystem: actorSystem)
    }
  }

  public func stop(_ system: ClusterSystem) async {
    self.actorSystem = nil
    self.registry = nil
  }

  func workflowDidFinish(id: WorkflowID, typeName: String) async {
    guard let registry = self.registry else { return }
    try? await registry.unregister(
      .workflow(
        id: id,
        typeName: typeName
      )
    )
  }

  public func schedule<WorkflowType: WorkflowProtocol>(
    id: ScheduleID,
    recurrence: Calendar.RecurrenceRule,
    workflow: WorkflowType.Type,
    input: WorkflowType.Input,
    retryPolicy: RetryPolicy? = nil,
    overlapPolicy: ScheduleOverlapPolicy = .allow
  ) async throws {
    let scheduler = try await self.scheduler(for: WorkflowType.self, id: id)
    try await self.registry.register(
      .schedule(
        id: id,
        typeName: WorkflowType.name
      )
    )
    try await scheduler.schedule(
      recurrence: recurrence,
      input: input,
      retryPolicy: retryPolicy,
      overlapPolicy: overlapPolicy
    )
  }

  public func cancelSchedule<WorkflowType: WorkflowProtocol>(id: ScheduleID, workflow: WorkflowType.Type) async throws {
    let scheduler = try await self.scheduler(for: WorkflowType.self, id: id)
    try await scheduler.cancel()
    try? await self.registry.unregister(
      .schedule(
        id: id,
        typeName: WorkflowType.name
      )
    )
  }

  /// Pauses future occurrences while keeping the durable schedule registered.
  public func pauseSchedule<WorkflowType: WorkflowProtocol>(
    id: ScheduleID,
    workflow: WorkflowType.Type,
    reason: String? = nil
  ) async throws {
    let scheduler = try await self.scheduler(for: WorkflowType.self, id: id)
    try await scheduler.pause(reason: reason)
  }

  /// Resumes future occurrences for a paused durable schedule.
  public func unpauseSchedule<WorkflowType: WorkflowProtocol>(
    id: ScheduleID,
    workflow: WorkflowType.Type,
    reason: String? = nil
  ) async throws {
    let scheduler = try await self.scheduler(for: WorkflowType.self, id: id)
    try await scheduler.unpause(reason: reason)
  }

  public func describeSchedule<WorkflowType: WorkflowProtocol>(
    id: ScheduleID,
    workflow: WorkflowType.Type
  ) async throws -> ScheduleDescription? {
    try await self.scheduler(for: WorkflowType.self, id: id).describe()
  }

  /// Triggers one occurrence now without moving the recurring schedule's
  /// calendar cursor. The schedule's next planned occurrence is unchanged.
  public func triggerSchedule<WorkflowType: WorkflowProtocol>(
    id: ScheduleID,
    workflow: WorkflowType.Type
  ) async throws -> ScheduleTriggerOutcome {
    try await self.scheduler(for: WorkflowType.self, id: id).trigger()
  }

  /// Lists the schedules registered for one workflow type. Stale registry
  /// entries are omitted; recovery remains responsible for cleaning them up.
  public func listSchedules<WorkflowType: WorkflowProtocol>(
    workflow: WorkflowType.Type
  ) async throws -> [ScheduleDescription] {
    let registrations = try await self.registry.registrations()
    let ids = registrations.compactMap { registration -> ScheduleID? in
      guard case .schedule(let id, let typeName) = registration,
        typeName == WorkflowType.name
      else {
        return nil
      }
      return id
    }

    return try await withThrowingTaskGroup(of: ScheduleDescription?.self) { group in
      for id in ids {
        group.addTask {
          try? await self.describeSchedule(id: id, workflow: WorkflowType.self)
        }
      }

      var descriptions = [ScheduleDescription]()
      for try await description in group {
        if let description {
          descriptions.append(description)
        }
      }
      return descriptions.sorted { $0.id.rawValue < $1.id.rawValue }
    }
  }

  /// Lists registered workflow executions for one workflow type. The input is
  /// intentionally opaque at this generic boundary; the owning domain decodes
  /// it into its own request type.
  public func listWorkflows<WorkflowType: WorkflowProtocol>(
    workflow: WorkflowType.Type
  ) async throws -> [WorkflowDescription] {
    let registrations = try await self.registry.registrations()
    let ids = registrations.compactMap { registration -> WorkflowID? in
      guard case .workflow(let id, let typeName) = registration,
        typeName == WorkflowType.name
      else {
        return nil
      }
      return id
    }

    return try await withThrowingTaskGroup(of: WorkflowDescription?.self) { group in
      for id in ids {
        group.addTask {
          guard
            let actor = try? await self.getActor(
              WorkflowType.self,
              options: WorkflowOptions(id: id)
            )
          else {
            return nil
          }
          guard let status = try? await actor.getStatus() else {
            return nil
          }
          let inputData = status.events.reversed().compactMap { event -> Data? in
            guard case .executionStarted(let inputData) = event else {
              return nil
            }
            return inputData
          }.first
          return WorkflowDescription(
            id: id,
            status: status.status,
            inputData: inputData
          )
        }
      }

      var descriptions = [WorkflowDescription]()
      for try await description in group {
        if let description {
          descriptions.append(description)
        }
      }
      return descriptions.sorted { $0.id.rawValue < $1.id.rawValue }
    }
  }

  private func scheduler<WorkflowType: WorkflowProtocol>(for type: WorkflowType.Type, id: ScheduleID) async throws -> WorkflowScheduler<WorkflowType> {
    try await self.actorSystem.virtualActors.getActor(
      identifiedBy: .init(
        rawValue: SchedulePersistenceID(
          workflowType: WorkflowType.name,
          scheduleID: id
        ).rawValue
      ),
      dependency: WorkflowScheduler<WorkflowType>.Dependency(
        scheduleID: id
      )
    )
  }

  @discardableResult
  public func execute<WorkflowType: WorkflowProtocol>(
    type: WorkflowType.Type,
    options: WorkflowOptions,
    input: WorkflowType.Input
  ) async throws -> WorkflowType.Output {
    let actor = try await self.getActor(WorkflowType.self, options: options)
    let registration = WorkflowRegistration.workflow(
      id: options.id,
      typeName: WorkflowType.name
    )
    try? await self.registry.register(registration)
    defer { Task { try? await self.registry.unregister(registration) } }
    let result = try await actor.execute(input: input, retryPolicy: options.retryPolicy)
    return result.output
  }

  /// Starts a durable workflow without waiting for its result. The workflow
  /// actor owns the execution task and its journaled lifecycle.
  public func start<WorkflowType: WorkflowProtocol>(
    type: WorkflowType.Type,
    options: WorkflowOptions,
    input: WorkflowType.Input
  ) async throws {
    let actor = try await self.getActor(WorkflowType.self, options: options)
    let registration = WorkflowRegistration.workflow(
      id: options.id,
      typeName: WorkflowType.name
    )
    try? await self.registry.register(registration)
    do {
      let outcome = try await actor.start(input: input, retryPolicy: options.retryPolicy)
      if case .alreadyCompleted = outcome {
        try? await self.registry.unregister(registration)
      }
    } catch {
      try? await self.registry.unregister(registration)
      throw error
    }
  }

  public func getStatus<WorkflowType: WorkflowProtocol>(
    type: WorkflowType.Type,
    options: WorkflowOptions
  ) async throws -> WorkflowStatusInfo {
    let actor = try await self.getActor(WorkflowType.self, options: options)
    return try await actor.getStatus()
  }

  public func cancel<WorkflowType: WorkflowProtocol>(
    type: WorkflowType.Type,
    options: WorkflowOptions
  ) async throws {
    let actor = try await self.getActor(WorkflowType.self, options: options)
    try await actor.cancel()
    try? await self.registry.unregister(
      .workflow(
        id: options.id,
        typeName: WorkflowType.name
      )
    )
  }

  public func recoverAll<WorkflowType: WorkflowProtocol>(ofType type: WorkflowType.Type) async throws {
    for registration in try await self.registry.registrations()
    where registration.typeName == WorkflowType.name {
      switch registration {
      case .workflow(let id, _):
        guard
          let actor = try? await self.getActor(
            WorkflowType.self,
            options: WorkflowOptions(id: id)
          )
        else { continue }
        guard let status = try? await actor.getStatus() else { continue }
        switch status.status {
        case .running:
          break
        case .idle, .completed, .cancelled, .failed:
          try? await self.registry.unregister(registration)
        }
      case .schedule(let id, _):
        guard
          let scheduler = try? await self.scheduler(
            for: WorkflowType.self,
            id: id
          )
        else { continue }
        do {
          guard try await scheduler.describe() != nil else {
            try? await self.registry.unregister(registration)
            continue
          }
        } catch {
          // Keep the registration on transient startup, receptionist, or
          // journal failures. Removing it here would turn a temporary
          // recovery failure into lost work; the next recovery pass can try
          // the scheduler again.
          continue
        }
      }
    }
  }

  private func getActor<WorkflowType: WorkflowProtocol>(
    _ type: WorkflowType.Type,
    options: WorkflowOptions
  ) async throws -> WorkflowActor<WorkflowType> {
    try await self.actorSystem.virtualActors.getActor(
      identifiedBy: .init(
        rawValue: WorkflowPersistenceID(workflowType: WorkflowType.name, id: options.id).rawValue
      ),
      dependency: WorkflowActor<WorkflowType>.Dependency(
        workflowID: options.id
      )
    )
  }
}

extension ClusterSystem {
  public var workflows: DurableWorkflowsPlugin {
    let key = DurableWorkflowsPlugin.pluginKey
    guard let workflowsPlugin = self.settings.plugins[key] else {
      fatalError("No plugin found for key: [\(key)], installed plugins: \(self.settings.plugins)")
    }
    return workflowsPlugin
  }
}
