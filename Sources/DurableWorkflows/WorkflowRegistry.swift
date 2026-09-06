import Distributed
import DistributedCluster
import EventSourcing

/// A durable object that must be rediscovered after a process restart.
public enum WorkflowRegistration: Codable, Sendable, Equatable {
  case workflow(id: WorkflowID, typeName: String)
  case schedule(id: ScheduleID, typeName: String)

  public var typeName: String {
    switch self {
    case .workflow(_, let typeName), .schedule(_, let typeName):
      return typeName
    }
  }
}

public enum WorkflowRegistryEvent: Codable, Sendable {
  case registered(WorkflowRegistration)
  case unregistered(WorkflowRegistration)
}

@EventSourced
public distributed actor WorkflowRegistry: ClusterSingleton {
  public typealias ActorSystem = ClusterSystem
  public typealias Event = WorkflowRegistryEvent

  public struct State: Codable, Sendable {
    var registrations: [WorkflowRegistration] = []

    public init() {}
  }

  public var state = State()

  public init(actorSystem: ClusterSystem) async throws {
    self.actorSystem = actorSystem
    try await actorSystem.journal.register(actor: self, with: "workflow-registry")
  }

  distributed public func register(
    _ registration: WorkflowRegistration
  ) async throws {
    guard !self.state.registrations.contains(registration) else { return }
    try await self.emit(event: .registered(registration))
  }

  distributed public func unregister(
    _ registration: WorkflowRegistration
  ) async throws {
    guard self.state.registrations.contains(registration) else { return }
    try await self.emit(event: .unregistered(registration))
  }

  distributed public func registrations() -> [WorkflowRegistration] {
    self.state.registrations
  }

  distributed public func handleEvent(_ event: WorkflowRegistryEvent) {
    switch event {
    case .registered(let registration):
      guard !self.state.registrations.contains(registration) else { return }
      self.state.registrations.append(registration)
    case .unregistered(let registration):
      self.state.registrations.removeAll { $0 == registration }
    }
  }
}
