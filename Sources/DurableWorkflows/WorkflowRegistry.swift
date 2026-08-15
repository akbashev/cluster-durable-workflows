import Distributed
import DistributedCluster
import EventSourcing

public enum WorkflowRegistryEvent: Codable, Sendable {
  case workflowStarted(id: WorkflowID, typeName: String)
  case workflowFinished(id: WorkflowID)
}

@EventSourced
public distributed actor WorkflowRegistry: ClusterSingleton {
  public typealias ActorSystem = ClusterSystem
  public typealias Event = WorkflowRegistryEvent

  public struct State: Codable, Sendable {
    // Keyed by raw id for Codable-friendly storage (string-keyed maps encode
    // as objects); the public API speaks `WorkflowID`.
    var running: [String: String] = [:]  // id.rawValue → typeName
  }

  public var state = State()

  public init(actorSystem: ClusterSystem) async throws {
    self.actorSystem = actorSystem
    try await actorSystem.journal.register(actor: self, with: "workflow-registry")
  }

  distributed public func trackStarted(id: WorkflowID, workflowType: String) async throws {
    try await self.emit(event: .workflowStarted(id: id, typeName: workflowType))
  }

  distributed public func trackFinished(id: WorkflowID) async throws {
    guard self.state.running[id.rawValue] != nil else { return }
    try await self.emit(event: .workflowFinished(id: id))
  }

  distributed public func runningWorkflows() -> [WorkflowID: String] {
    self.state.running.reduce(into: [:]) { $0[WorkflowID(rawValue: $1.key)] = $1.value }
  }

  distributed public func handleEvent(_ event: WorkflowRegistryEvent) {
    switch event {
    case .workflowStarted(let id, let typeName):
      self.state.running[id.rawValue] = typeName
    case .workflowFinished(let id):
      self.state.running.removeValue(forKey: id.rawValue)
    }
  }
}
