import Distributed
import DistributedCluster
import EventSourcing

public enum WorkflowRegistryEvent: Codable, Sendable {
  case workflowStarted(id: String, typeName: String)
  case workflowFinished(id: String)
}

@EventSourced
public distributed actor WorkflowRegistry: ClusterSingleton {
  public typealias ActorSystem = ClusterSystem
  public typealias Event = WorkflowRegistryEvent

  struct State: Sendable {
    var running: [String: String] = [:]  // id → typeName
  }

  private var state = State()

  public init(actorSystem: ClusterSystem) async throws {
    self.actorSystem = actorSystem
    try await actorSystem.journal.register(actor: self, with: "workflow-registry")
  }

  distributed public func trackStarted(id: String, workflowType: String) async throws {
    try await self.emit(event: .workflowStarted(id: id, typeName: workflowType))
  }

  distributed public func trackFinished(id: String) async throws {
    guard self.state.running[id] != nil else { return }
    try await self.emit(event: .workflowFinished(id: id))
  }

  distributed public func runningWorkflows() -> [String: String] {
    self.state.running
  }

  distributed public func handleEvent(_ event: WorkflowRegistryEvent) {
    switch event {
    case .workflowStarted(let id, let typeName):
      self.state.running[id] = typeName
    case .workflowFinished(let id):
      self.state.running.removeValue(forKey: id)
    }
  }
}
