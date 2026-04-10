import Distributed
import DistributedCluster
import Foundation
import VirtualActors

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

  @discardableResult
  public func execute<WorkflowType: WorkflowProtocol>(
    type: WorkflowType.Type,
    options: WorkflowOptions,
    input: WorkflowType.Input
  ) async throws -> WorkflowType.Output {
    let actor = try await self.getActor(WorkflowType.self, options: options)
    try? await self.registry.trackStarted(id: options.id, workflowType: WorkflowType.name)
    defer { Task { try? await self.registry.trackFinished(id: options.id) } }
    let result = try await actor.execute(input: input)
    return result.output
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
    try? await self.registry.trackFinished(id: options.id)
  }

  public func recoverAll<WorkflowType: WorkflowProtocol>(ofType type: WorkflowType.Type) async throws {
    let running = try await self.registry.runningWorkflows()
    for (id, name) in running where name == WorkflowType.name {
      _ = try? await self.getActor(WorkflowType.self, options: WorkflowOptions(id: id))
    }
  }

  private func getActor<WorkflowType: WorkflowProtocol>(
    _ type: WorkflowType.Type,
    options: WorkflowOptions
  ) async throws -> WorkflowActor<WorkflowType> {
    let dependency = WorkflowActor<WorkflowType>.Dependency(
      workflowID: options.id
    )
    return try await self.actorSystem.virtualActors.getActor(
      identifiedBy: .init(rawValue: "\(WorkflowType.name)-\(options.id)"),
      dependency: dependency
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
