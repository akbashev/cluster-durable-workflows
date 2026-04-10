import Distributed
import DistributedCluster
import Foundation
import VirtualActors

@attached(extension, names: named(name), conformances: WorkflowProtocol)
public macro Workflow() = #externalMacro(module: "DurableWorkflowsMacros", type: "WorkflowMacro")

@attached(peer, names: arbitrary)
public macro Activity() = #externalMacro(module: "DurableWorkflowsMacros", type: "ActivityMacro")

@attached(extension, conformances: ActivityContainerProtocol)
@attached(member, names: arbitrary)
public macro ActivityContainer() = #externalMacro(module: "DurableWorkflowsMacros", type: "ActivityContainerMacro")

public struct DurableVoid: Codable, Sendable {
  public init() {}
}

public protocol WorkflowProtocol: Sendable {
  associatedtype Input: Codable & Sendable
  associatedtype Output: Codable & Sendable
  associatedtype Activities: ActivityContainerProtocol

  init()
  func run(input: Input, context: WorkflowContext) async throws -> Output
  static var name: String { get }
}

public struct WorkflowOptions: Codable, Sendable {
  public let id: String

  public init(id: String) {
    self.id = id
  }
}

public struct ActivityOptions: Codable, Sendable {
  public let startToCloseTimeoutMillis: Int?

  public init(startToCloseTimeoutMillis: Int? = nil) {
    self.startToCloseTimeoutMillis = startToCloseTimeoutMillis
  }
}

public struct WorkflowResult<Output: Codable & Sendable>: Codable, Sendable {
  public let output: Output

  public init(output: Output) {
    self.output = output
  }
}

public protocol ActivityReference {
  associatedtype Input: Codable & Sendable
  associatedtype Output: Codable & Sendable
  static var name: String { get }
}

public protocol ActivityContainerProtocol: Sendable {
  init()
  func handle(invocation: ActivityInvocation, on system: ClusterSystem) async throws -> Data
}
