import DistributedCluster
import DurableWorkflows
import EventSourcing
import Foundation
import Synchronization
import VirtualActors

/// Journal store for tests: keeps events in memory, idempotent per
/// `(id, sequenceNumber)` as `EventStore` requires.
final class InMemoryEventStore: EventStore, Sendable {
  struct Conflict: Error {}

  private let storage = Mutex<[String: [(sequenceNumber: Int64, data: Data)]]>([:])

  func persistEvent<Event: Codable & Sendable>(
    _ event: Event,
    id: String,
    sequenceNumber: Int64
  ) async throws {
    let data = try JSONEncoder().encode(event)
    try self.storage.withLock { store in
      var log = store[id] ?? []
      if let existing = log.first(where: { $0.sequenceNumber == sequenceNumber }) {
        guard existing.data == data else { throw Conflict() }
        return
      }
      log.append((sequenceNumber, data))
      log.sort { $0.sequenceNumber < $1.sequenceNumber }
      store[id] = log
    }
  }

  func eventsFor<Event: Codable & Sendable>(
    id: String,
    fromSequenceNumber: Int64
  ) async throws -> [Event] {
    let log = self.storage.withLock { $0[id] ?? [] }
    return
      try log
      .filter { $0.sequenceNumber >= fromSequenceNumber }
      .map { try JSONDecoder().decode(Event.self, from: $0.data) }
  }
}

/// A workflow with one activity around a timer — the realistic replay shape.
@Workflow
public struct TimerWorkflow {
  public typealias Activities = TimerActivities

  public struct Input: Codable, Sendable {
    public let delayMillis: Int

    public init(delayMillis: Int) {
      self.delayMillis = delayMillis
    }
  }

  public init() {}

  public func run(input: Input, context: WorkflowContext) async throws -> String {
    let greeting = try await context.executeActivity(
      TimerActivities.Activities.Greet.self,
      input: "world"
    )
    try await context.sleep(for: .milliseconds(input.delayMillis), summary: "test timer")
    return greeting
  }
}

@ActivityContainer
public struct TimerActivities {
  @Activity
  public func greet(input: String, context: ActivityContext) async throws -> String {
    "hello \(input)"
  }
}

/// Boots a single-node cluster with the full plugin stack. The returned node
/// and worker must stay bound for the system's lifetime — nothing else
/// retains them.
func makeWorkflowSystem(
  name: String,
  port: Int,
  store: InMemoryEventStore
) async throws -> (ClusterSystem, VirtualNode, DurableActivityDispatchWorker<TimerWorkflow>) {
  let system = await ClusterSystem(name) {
    $0.bindPort = port
    // For singleton plugin to work we need to choose a leader by having 1 member
    $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
    $0.plugins.install(plugin: ClusterSingletonPlugin())
    $0.plugins.install(plugin: ClusterVirtualActorsPlugin(replicationFactor: 10))
    $0.plugins.install(plugin: ClusterJournalPlugin { _ in store })
    $0.plugins.install(plugin: DurableWorkflowsPlugin())
  }
  let node = await VirtualNode(actorSystem: system)
  let worker = await DurableActivityDispatchWorker<TimerWorkflow>(actorSystem: system)
  system.cluster.join(endpoint: system.cluster.endpoint)
  try await system.cluster.joined(within: .seconds(5))
  return (system, node, worker)
}

/// Polls until `condition` holds or the attempts run out.
func eventually(
  attempts: Int = 100,
  interval: Duration = .milliseconds(50),
  _ condition: () async throws -> Bool
) async throws {
  for _ in 0..<attempts {
    if try await condition() { return }
    try await Task.sleep(for: interval)
  }
  struct Timeout: Error {}
  throw Timeout()
}
