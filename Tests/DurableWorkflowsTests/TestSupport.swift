import DistributedCluster
import DurableWorkflows
import EventSourcing
import Foundation
import Synchronization
import VirtualActors

/// Journal store for tests: keeps events in memory and rejects every repeated
/// `(id, sequenceNumber)` write as `EventStore` requires.
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
      if log.contains(where: { $0.sequenceNumber == sequenceNumber }) {
        throw Conflict()
      }
      log.append((sequenceNumber, data))
      log.sort { $0.sequenceNumber < $1.sequenceNumber }
      store[id] = log
    }
  }

  func eventStream<Event: Codable & Sendable>(
    id: String,
    fromSequenceNumber: Int64
  ) async throws -> EventStream<Event> {
    let log = self.storage.withLock { $0[id] ?? [] }
    let envelopes =
      try log
      .filter { $0.sequenceNumber >= fromSequenceNumber }
      .map {
        EventEnvelope(
          persistenceID: id,
          sequenceNumber: $0.sequenceNumber,
          event: try JSONDecoder().decode(Event.self, from: $0.data)
        )
      }
    return AsyncThrowingStream { continuation in
      for envelope in envelopes {
        continuation.yield(envelope)
      }
      continuation.finish()
    }
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

/// Counts down failures for `FlakyActivities.flaky` — fails N times, then
/// succeeds, to exercise automatic retry over a real cluster.
final class FlakySwitch: Sendable {
  static let shared = FlakySwitch()

  private let _failuresRemaining = Mutex(0)

  var failuresRemaining: Int {
    get { self._failuresRemaining.withLock { $0 } }
    set { self._failuresRemaining.withLock { $0 = newValue } }
  }

  /// Records the invocation and reports whether this call should fail.
  func failThisCall() -> Bool {
    self._failuresRemaining.withLock { remaining in
      guard remaining > 0 else { return false }
      remaining -= 1
      return true
    }
  }
}

/// One activity that fails until `FlakySwitch`'s failure budget is spent.
@Workflow
public struct FlakyWorkflow {
  public typealias Activities = FlakyActivities

  public struct Input: Codable, Sendable {
    /// Varies the encoded input so tests can submit a genuinely NEW input to
    /// a workflow that already has history (the demo's shape: a fresh
    /// Connection actor per submission).
    public var label: String

    public init(label: String = "") {
      self.label = label
    }
  }

  public init() {}

  public func run(input: Input, context: WorkflowContext) async throws -> String {
    try await context.executeActivity(
      FlakyActivities.Activities.Flaky.self,
      input: "attempt"
    )
  }
}

@ActivityContainer
public struct FlakyActivities {
  @Activity
  public func flaky(input: String, context: ActivityContext) async throws -> String {
    InvocationCounter.shared.increment("flaky")
    if FlakySwitch.shared.failThisCall() {
      throw ApplicationError.typed(
        message: "flaky activity failed",
        type: "FlakyError",
        isNonRetryable: false
      )
    }
    return "ok"
  }
}

/// Counts live activity dispatches — replay-safety tests assert nothing ran.
final class InvocationCounter: Sendable {
  static let shared = InvocationCounter()

  private let counts = Mutex<[String: Int]>([:])

  func increment(_ name: String) {
    self.counts.withLock { $0[name, default: 0] += 1 }
  }

  func count(_ name: String) -> Int {
    self.counts.withLock { $0[name] ?? 0 }
  }

  func reset(_ name: String) {
    self.counts.withLock { $0[name] = nil }
  }
}

/// `primary` failed in a past run (healed by now — succeeds if dispatched
/// live); `fallback` is the compensation path.
@ActivityContainer
public struct CompensatingActivities {
  @Activity
  public func primary(input: String, context: ActivityContext) async throws -> String {
    InvocationCounter.shared.increment("primary")
    return "primary ok"
  }

  @Activity
  public func fallback(input: String, context: ActivityContext) async throws -> String {
    InvocationCounter.shared.increment("fallback")
    return "fallback \(input)"
  }
}

/// Catches an activity failure and continues down a compensation branch —
/// the shape whose replay must rethrow the journaled failure, not re-dispatch.
@Workflow
public struct CompensatingWorkflow {
  public typealias Activities = CompensatingActivities

  public struct Input: Codable, Sendable {
    public init() {}
  }

  public init() {}

  public func run(input: Input, context: WorkflowContext) async throws -> String {
    let result: String
    do {
      result = try await context.executeActivity(
        CompensatingActivities.Activities.Primary.self,
        input: "live"
      )
    } catch {
      result = try await context.executeActivity(
        CompensatingActivities.Activities.Fallback.self,
        input: "live"
      )
    }
    try await context.sleep(for: .seconds(30), summary: "post-compensation window")
    return result
  }
}

/// Boots a single-node cluster for `CompensatingWorkflow`. See `makeWorkflowSystem`.
func makeCompensatingWorkflowSystem(
  name: String,
  port: Int,
  store: InMemoryEventStore
) async throws -> (ClusterSystem, VirtualNode, DurableActivityDispatchWorker<CompensatingWorkflow>) {
  let system = await ClusterSystem(name) {
    $0.bindPort = port
    $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
    $0.plugins.install(plugin: ClusterSingletonPlugin())
    $0.plugins.install(plugin: ClusterVirtualActorsPlugin(replicationFactor: 10))
    $0.plugins.install(plugin: ClusterJournalPlugin { _ in store })
    $0.plugins.install(plugin: DurableWorkflowsPlugin())
  }
  let node = await VirtualNode(actorSystem: system)
  let worker = await DurableActivityDispatchWorker<CompensatingWorkflow>(actorSystem: system)
  system.cluster.join(endpoint: system.cluster.endpoint)
  try await system.cluster.joined(within: .seconds(5))
  return (system, node, worker)
}

/// Boots a single-node cluster for `FlakyWorkflow`. See `makeWorkflowSystem`.
func makeFlakyWorkflowSystem(
  name: String,
  port: Int,
  store: any EventStore
) async throws -> (ClusterSystem, VirtualNode, DurableActivityDispatchWorker<FlakyWorkflow>) {
  let system = await ClusterSystem(name) {
    $0.bindPort = port
    $0.autoLeaderElection = .lowestReachable(minNumberOfMembers: 1)
    $0.plugins.install(plugin: ClusterSingletonPlugin())
    $0.plugins.install(plugin: ClusterVirtualActorsPlugin(replicationFactor: 10))
    $0.plugins.install(plugin: ClusterJournalPlugin { _ in store })
    $0.plugins.install(plugin: DurableWorkflowsPlugin())
  }
  let node = await VirtualNode(actorSystem: system)
  let worker = await DurableActivityDispatchWorker<FlakyWorkflow>(actorSystem: system)
  system.cluster.join(endpoint: system.cluster.endpoint)
  try await system.cluster.joined(within: .seconds(5))
  return (system, node, worker)
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

/// Blocks all persists while the gate is closed — holds a workflow's first
/// `executionStarted` emit hostage to widen the execute-start race window
/// deterministically.
final class GatedEventStore: EventStore, Sendable {
  private let inner: InMemoryEventStore
  private let _closed = Mutex(true)

  init(inner: InMemoryEventStore) {
    self.inner = inner
  }

  func open() {
    self._closed.withLock { $0 = false }
  }

  func persistEvent<Event: Codable & Sendable>(
    _ event: Event,
    id: String,
    sequenceNumber: Int64
  ) async throws {
    while self._closed.withLock({ $0 }) {
      try await Task.sleep(for: .milliseconds(10))
    }
    try await self.inner.persistEvent(event, id: id, sequenceNumber: sequenceNumber)
  }

  func eventStream<Event: Codable & Sendable>(
    id: String,
    fromSequenceNumber: Int64
  ) async throws -> EventStream<Event> {
    try await self.inner.eventStream(id: id, fromSequenceNumber: fromSequenceNumber)
  }
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
