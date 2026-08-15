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
  public let id: WorkflowID
  public let retryPolicy: RetryPolicy?

  public init(id: WorkflowID, retryPolicy: RetryPolicy? = nil) {
    self.id = id
    self.retryPolicy = retryPolicy
  }
}

/// Identity of a workflow instance, chosen by the caller. A domain type so a
/// workflow id can't be silently passed where a persistence id or an
/// activity key is expected. String literals convert directly.
public struct WorkflowID: Hashable, Codable, Sendable, CustomStringConvertible,
  ExpressibleByStringLiteral
{
  public let rawValue: String

  public init(rawValue: String) {
    self.rawValue = rawValue
  }

  public init(stringLiteral value: String) {
    self.rawValue = value
  }

  public var description: String { self.rawValue }
}

/// Journal and virtual-actor identity for a workflow type/id pair. The ONE
/// place this format lives: both the plugin (actor identity) and the actor
/// (journal id) derive it here, so the two can never drift.
public struct WorkflowPersistenceID: Hashable, Codable, Sendable, CustomStringConvertible {
  public let rawValue: String

  public init(workflowType: String, id: WorkflowID) {
    self.rawValue = "\(workflowType)-\(id.rawValue)"
  }

  public var description: String { self.rawValue }
}

/// Content-addressed identity of an activity call within a run: which
/// activity, with exactly which encoded input. Owns the key format — the
/// runtime builds it, views and tests read it; nothing parses strings.
public struct ActivityKey: Hashable, Codable, Sendable {
  public let name: String
  public let inputData: Data

  public init(name: String, inputData: Data) {
    self.name = name
    self.inputData = inputData
  }
}

/// Automatic retry policy for a workflow run, mirroring Temporal's
/// `RetryPolicy`. When set, a failed run is retried with exponential backoff
/// instead of going terminal: each retry is journaled (`retryScheduled` with
/// an absolute deadline), so a crash mid-backoff resumes the wait rather
/// than losing the retry.
///
/// Unlike Temporal, a retried attempt here REUSES the journal: completed
/// activities replay from cache, so a workflow-level retry costs only the
/// steps that never completed.
public struct RetryPolicy: Codable, Sendable {
  /// Backoff before the first retry.
  public let initialInterval: Duration
  /// Multiplier applied to the backoff after each retry (>= 1).
  public let backoffCoefficient: Double
  /// Upper bound for the backoff interval (nil = unbounded).
  public let maximumInterval: Duration?
  /// Total number of attempts INCLUDING the first run (1 = never retry).
  public let maximumAttempts: Int
  /// `ApplicationError` type names that should fail immediately, never retry.
  public let nonRetryableErrorTypes: [String]

  public init(
    initialInterval: Duration = .milliseconds(200),
    backoffCoefficient: Double = 1.5,
    maximumInterval: Duration? = nil,
    maximumAttempts: Int = 3,
    nonRetryableErrorTypes: [String] = []
  ) {
    self.initialInterval = initialInterval
    self.backoffCoefficient = backoffCoefficient
    self.maximumInterval = maximumInterval
    self.maximumAttempts = maximumAttempts
    self.nonRetryableErrorTypes = nonRetryableErrorTypes
  }

  /// Backoff interval calculator, rebuilt per failure from the journaled
  /// policy. Values are clamped to the strategy's documented preconditions.
  /// Jitter (`randomFactor`) is safe here: it is rolled once, live, and the
  /// result is journaled — replay reads the deadline, never re-rolls.
  func backoffStrategy() -> ExponentialBackoffStrategy {
    let initial = max(self.initialInterval, .milliseconds(1))
    return Backoff.exponential(
      initialInterval: initial,
      multiplier: max(1.0, self.backoffCoefficient),
      capInterval: max(self.maximumInterval ?? .nanoseconds(Int64.max), initial),
      randomFactor: 0.25,
      maxAttempts: nil
    )
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
