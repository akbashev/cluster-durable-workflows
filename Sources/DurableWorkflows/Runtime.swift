import Distributed
import DistributedCluster
import EventSourcing
import Foundation
import ServiceLifecycle
import VirtualActors

public enum WorkflowRuntimeError: Error, Sendable, Equatable {
  case activityContainerNotRegistered(String)
  case unknownActivityFailure(String)
  case workflowAlreadyRunning
  case workflowInputMismatch
  case workflowCancelled
  case workflowNotRunning
}

public enum ApplicationError: Error, Codable, Sendable {
  case typed(message: String, type: String, isNonRetryable: Bool)
}

public struct ActivityFailurePayload: Codable, Sendable {
  public let message: String
  public let type: String
  public let isNonRetryable: Bool

  public init(message: String, type: String, isNonRetryable: Bool) {
    self.message = message
    self.type = type
    self.isNonRetryable = isNonRetryable
  }
}

public struct ActivityInvocation: Codable, Sendable {
  public let name: String
  public let inputData: Data
  public let workflowID: String

  public init(name: String, inputData: Data, workflowID: String) {
    self.name = name
    self.inputData = inputData
    self.workflowID = workflowID
  }
}

public enum ActivityInvocationResult: Codable, Sendable {
  case success(outputData: Data)
  case failure(ActivityFailurePayload)
}

public struct ActivityContext: Sendable {
  public let workflowID: String
  public let activityName: String
  public let system: ClusterSystem

  public init(workflowID: String, activityName: String, system: ClusterSystem) {
    self.workflowID = workflowID
    self.activityName = activityName
    self.system = system
  }
}

public distributed actor DurableActivityDispatchWorker<WorkflowType: WorkflowProtocol>: DistributedWorker {
  public typealias ActorSystem = ClusterSystem
  public typealias WorkItem = ActivityInvocation
  public typealias WorkResult = ActivityInvocationResult

  private let container: WorkflowType.Activities
  private var recoverTask: Task<Void, Never>?

  public init(actorSystem: ClusterSystem) async {
    self.actorSystem = actorSystem
    self.container = WorkflowType.Activities()
    await self.actorSystem.receptionist.checkIn(self, with: .durableWorkers(for: WorkflowType.self))
    Task {
      try? await self.actorSystem.workflows.recoverAll(ofType: WorkflowType.self)
    }
  }

  distributed public func submit(work: ActivityInvocation) async throws -> ActivityInvocationResult {
    do {
      let output = try await container.handle(invocation: work, on: self.actorSystem)
      return .success(outputData: output)
    } catch let applicationError as ApplicationError {
      switch applicationError {
      case .typed(let message, let type, let isNonRetryable):
        return .failure(.init(message: message, type: type, isNonRetryable: isNonRetryable))
      }
    } catch {
      return .failure(.init(message: String(describing: error), type: "ActivityError", isNonRetryable: false))
    }
  }

  private func checkRecover() {
    self.recoverTask = Task {
      defer {
        self.recoverTask = nil
      }

      for await event in self.actorSystem.cluster.events {
        print("event::", event)
        if case .membershipChange(let change) = event {
          guard change.node == self.actorSystem.cluster.node else {
            continue
          }
          guard change.isUp else {
            continue
          }
          try? await self.actorSystem.workflows.recoverAll(ofType: WorkflowType.self)
          // finish checking events
          return
        }
      }
    }
  }
}

extension DistributedReception.Key {
  public static func durableWorkers<W: WorkflowProtocol>(for type: W.Type) -> DistributedReception.Key<DurableActivityDispatchWorker<W>> {
    .init(id: "durable-workers-\(String(describing: W.Activities.self))")
  }
}

public enum ActivityOutcomeRecord: Codable, Sendable {
  case success(outputData: Data)
  case failure(ActivityFailurePayload)
}

public actor ActivityExecutionCursor {
  private var nextIndex: Int = 0
  private let cachedOutcomes: [Int: ActivityOutcomeRecord]

  init(cachedOutcomes: [Int: ActivityOutcomeRecord]) {
    self.cachedOutcomes = cachedOutcomes
  }

  func nextCall() -> (index: Int, cached: ActivityOutcomeRecord?) {
    let index = self.nextIndex
    self.nextIndex += 1
    return (index, self.cachedOutcomes[index])
  }
}

public struct WorkflowContext: Sendable {
  private let cursor: ActivityExecutionCursor
  private let workflowID: String
  public let system: ClusterSystem
  private let dispatch: @Sendable (Int, ActivityInvocation, ActivityOptions) async throws -> Data
  private let decoder: JSONDecoder
  private let encoder: JSONEncoder

  init(
    cursor: ActivityExecutionCursor,
    workflowID: String,
    system: ClusterSystem,
    dispatch: @escaping @Sendable (Int, ActivityInvocation, ActivityOptions) async throws -> Data
  ) {
    self.cursor = cursor
    self.workflowID = workflowID
    self.system = system
    self.dispatch = dispatch

    let decoder = JSONDecoder()
    decoder.userInfo[.actorSystemKey] = system
    self.decoder = decoder

    let encoder = JSONEncoder()
    encoder.userInfo[.actorSystemKey] = system
    self.encoder = encoder
  }

  public func getActor<ActorType: VirtualActor>(
    identifiedBy id: VirtualActorID,
    dependency: any Sendable & Codable
  ) async throws -> ActorType {
    try await self.system.virtualActors.getActor(identifiedBy: id, dependency: dependency)
  }

  @discardableResult
  public func executeActivity<ActivityType: ActivityReference>(
    _ activity: ActivityType.Type,
    options: ActivityOptions = .init(),
    input: ActivityType.Input
  ) async throws -> ActivityType.Output {
    try Task.checkCancellation()

    let (index, cached) = await cursor.nextCall()

    if let cached {
      switch cached {
      case .success(let outputData):
        return try decoder.decode(ActivityType.Output.self, from: outputData)
      case .failure(let failure):
        throw ApplicationError.typed(
          message: failure.message,
          type: failure.type,
          isNonRetryable: failure.isNonRetryable
        )
      }
    }

    let inputData = try encoder.encode(input)
    let invocation = ActivityInvocation(
      name: ActivityType.name,
      inputData: inputData,
      workflowID: workflowID
    )

    let outputData = try await dispatch(index, invocation, options)
    return try decoder.decode(ActivityType.Output.self, from: outputData)
  }
}

public enum WorkflowEvent: Codable, Sendable {
  case executionStarted(inputData: Data)
  case activitySucceeded(index: Int, name: String, outputData: Data)
  case activityFailed(index: Int, name: String, failure: ActivityFailurePayload)
  case executionCompleted(outputData: Data)
  case executionCancelled
  case executionFailed(message: String)
}

public enum WorkflowStatus: Codable, Sendable, Equatable {
  case idle
  case running
  case completed(data: Data)
  case cancelled
  case failed(error: String)

  public var name: String {
    switch self {
    case .idle: "idle"
    case .running: "running"
    case .completed: "completed"
    case .cancelled: "cancelled"
    case .failed: "failed"
    }
  }
}

public struct WorkflowStatusInfo: Codable, Sendable {
  public let status: WorkflowStatus
  public let events: [WorkflowEvent]
}

@EventSourced
public distributed actor WorkflowActor<WorkflowType: WorkflowProtocol>: VirtualActor {
  public typealias ActorSystem = ClusterSystem
  public typealias Event = WorkflowEvent

  public struct Dependency: Codable, Sendable {
    public let workflowID: String

    public init(workflowID: String) {
      self.workflowID = workflowID
    }
  }

  struct State: Sendable {
    var status: WorkflowStatus = .idle
    var inputData: Data?
    var activityOutcomes: [Int: ActivityOutcomeRecord] = [:]
    var events: [WorkflowEvent] = []
    var error: String?
  }

  private var state = State()
  private let encoder: JSONEncoder
  private let decoder: JSONDecoder

  private let persistenceID: String
  private let workflowID: String
  private let activityPool: WorkerPool<DurableActivityDispatchWorker<WorkflowType>>
  private var currentExecutionTask: Task<WorkflowResult<WorkflowType.Output>, Error>?

  public init(actorSystem: ClusterSystem, dependency: Dependency) async throws {
    self.actorSystem = actorSystem
    self.persistenceID = "\(WorkflowType.name)-\(dependency.workflowID)"
    self.workflowID = dependency.workflowID

    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    encoder.userInfo[.actorSystemKey] = actorSystem
    self.encoder = encoder

    let decoder = JSONDecoder()
    decoder.userInfo[.actorSystemKey] = actorSystem
    self.decoder = decoder

    let poolSettings = WorkerPoolSettings<DurableActivityDispatchWorker<WorkflowType>>(
      selector: .dynamic(.durableWorkers(for: WorkflowType.self)),
      strategy: .simpleRoundRobin
    )
    self.activityPool = try await WorkerPool(settings: poolSettings, actorSystem: actorSystem)

    try await actorSystem.journal.register(actor: self, with: self.persistenceID)

    if case .running = self.state.status {
      Task { try? await self.resume() }
    }
  }

  public static func spawn(
    on actorSystem: ClusterSystem,
    dependency: any Sendable & Codable
  ) async throws -> Self {
    guard let typedDependency = dependency as? Dependency else {
      throw VirtualActorError.spawnDependencyTypeMismatch
    }
    return try await Self(actorSystem: actorSystem, dependency: typedDependency)
  }

  distributed public func getStatus() async throws -> WorkflowStatusInfo {
    WorkflowStatusInfo(
      status: self.state.status,
      events: self.state.events
    )
  }

  distributed public func cancel() async throws {
    guard case .running = self.state.status else { return }
    try await self.emit(event: .executionCancelled)
    self.currentExecutionTask?.cancel()
    self.currentExecutionTask = nil
  }

  @discardableResult
  distributed public func resume() async throws -> WorkflowResult<WorkflowType.Output> {
    switch self.state.status {
    case .running:
      if let task = self.currentExecutionTask { return try await task.value }
      guard let inputData = self.state.inputData else {
        throw WorkflowRuntimeError.workflowInputMismatch
      }
      let input = try self.decoder.decode(WorkflowType.Input.self, from: inputData)
      return try await self._run(input: input)
    case .completed(let outputData):
      let output = try self.decoder.decode(WorkflowType.Output.self, from: outputData)
      return WorkflowResult(output: output)
    case .cancelled:
      throw WorkflowRuntimeError.workflowCancelled
    case .idle, .failed:
      throw WorkflowRuntimeError.workflowNotRunning
    }
  }

  @discardableResult
  distributed public func execute(input: WorkflowType.Input) async throws -> WorkflowResult<WorkflowType.Output> {
    let inputData = try self.encoder.encode(input)
    if let previousInputData = self.state.inputData, previousInputData != inputData {
      throw WorkflowRuntimeError.workflowInputMismatch
    }

    switch self.state.status {
    case .completed(let outputData):
      if let output = try? self.decoder.decode(WorkflowType.Output.self, from: outputData) {
        return WorkflowResult(output: output)
      }
    case .running:
      if let task = self.currentExecutionTask { return try await task.value }
      return try await self._run(input: input)
    case .cancelled:
      throw WorkflowRuntimeError.workflowCancelled
    case .idle, .failed:
      break
    }

    try await self.emit(event: .executionStarted(inputData: inputData))
    return try await self._run(input: input)
  }

  private func _run(input: WorkflowType.Input) async throws -> WorkflowResult<WorkflowType.Output> {
    let workflowTask = Task {
      let workflow = WorkflowType()
      let cursor = ActivityExecutionCursor(cachedOutcomes: self.state.activityOutcomes)
      let context = WorkflowContext(
        cursor: cursor,
        workflowID: self.workflowID,
        system: self.actorSystem,
        dispatch: { index, invocation, _ in
          let result = try await self.activityPool.submit(work: invocation)
          switch result {
          case .success(let outputData):
            try await self.emit(
              event: .activitySucceeded(
                index: index,
                name: invocation.name,
                outputData: outputData
              )
            )
            return outputData
          case .failure(let failure):
            try await self.emit(
              event: .activityFailed(
                index: index,
                name: invocation.name,
                failure: failure
              )
            )
            throw ApplicationError.typed(
              message: failure.message,
              type: failure.type,
              isNonRetryable: failure.isNonRetryable
            )
          }
        }
      )

      do {
        let output = try await workflow.run(input: input, context: context)
        let outputData = try self.encoder.encode(output)
        try await self.emit(event: .executionCompleted(outputData: outputData))
        return WorkflowResult(output: output)
      } catch {
        try await self.emit(event: .executionFailed(message: String(describing: error)))
        throw error
      }
    }

    self.currentExecutionTask = workflowTask
    defer { self.currentExecutionTask = nil }

    return try await withTaskCancellationHandler {
      try await workflowTask.value
    } onCancel: {
      workflowTask.cancel()
    }
  }

  distributed public func history() -> [WorkflowEvent] {
    self.state.events
  }

  distributed public func handleEvent(_ event: WorkflowEvent) {
    self.state.events.append(event)

    switch event {
    case .executionStarted(let inputData):
      self.state.status = .running
      self.state.inputData = inputData
      self.state.error = nil
    case .activitySucceeded(let index, _, let outputData):
      self.state.activityOutcomes[index] = .success(outputData: outputData)
    case .activityFailed(let index, _, let failure):
      self.state.activityOutcomes[index] = .failure(failure)
    case .executionCompleted(let outputData):
      self.state.status = .completed(data: outputData)
      self.state.error = nil
    case .executionFailed(let message):
      self.state.status = .failed(error: message)
      self.state.error = message
    case .executionCancelled:
      self.state.status = .cancelled
    }
  }
}
