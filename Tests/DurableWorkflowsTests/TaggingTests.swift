import DistributedCluster
import EventSourcing
import Foundation
import Testing

@testable import DurableWorkflows

@Suite(.serialized)
struct TaggingTests {
  /// Every event of a workflow type's runs, and of its schedules, is kept with that type's tag, so
  /// a read side can follow them all: from a run's start to its end, and from a schedule's making
  /// to its cancelling.
  @Test
  func runsAndSchedulesTagTheirEventsByType() async throws {
    let store = InMemoryEventStore()
    let (system, node, worker) = try await makeWorkflowSystem(name: "tags", port: 4640, store: store)

    _ = try await system.workflows.execute(
      type: TimerWorkflow.self,
      options: WorkflowOptions(id: "wf-tagged"),
      input: .init(delayMillis: 1)
    )
    try await system.workflows.schedule(
      id: "tagged-schedule",
      recurrence: .daily(calendar: Calendar(identifier: .gregorian), hours: [8], minutes: [30]),
      workflow: TimerWorkflow.self,
      input: .init(delayMillis: 1)
    )
    try await system.workflows.cancelSchedule(id: "tagged-schedule", workflow: TimerWorkflow.self)

    var runs: [WorkflowEvent] = []
    let tagged: EventStream<WorkflowEvent> = try await store.currentEventsByTag(
      .workflows(of: TimerWorkflow.self),
      after: .start
    )
    for try await envelope in tagged {
      runs.append(envelope.event)
    }
    guard case .executionStarted = runs.first, case .executionCompleted = runs.last else {
      Issue.record("expected the run from its start to its end, got \(runs)")
      return
    }

    var schedules: [WorkflowSchedulerEvent] = []
    let scheduled: EventStream<WorkflowSchedulerEvent> = try await store.currentEventsByTag(
      .schedules(of: TimerWorkflow.self),
      after: .start
    )
    for try await envelope in scheduled {
      schedules.append(envelope.event)
    }
    guard case .scheduled(let record) = schedules.first, case .cancelled = schedules.last else {
      Issue.record("expected the schedule from its making to its cancelling, got \(schedules)")
      return
    }
    #expect(record.id == "tagged-schedule")
    #expect(EventTag.workflows(of: TimerWorkflow.self) != EventTag.schedules(of: TimerWorkflow.self))

    _ = node
    _ = worker
  }
}
