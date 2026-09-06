import DistributedCluster
import DurableWorkflows
import Foundation
import Testing

/// Exercises the ordinary restart path through the durable plugin registry.
/// The Norby application supplies the activities; this test verifies that the
/// workflow and scheduler actors are rediscovered from durable state.
@Suite(.timeLimit(.minutes(2)), .serialized)
struct WorkflowRecoveryTests {
  @Test
  func runningTimerAndScheduleSurviveRestart() async throws {
    let store = InMemoryEventStore()
    let (first, firstNode, firstWorker) = try await makeWorkflowSystem(
      name: "workflow-recovery-before",
      port: 4630,
      store: store
    )

    let timerOptions = WorkflowOptions(id: "restart-timer")
    try await first.workflows.start(
      type: TimerWorkflow.self,
      options: timerOptions,
      input: .init(delayMillis: 500)
    )
    try await eventually {
      let status = try await first.workflows.getStatus(
        type: TimerWorkflow.self,
        options: timerOptions
      )
      return status.events.contains {
        if case .timerScheduled = $0 { return true }
        return false
      }
    }

    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = TimeZone(secondsFromGMT: 0)!
    let currentHour = calendar.component(.hour, from: .now)
    let nextHour = (currentHour + 1) % 24
    let recurrence = Calendar.RecurrenceRule.daily(
      calendar: calendar,
      hours: [nextHour],
      minutes: [0]
    )
    try await first.workflows.schedule(
      id: "restart-schedule",
      recurrence: recurrence,
      workflow: TimerWorkflow.self,
      input: .init(delayMillis: 1)
    )
    let firstDescription = try await first.workflows.describeSchedule(
      id: "restart-schedule",
      workflow: TimerWorkflow.self
    )
    #expect(firstDescription?.nextFireAt != nil)

    // Registering the same durable schedule again is a no-op.
    try await first.workflows.schedule(
      id: "restart-schedule",
      recurrence: recurrence,
      workflow: TimerWorkflow.self,
      input: .init(delayMillis: 2)
    )
    let repeatedDescription = try await first.workflows.describeSchedule(
      id: "restart-schedule",
      workflow: TimerWorkflow.self
    )
    #expect(repeatedDescription?.createdAt == firstDescription?.createdAt)

    _ = firstNode
    _ = firstWorker
    try await first.shutdown().wait()

    let (second, secondNode, secondWorker) = try await makeWorkflowSystem(
      name: "workflow-recovery-after",
      port: 4631,
      store: store
    )
    try await second.workflows.recoverAll(ofType: TimerWorkflow.self)

    try await eventually {
      let status = try await second.workflows.getStatus(
        type: TimerWorkflow.self,
        options: timerOptions
      )
      if case .completed = status.status { return true }
      return false
    }

    let recoveredDescription = try await second.workflows.describeSchedule(
      id: "restart-schedule",
      workflow: TimerWorkflow.self
    )
    #expect(recoveredDescription?.nextFireAt != nil)

    try await second.workflows.cancelSchedule(
      id: "restart-schedule",
      workflow: TimerWorkflow.self
    )
    #expect(
      try await second.workflows.describeSchedule(
        id: "restart-schedule",
        workflow: TimerWorkflow.self
      ) == nil
    )

    _ = secondNode
    _ = secondWorker
    try await second.shutdown().wait()
  }
}
