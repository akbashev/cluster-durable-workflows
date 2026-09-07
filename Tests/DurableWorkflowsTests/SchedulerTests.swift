import DistributedCluster
import Foundation
import Testing

@testable import DurableWorkflows

@Suite(.serialized)
struct SchedulerTests {
  @Test
  func occurrenceIDIsDeterministic() {
    let date = Date(timeIntervalSince1970: 1_700_000_000)
    let first = ScheduleOccurrenceID(scheduleID: "daily-report", scheduledAt: date)
    let second = ScheduleOccurrenceID(scheduleID: "daily-report", scheduledAt: date)
    #expect(first == second)
    #expect(first.description == "daily-report-1700000000000")
  }

  @Test
  func reducerTracksOccurrenceLifecycle() {
    let date = Date(timeIntervalSince1970: 1_700_000_000)
    let occurrence = ScheduledWorkflowOccurrence(
      id: .init(scheduleID: "daily-report", scheduledAt: date),
      scheduleID: "daily-report",
      workflowType: TimerWorkflow.name,
      inputData: Data(),
      retryPolicy: nil,
      scheduledAt: date
    )
    var state = WorkflowScheduler<TimerWorkflow>.State()
    state = WorkflowScheduler<TimerWorkflow>.reduce(state: state, event: .occurrenceClaimed(occurrence))
    guard case .pending = state.executions[occurrence.id] else {
      Issue.record("expected pending occurrence")
      return
    }
    state = WorkflowScheduler<TimerWorkflow>.reduce(state: state, event: .occurrenceStarted(occurrence))
    guard case .running = state.executions[occurrence.id] else {
      Issue.record("expected running occurrence")
      return
    }
    state = WorkflowScheduler<TimerWorkflow>.reduce(state: state, event: .occurrenceFinished(id: occurrence.id))
    #expect(state.executions[occurrence.id] == nil)
  }

  @Test
  func manualOccurrenceDoesNotAdvanceTheRecurringSchedule() {
    let scheduledAt = Date(timeIntervalSince1970: 1_700_000_000)
    let manualAt = scheduledAt.addingTimeInterval(12)
    let recurrence = Calendar.RecurrenceRule.daily(
      calendar: Calendar(identifier: .gregorian),
      hours: [8],
      minutes: [30]
    )
    let record = ScheduledWorkflowRecord(
      id: "daily-report",
      recurrence: recurrence,
      startedAt: scheduledAt,
      workflowType: TimerWorkflow.name,
      inputData: Data(),
      retryPolicy: nil,
      lastFiredAt: nil
    )
    let manual = WorkflowScheduler<TimerWorkflow>.occurrence(
      for: record,
      at: manualAt,
      triggerID: UUID()
    )

    var state = WorkflowScheduler<TimerWorkflow>.reduce(
      state: .init(),
      event: .scheduled(record)
    )
    state = WorkflowScheduler<TimerWorkflow>.reduce(
      state: state,
      event: .occurrenceTriggered(manual)
    )

    #expect(state.schedule?.lastFiredAt == nil)
    #expect(state.executions[manual.id] != nil)
    #expect(manual.workflowID.rawValue.contains("trigger-"))
  }

  @Test
  func recurrenceComputesNextCalendarFire() throws {
    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = TimeZone(secondsFromGMT: 0)!
    let recurrence = Calendar.RecurrenceRule.daily(calendar: calendar, hours: [8], minutes: [30])
    let start = Date(timeIntervalSince1970: 1_700_000_000)
    let record = ScheduledWorkflowRecord(
      id: "daily-report",
      recurrence: recurrence,
      startedAt: start,
      workflowType: TimerWorkflow.name,
      inputData: Data(),
      retryPolicy: nil,
      lastFiredAt: nil
    )
    let next = WorkflowScheduler<TimerWorkflow>.nextFireDate(for: record)
    #expect(next != nil)
    #expect(calendar.component(.hour, from: next!) == 8)
    #expect(calendar.component(.minute, from: next!) == 30)
  }

  @Test
  func scheduleIsIdempotentAndCanBeCancelled() async throws {
    let (system, node, worker) = try await makeWorkflowSystem(
      name: "scheduler-lifecycle",
      port: 4620,
      store: InMemoryEventStore()
    )
    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = TimeZone(secondsFromGMT: 0)!
    let recurrence = Calendar.RecurrenceRule.daily(calendar: calendar, hours: [8], minutes: [30])

    try await system.workflows.schedule(
      id: "daily-report",
      recurrence: recurrence,
      workflow: TimerWorkflow.self,
      input: .init(delayMillis: 1)
    )
    try await system.workflows.schedule(
      id: "daily-report",
      recurrence: recurrence,
      workflow: TimerWorkflow.self,
      input: .init(delayMillis: 2)
    )
    let description = try await system.workflows.describeSchedule(id: "daily-report", workflow: TimerWorkflow.self)
    #expect(description?.id == "daily-report")
    #expect(description?.workflowType == TimerWorkflow.name)
    #expect(description?.createdAt != nil)
    #expect(description?.lastFiredAt == nil)
    #expect(description?.nextFireAt != nil)
    #expect(description?.nextFireIn != nil)
    #expect(description?.executions.isEmpty == true)

    try await system.workflows.cancelSchedule(id: "daily-report", workflow: TimerWorkflow.self)
    #expect(try await system.workflows.describeSchedule(id: "daily-report", workflow: TimerWorkflow.self) == nil)
    _ = node
    _ = worker
  }
}
