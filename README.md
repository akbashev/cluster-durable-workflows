# DurableWorkflows

A research project exploring durable workflow patterns in Swift, built on top of [swift-distributed-actors](https://github.com/apple/swift-distributed-actors), [cluster-event-sourcing](https://github.com/akbashev/cluster-event-sourcing), and [cluster-virtual-actors](https://github.com/akbashev/cluster-virtual-actors).

The `Examples` directory contains a full working example with a web frontend:

![Booking example](Examples/booking-example.png)

The goal was to mimic the design of [swift-temporal-sdk](https://github.com/apple/swift-temporal-sdk) — workflows that survive process crashes and node restarts — but built on top of distributed actors. This was a few-days experiment, not a production system, so expect rough edges that a full Temporal deployment handles for you.

Each activity result is persisted before moving to the next step, so a workflow replays only the activities that haven't completed yet when it resumes.

## Comparison with `swift-temporal-sdk`

| | **DurableWorkflows** | **swift-temporal-sdk** |
|---|---|---|
| **Runtime** | Self-hosted, built on `swift-distributed-actors` | Requires a running Temporal server |
| **Storage** | Pluggable `EventStore` (file, Postgres, …) | Temporal's own persistence (Postgres/Cassandra) |
| **Workflow definition** | `@Workflow` | `@Workflow` |
| **Activity definition** | `@Activity` inside `@ActivityContainer` | `@Activity` inside `@ActivityContainer` |
| **Maturity** | Research / showcase | Production-ready |

The core idea is the same — workflows are deterministic functions whose intermediate results are persisted — but DurableWorkflows is fully self-contained Swift with no external services required beyond an event store.

> [!TIP]
> One interesting side effect of building on `swift-distributed-actors`: distributed actors are `Codable` and `Sendable` by default, so you can pass them directly as activity inputs. This lets activities call back into actors to signal state — for example, notifying a `UserActor` of balance changes mid-workflow — without any extra plumbing.

## Concepts

**Workflow** — a plain Swift function that orchestrates activities. It must be deterministic: the same sequence of activity results always produces the same output. Defined with `@Workflow`.

**Activity** — a side-effectful unit of work (API call, DB write, payment charge). Defined with `@Activity` inside an `@ActivityContainer`. Activities are individually persisted and never re-executed on replay.

**WorkflowContext** — passed into `run(input:context:)`. Use it to execute activities (`context.executeActivity(...)`) and resolve distributed actors (`context.getActor(...)`).

**Event store** — pluggable persistence for workflow and activity events. Swap between file-based (dev) and Postgres (production) without changing any workflow code.

## Requirements

- Swift 6.2+
- macOS 26+

## Installation

```swift
// Package.swift
dependencies: [
    .package(url: "https://github.com/akbashev/durable-workflows.git", branch: "main"),
],
targets: [
    .target(
        name: "MyTarget",
        dependencies: [
            .product(name: "DurableWorkflows", package: "durable-workflows"),
        ]
    ),
]
```

## Usage

### 1. Define activities

```swift
import DurableWorkflows

@ActivityContainer
struct OrderActivities {
    @Activity
    func chargePayment(input: ChargeRequest, context: ActivityContext) async throws -> String {
        // call payment API — result is persisted, never retried on replay
        return try await paymentGateway.charge(input.amountCents)
    }

    @Activity
    func sendConfirmation(input: SendRequest, context: ActivityContext) async throws {
        try await emailService.send(to: input.email, body: "Order confirmed!")
    }
}
```

### 2. Define the workflow

```swift
@Workflow
struct OrderWorkflow {
    typealias Activities = OrderActivities

    struct Input: Codable, Sendable { let orderId: String; let email: String; let amountCents: Int }
    struct Output: Codable, Sendable { let chargeId: String }

    func run(input: Input, context: WorkflowContext) async throws -> Output {
        let chargeId = try await context.executeActivity(
            OrderActivities.Activities.ChargePayment.self,
            input: .init(amountCents: input.amountCents)
        )
        try await context.executeActivity(
            OrderActivities.Activities.SendConfirmation.self,
            input: .init(email: input.email)
        )
        return Output(chargeId: chargeId)
    }
}
```

### 3. Register the plugin and run

```swift
import DurableWorkflows
import EventSourcing

let store: any EventStore = MyEventStore()

let system = await ClusterSystem("my-app") {
    $0.plugins.install(plugin: ClusterSingletonPlugin())
    $0.plugins.install(plugin: ClusterVirtualActorsPlugin())
    $0.plugins.install(plugin: ClusterJournalPlugin { _ in store })
    $0.plugins.install(plugin: DurableWorkflowsPlugin())
}

// Start a worker that executes activities
let worker = await DurableActivityDispatchWorker<OrderWorkflow>(actorSystem: system)
```

### 4. Execute a workflow

```swift
let output = try await system.workflows.execute(
    type: OrderWorkflow.self,
    options: WorkflowOptions(id: "order-\(orderId)"),
    input: .init(orderId: orderId, email: email, amountCents: 9900)
)
print(output.chargeId)
```

### Status

```swift
let info = try await system.workflows.getStatus(type: OrderWorkflow.self, options: options)
print(info.status)  // .idle / .running / .completed(data:) / .cancelled / .failed(error:)
print(info.events)  // full activity history
```

### Cancellation

Cancellation stops the current execution task and persists a `.cancelled` status, so the workflow will not resume on restart.

```swift
try await system.workflows.cancel(type: OrderWorkflow.self, options: options)
```

To handle cancellation gracefully inside the workflow, wrap the compensation logic in a detached `Task` — cancellation propagates through Swift's structured concurrency, so any `try await` inside `run` will throw `CancellationError`:

```swift
func run(input: Input, context: WorkflowContext) async throws -> Output {
    var reservationId: String?

    do {
        reservationId = try await context.executeActivity(ReserveSpot.self, input: input)
        // ... more activities
    } catch {
        // CancellationError or activity failure — compensate
        let task = Task {
            if let id = reservationId {
                try? await context.executeActivity(CancelSpot.self, input: .init(id: id))
            }
            throw error
        }
        return try await task.value
    }
}
```

## Durability

If the process crashes mid-workflow, the next call to `execute` or `resume` replays the workflow from the persisted event log. Completed activities are served from cache — their side effects do **not** run again. Only the next pending activity is dispatched to a worker.

## License

Apache 2.0 — see [LICENSE.txt](LICENSE.txt).
