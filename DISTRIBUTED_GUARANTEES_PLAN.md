# Distributed Guarantees Plan

This is a separate follow-up plan for improving the promises made by
`cluster-durable-workflows` in the presence of crashes, retries, node changes,
partitions, duplicate messages, and concurrent callers.

It is deliberately not required for the current MVP. The runtime currently
aims for durable workflow state and best-effort recovery; this document defines
how to make each stronger promise explicit before adding machinery.

## 1. Name the promises first

- [ ] Define the vocabulary used by the public API:
  - accepted: the durable command boundary was recorded;
  - running: durable state says execution may continue;
  - completed: a durable result exists;
  - cancelled: cancellation won the lifecycle race;
  - delivered: an external effect acknowledged receipt.
- [ ] Document the guarantee for every operation:
  - workflow start: accepted at most once for one `(workflowID, input)`;
  - workflow execution: resumes after a crash when its durable start exists;
  - activity dispatch: at-least-once unless the activity supplies idempotency;
  - timer: fires according to its durable deadline, possibly immediately after
    recovery when the deadline has passed;
  - schedule: one durable occurrence claim per schedule/instant under a valid
    single-owner/fencing regime;
  - external delivery: at-least-once unless the destination supports a stable
    idempotency key.
- [ ] State explicitly that exactly-once execution of arbitrary external side
  effects is not a runtime guarantee. It requires an idempotent receiver,
  transactional outbox, or a destination-specific protocol.

## 2. Make ownership and authority explicit

- [ ] Identify the authoritative state for each decision:
  - workflow actor journal for workflow lifecycle;
  - scheduler journal for recurring schedules and occurrences;
  - registry only as a rediscovery index;
  - external destination for delivery acknowledgement.
- [ ] Decide whether the registry remains a separately journaled index or is
  replaced by a discoverable projection. Do not silently treat two journals as
  one transaction.
- [ ] If the registry remains, add a reconciliation operation that compares
  registrations with authoritative actors and removes stale entries without
  deleting work that is merely unavailable.
- [ ] Give every registration and operation a typed identity. Avoid comparing
  or deduplicating by ad-hoc strings at call sites.
- [ ] Define the ownership/fencing rule for a schedule or workflow after node
  movement. A new owner must invalidate the old owner's ability to commit.

## 3. Close workflow lifecycle races

- [ ] Model the `start` acceptance boundary as a small pure decision:
  - new input → accept;
  - same input while running → join/already running;
  - different input → reject;
  - completed same input → return cached result;
  - cancelled input → reject or require a new identity.
- [ ] Journal the durable lifecycle boundary before launching work, and test
  crashes before and after each boundary.
- [ ] Define cancellation races precisely:
  - cancellation before start is durable;
  - cancellation during an activity;
  - completion racing cancellation;
  - cancellation during retry backoff;
  - caller cancellation versus workflow cancellation.
- [ ] Ensure every owned execution task has one structured owner and one
  terminal cleanup path. A caller disconnect must not accidentally cancel a
  durable workflow.
- [ ] Make terminal registration cleanup retryable or reconciled; an ignored
  cleanup failure must not become an undocumented permanent leak.

## 4. Give activities a real delivery contract

- [ ] Add a durable invocation identity containing workflow ID, run/attempt,
  activity name, and deterministic input identity.
- [ ] Separate activity retry policy from whole-workflow retry policy.
- [ ] Define what happens when a worker dies:
  - invocation remains pending;
  - invocation is retried;
  - late result is accepted only if its invocation identity is still current.
- [ ] Add activity timeouts with a journaled result, not only an in-memory task
  timeout.
- [ ] Require activities with external side effects to accept or derive an
  idempotency key. Document that retrying a non-idempotent activity can repeat
  the side effect.
- [ ] Keep retry/backoff decisions pure and interpret sleeps, dispatches, and
  persistence in the imperative shell.

## 5. Make timers and schedules safe under node changes

- [ ] Establish a single active owner for each timer/schedule occurrence.
- [ ] Add fencing or a durable lease/epoch so an old node cannot dispatch after
  ownership changes.
- [ ] Define missed-run policy:
  - skip;
  - fire once immediately;
  - catch up every missed occurrence;
  - bounded catch-up.
- [ ] Define overlap policy independently from retry policy. A retry of one
  occurrence must not accidentally create a second occurrence.
- [ ] Recover pending timers when a node leaves, not only when the same process
  restarts. Test both graceful shutdown and abrupt node loss.
- [ ] Ensure schedule cancellation races with a claimed/running occurrence are
  represented by an explicit outcome rather than inferred from task state.

## 6. Protect external effects with an outbox/inbox boundary

- [ ] Define the effect protocol for a destination:
  - `accepted` means the destination has durably accepted the effect;
  - `delivered` means it has acknowledged transport delivery;
  - `unknown` means the caller must retry using the same idempotency key.
- [ ] Add a durable outbox only where the domain needs crash-safe handoff from
  workflow completion to external delivery.
- [ ] Use destination-specific idempotency keys for Telegram messages,
  notifications, and future integrations.
- [ ] Keep transport retries out of workflow state transitions; the transport
  adapter reports typed outcomes back to the workflow/application shell.
- [ ] Test the ambiguous result window: request sent, response lost, process
  crashes, then recovery retries the same effect.

## 7. Define cluster and storage assumptions

- [ ] Document the required event-store guarantees: per-persistence-ID
  ordering, conflict detection, durable acknowledgement, and replay behavior.
- [ ] Verify snapshot and event-store writes cannot silently acknowledge data
  that was not persisted.
- [ ] Decide what consistency level the registry, singleton, virtual actors,
  and journal require during partitions.
- [ ] Add explicit behavior for unavailable stores and unavailable virtual
  actor nodes; distinguish transient unavailability from a terminal domain
  failure.
- [ ] Add a versioning policy for workflow code and encoded inputs before
  rolling upgrades or mixed-version clusters.

## 8. Build a failure matrix before implementation

- [ ] For each boundary, record: durable state before, crash point, state after
  restart, allowed duplicate effects, and recovery action.
- [ ] Cover at least:
  - registry append succeeds / object append fails;
  - object append succeeds / registry append fails;
  - task created / first lifecycle event not persisted;
  - activity request sent / result lost;
  - timer deadline reached / owner dies before claim;
  - occurrence claimed / process dies before dispatch;
  - dispatch succeeds / completion event fails;
  - cancellation races with completion;
  - two nodes attempt the same recovery.
- [ ] Turn each row into a deterministic reducer test where possible and a
  process-level fault test where timing or transport is involved.

## 9. Add fault-injection and observability

- [ ] Add injectable failure points around journal append, actor activation,
  worker dispatch, timer wake-up, registry updates, and external delivery.
- [ ] Add correlation IDs for workflow, run, activity invocation, occurrence,
  and external effect.
- [ ] Emit structured transitions rather than only free-form error messages.
- [ ] Provide inspection APIs for current owner, durable status, pending retry,
  last acknowledgement, and unresolved recovery entries.
- [ ] Test duplicate, delayed, reordered, and lost messages without relying on
  sleeps as the assertion mechanism.

## 10. Roll out in this order

- [ ] Publish the semantics and failure matrix.
- [ ] Fix activity identity, retry, timeout, and idempotency boundaries.
- [ ] Fix schedule ownership, fencing, and node-down recovery.
- [ ] Reconcile or redesign the workflow registry.
- [ ] Add outbox/inbox only for integrations that need crash-safe delivery.
- [ ] Add mixed-version and partition tests.
- [ ] Revisit stronger guarantees only after the failure tests demonstrate the
  current contract.

## Explicit non-goals for this follow-up

- No generic distributed transaction layer.
- No claim of exactly-once arbitrary side effects.
- No Raft/consensus implementation inside the workflow package without first
  defining the ownership invariant it would enforce.
- No application-specific Telegram or Norby policy in the durable runtime.
