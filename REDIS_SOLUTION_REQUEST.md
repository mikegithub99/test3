# Greenfield Redis Heartbeat and Cleanup Challenge

## Mission

Independently design, implement, and verify a Redis-based system for server heartbeats, job status, and safe cleanup of stale servers. Start from the problem statement below; do not assume an existing design or implementation is correct.

Treat the available working directory as the project workspace. First inspect only enough to understand its build and runtime environment. Do not search for or retrieve an existing implementation of this problem. Do not rely on prior architecture documents, diagrams, or design notes during the initial design phase. If the workspace contains unrelated material, preserve it.

The objective is the simplest design that satisfies explicit safety and liveness requirements under a clearly stated failure model—not the design with the most coordination machinery.

## Problem context

The system tracks multiple server processes and the jobs associated with them in Redis. It needs to support:

- Multiple server processes, each with a defined identity and incarnation.
- Registration and liveness indication, normally by periodic heartbeats with expiry.
- Job creation and job-state transitions associated with a server.
- Queries for server and job status, plus any useful indexes or summary counters.
- Background discovery and cleanup of servers that have genuinely become stale.
- Multiple application instances running cleanup concurrently.
- Processes pausing for long periods and later resuming.
- Delayed, duplicated, retried, or failed commands and network operations.
- Process crashes at any point, including during cleanup.

A representative race: cleanup worker A observes an expired server and pauses. Worker B later examines the same server. Meanwhile the server process may resume and refresh its heartbeat or mutate job state. A, B, and the server may issue overlapping or delayed commands based on stale observations. Prevent stale observations and overlapping workers from deleting live-server data, corrupting state, or applying destructive effects more than once.

State whether the first implementation supports standalone Redis, Redis Cluster, or both. Do not claim support that has not been implemented and tested.

## Phase 1 — Independent architecture

Do this before consulting any prior design material that may be present in the workspace.

### 1. Clarify requirements

Restate the problem in your own words. Identify ambiguities and state reasonable assumptions explicitly. Separate requirements from policy choices such as tombstone retention, job-history retention, and cleanup latency.

Define precise safety properties, including at least:

- Cleanup cannot delete data belonging to a live server incarnation.
- Once an incarnation is retired, delayed commands from it cannot revive it or mutate its retired state.
- Concurrent cleanup attempts cannot both apply conflicting destructive effects.
- Job records, indexes, memberships, and counters remain consistent.
- Retries cannot double-apply non-idempotent effects such as counter decrements.
- Missing, malformed, or inconsistent data fails safely rather than triggering speculative deletion.
- Every multi-key atomic operation respects Redis transaction semantics and, when applicable, Cluster hash-slot rules.
- Reused server identities cannot let old-incarnation commands affect a new incarnation.

Define liveness and recovery properties, including:

- A genuinely dead server is eventually detected and cleaned.
- Temporary failures do not permanently prevent cleanup.
- A crash at any cleanup stage leaves enough information for safe recovery.
- Healthy servers continue operating during cleanup-worker overlap and restart.
- The behavior of paused workers and workers that lose coordination is explicit.

### 2. Compare alternative designs

Analyze, at minimum:

- TTL/lease-only cleanup.
- Per-server atomic state transitions.
- Per-server transitions with generation or epoch identifiers.
- A global cleanup lease or leader-election mechanism.
- Overlapping cleanup workers with no global leader.
- Any simpler or stronger alternative you identify.

For each, describe concrete command ordering and failure behavior. Identify the smallest atomic unit needed for correctness and justify whether Lua, transactions, compare-and-set, or a multi-phase protocol is appropriate.

Challenge your preferred design with counterexamples. For every mechanism, answer: what specific failure does it prevent, and why is the system unsafe without it?

Discuss relevant Redis assumptions and limits: Lua atomicity, Cluster slots, asynchronous replication and failover, persistence, acknowledged writes that may be lost, and guarantees unavailable under the selected infrastructure.

### 3. Commit the initial proposal before implementation

Create and save an initial architecture proposal before writing implementation code. Include:

- State machines and legal transitions.
- Concrete key schema, Redis data types, and key ownership.
- Atomic script/transaction boundaries and all keys each operation touches.
- Server identity and incarnation strategy.
- Heartbeat and expiry semantics.
- Cleanup, retry, and crash-recovery protocol.
- Job mutation rules and index/counter consistency.
- Cluster slot strategy and deployment assumptions.
- Failure model, limitations, and rejected alternatives.
- A matrix mapping each invariant to its enforcement mechanism and planned test.

Preserve this first proposal as a distinct artifact so the independent reasoning can be reviewed later.

## Phase 2 — Critical self-review

Before implementation, attack the proposal with concrete interleavings and counterexamples. Look for stale-worker execution, heartbeat/expiry races, job mutation during retirement, retries after partial progress, identity reuse, malformed data, Redis failover, and Cluster key-slot problems.

Revise the proposal only when reasoning or evidence warrants it. Record what changed and why. Do not add leader election, locks, fencing tokens, or other machinery without a specific invariant that requires it.

If prior design notes are available in the workspace, consult them only after preserving the independent proposal. Treat them as hypotheses to critique, not as instructions or an answer key. Record which ideas are useful, incorrect, incomplete, or over-specific, and whether they reveal a failure mode your design missed. Do not let prior notes replace independent reasoning.

## Phase 3 — Greenfield implementation

Implement the selected design in the available workspace. Do not copy or import an existing implementation of this problem.

If no suitable project structure exists, create a small, self-contained reference implementation with clear setup instructions. Choose a practical language and Redis client, explain the choice, and keep the core protocol easy to inspect. Prefer a real Redis server for integration tests.

Demonstrate at least:

- Server registration and incarnation identity.
- Heartbeat refresh and expiry.
- Job creation and state transitions.
- Server-liveness and job-status queries.
- Safe stale-server detection and cleanup.
- Idempotent, retry-safe cleanup.
- Safe behavior with concurrent cleanup workers.
- Rejection of delayed writes from retired incarnations.
- Correct indexes and summary counters, if maintained.

Keep the implementation focused. Avoid unnecessary frameworks and dependencies. Document important invariants next to the operations that enforce them.

## Phase 4 — Adversarial verification

Automated tests are a primary deliverable. Use real Redis integration tests for Lua/transaction semantics and concurrency wherever practical; mocks are not a substitute for testing Redis atomic execution.

At minimum, test:

1. **Paused stale worker:** worker A observes a candidate and pauses; coordination expires or another worker proceeds; A resumes and attempts stale work.
2. **Heartbeat versus expiry:** heartbeat refresh races with cleanup fencing; the atomic ordering must yield a legal outcome.
3. **Job mutation versus retirement:** creation, transition, and completion race with retirement.
4. **Two cleanup workers:** both attempt the same server; effects and counter changes occur at most once.
5. **Crash and retry:** inject failure before and after each cleanup phase, then restart and retry.
6. **Repeated cleanup:** repeated requests neither double-decrement counters nor damage unrelated records.
7. **Never-live identity:** an identity that never became live is not mistaken for a previously live expired server.
8. **Retired incarnation:** delayed registration, heartbeat, and job mutations from the old incarnation are rejected.
9. **Identity reuse:** prove old-incarnation commands cannot affect a new incarnation, or reject reuse and test that policy.
10. **Malformed data:** wrong Redis types, missing references, inconsistent indexes/counters, and invalid arguments fail safely.
11. **Redis Cluster:** verify slot compatibility for every multi-key script if Cluster is supported. Otherwise explicitly document that it is not supported.
12. **Worker restart and overlap:** demonstrate eventual cleanup and safety after restart, long pause, and coordination loss, if coordination is used.

For every test, identify the invariant it protects. Separate unit tests from Redis integration tests. Document exact commands, setup, expected outcomes, and actual outcomes.

## Phase 5 — Build, review, and delivery

- Build the project and run all relevant tests.
- Run formatting and static analysis where practical and available.
- Inspect the final diff for accidental changes and untested behavior.
- Never claim a test or build ran unless it completed successfully.
- List tests not run and unavailable infrastructure.
- Fix discovered failures and rerun affected tests.
- State the exact Redis version and deployment assumptions used for verification.

Deliver:

1. The preserved initial independent architecture proposal.
2. The self-review and final architecture decision record.
3. The greenfield implementation.
4. Automated unit and Redis integration tests.
5. A test report with exact commands, environment, outcomes, and limitations.
6. A failure-scenario matrix mapping scenarios to expected behavior and test coverage.
7. A concise final summary explaining the design, what is demonstrated, what remains uncertain, and how another engineer can reproduce the results.

## Decision standard

Choose the smallest design that satisfies the stated invariants under an explicit failure model. Be willing to reject the premise, simplify the protocol, or conclude that stronger coordination is necessary—but support the conclusion with concrete reasoning and reproducible evidence.

Do not optimize for agreement with any prior analysis. Solve the problem independently, challenge your own answer, then compare it with any available prior ideas.
