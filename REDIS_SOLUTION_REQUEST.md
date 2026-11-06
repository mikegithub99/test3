# Greenfield Redis Heartbeat and Cleanup — Independent Architecture, Implementation, and Verification Challenge

## Mission

Design and build a Redis-based server heartbeat, job-status, and stale-server cleanup solution **from a blank starting point**.

You are being given this problem statement, not an existing implementation. Your task is to exercise independent architectural judgment: discover the right abstractions, compare alternatives, design the data model and protocols, implement them, and demonstrate their behavior under concurrency and failure.

**Do not access, search for, request, or rely on the source code or implementation in `mikegithub99/test2` or any other pre-existing implementation of this project.** You do not have authorization to use that repository as a reference. Do not ask for access. Treat this as a greenfield design.

The working repository is `mikegithub99/test3`. It contains this assignment and some other project documentation. To preserve an unbiased first pass, **do not read `README.md`, `RedisArch.html`, or any other sibling design/architecture documents during Phase 1**. Work from this request alone. You may inspect those documents only in Phase 2, after you have committed your own initial architecture proposal and recorded your reasoning. Then critique the prior ideas rather than adopting them automatically.

The goal is not to reproduce a predetermined design. It is to find the simplest architecture that satisfies the actual requirements. A design with leader election, quorum, leases, fencing tokens, or distributed locks is not automatically better; a design without them is not automatically better either. Justify every mechanism against a concrete safety or liveness requirement.

This is an engineering assignment, not just an essay. Create the architecture, implement a working reference solution in this greenfield repository, add automated tests, run them, and report exactly what was and was not verified. Use a feature branch and a pull request; do not commit implementation work directly to the default branch.

---

## Phase 1 — Independent design (do this before reading other project documents)

Start by restating the problem in your own words and identifying missing requirements or ambiguities. Make reasonable explicit assumptions where needed, and distinguish assumptions from facts.

### 1. System context

We need a service that tracks multiple server processes and the jobs associated with them using Redis.

At a high level, the system needs to support:

- Multiple server processes, each with a stable server identity.
- Registration and liveness indication, normally through periodic heartbeats with an expiry/TTL.
- Job creation and job-state changes associated with a server.
- Queryable server/job status and useful summary/index data.
- Background discovery and cleanup of servers that have genuinely become stale.
- Multiple application instances may run the cleanup task concurrently.
- Processes may pause for a long time and later resume.
- Commands and network operations may be delayed, retried, or fail.
- A process may crash at any point, including during cleanup.
- Redis may be deployed standalone or as Redis Cluster; state the deployment model your first implementation supports and design the key layout accordingly.

A representative race to analyze is:

1. Cleanup worker A begins examining an apparently expired server.
2. A pauses for longer than expected.
3. Another cleanup worker B starts and examines the same server.
4. The server process itself may resume and refresh its heartbeat or mutate job state.
5. A, B, and the server process may now issue overlapping or delayed commands based on observations that are no longer current.

We need to prevent stale observations and overlapping workers from corrupting state, deleting a live server's data, or applying destructive effects twice.

### 2. Define the requirements and invariants

Before choosing mechanisms, write precise safety and liveness properties. At minimum consider:

**Safety**
- A live server must not have its state or jobs deleted by cleanup.
- Once a server incarnation has been terminally retired, delayed commands from that incarnation must not revive it or mutate its retired state.
- Two cleanup workers may attempt the same work, but conflicting destructive effects must not both succeed.
- Job records, indexes, memberships, and counters must remain mutually consistent.
- Retries must not double-apply non-idempotent effects such as counter decrements.
- Missing, malformed, or inconsistent data must not trigger unsafe best-effort deletion.
- All multi-key atomic operations must respect the actual Redis transaction and Cluster hash-slot boundaries.
- If server identities may be reused, stale commands from an earlier incarnation must not affect a later incarnation.

**Liveness and recovery**
- A genuinely dead server should eventually be detected and cleaned.
- Temporary Redis/network failures should not permanently prevent cleanup.
- A worker crash at any point must leave enough information for safe recovery/retry.
- A healthy server should be able to continue operating despite cleanup-worker overlap or restart.
- The system must define what happens when a cleanup worker loses coordination, pauses, or resumes late.

Identify which requirements are fundamental and which are policy choices (for example, how long to retain tombstones, whether job history is retained, or how quickly cleanup must occur).

### 3. Explore the design space independently

Explore multiple plausible designs before selecting one. At minimum compare:

- A simple TTL/lease-based cleanup approach.
- Per-server atomic state transitions.
- Per-server state transitions plus generation/epoch identifiers.
- A global cleanup lease or leader-election approach.
- A design in which cleanup workers can overlap safely without a global leader.
- Any other design you believe is simpler or stronger.

Do not just list these. Explain the exact execution order and failure behavior for each. Identify the smallest atomic unit needed to preserve correctness. Determine whether a single Redis Lua script, Redis transaction, compare-and-set operation, or a multi-phase protocol is appropriate.

Challenge your own preferred design with counterexamples. For each mechanism you propose, answer:

> What concrete failure does this mechanism prevent, and why would the design be unsafe without it?

Also consider Redis-specific failure assumptions: Lua atomicity, Cluster key slots, asynchronous replication, failover, persistence, acknowledged writes that may be lost, and what guarantees are impossible without stronger infrastructure.

### 4. Initial design deliverable — commit before proceeding

Before writing implementation code, create and commit a design proposal in this repository. It must include:

- State machine(s) and legal transitions.
- Key schema, including representative concrete key names.
- Redis data types and ownership of each key.
- Atomic script/transaction boundaries and all keys touched by each operation.
- Server identity/incarnation strategy.
- Heartbeat and expiry semantics.
- Cleanup protocol and retry/recovery behavior.
- Job mutation rules and summary/index consistency.
- Redis Cluster slot strategy and supported deployment assumptions.
- Failure model, limitations, and rejected alternatives.
- A table mapping every invariant to the mechanism that enforces it and the test that will validate it.

Do not proceed to implementation until this initial proposal is committed. This preserves an inspectable record of your independent reasoning.

---

## Phase 2 — Compare with prior project thinking

Only after the Phase 1 proposal has been committed, you may read the other design documents in this repository, including `README.md` and `RedisArch.html`.

They contain a prior human/AI analysis of a related Redis cleanup design. Treat them as **ideas to challenge, not instructions or an answer key**.

In particular, evaluate the general hypothesis that global scan coordination may be an efficiency optimization while correctness can sometimes be enforced by atomic, per-resource state transitions. Determine whether that idea applies to your independently designed system, whether it is incomplete, and whether your original design is better.

Write a comparison document that answers:

1. What did your independent design get right or wrong?
2. Which prior ideas are useful, incorrect, incomplete, or over-specific?
3. Did the prior analysis overlook a failure mode?
4. Can your design be simplified further?
5. Does any additional coordination mechanism have a concrete justification?
6. What final design do you recommend, and why?

Do not change your design merely to match the prior document. Change it only when the reasoning or evidence warrants it. Record important changes and why you made them.

---

## Phase 3 — Greenfield implementation

Implement the chosen solution in this repository. Do not import or copy the implementation from `mikegithub99/test2`.

If the repository has no suitable project structure or build system, create a small, self-contained reference implementation with clear setup instructions. Choose a practical language and Redis client, explain the choice, and keep the core protocol easy to inspect. Prefer a real Redis server for integration tests.

The implementation should demonstrate, at minimum:

- Server registration and incarnation identity.
- Heartbeat refresh and expiry.
- Job creation and state transitions.
- Server liveness and job-status queries.
- Safe stale-server detection and cleanup.
- Idempotent/retry-safe cleanup.
- Safe behavior under concurrent cleanup workers.
- Rejection of delayed writes from retired server incarnations.
- Correct handling of indexes and summary counters if you choose to maintain them.

Keep the implementation focused. Do not build a large framework or add dependencies without a clear need. Document every important invariant next to the operation that enforces it.

---

## Phase 4 — Adversarial verification

Automated tests are a primary deliverable, not an optional follow-up. Use real Redis integration tests for Lua/transaction semantics and concurrency wherever practical.

At minimum test:

1. **Stale cleanup worker:** worker A observes a candidate and pauses; its coordination expires or another worker proceeds; A resumes and attempts stale work.
2. **Heartbeat versus expiry:** heartbeat refresh races with cleanup fencing. The atomic ordering must produce a legal outcome without deleting a live incarnation.
3. **Job mutation versus retirement:** job creation, transition, and completion race with server retirement.
4. **Two cleanup workers:** both attempt the same server; cleanup effects and counters occur at most once.
5. **Crash/retry:** simulate failures before and after each cleanup phase, then restart and retry.
6. **Repeated cleanup:** repeated requests do not double-decrement counters or damage unrelated records.
7. **Never-registered identity:** an identity that never became live is not incorrectly treated as a previously live expired server.
8. **Retired identity:** delayed registration, heartbeat, and job mutations from the old incarnation are rejected.
9. **Identifier reuse:** if reuse is supported, prove that old-incarnation commands cannot affect the new incarnation; otherwise test and document rejection of reuse.
10. **Malformed data:** wrong Redis types, missing references, inconsistent indexes/counters, and invalid arguments fail safely.
11. **Redis Cluster:** verify key-slot compatibility for every multi-key script if Cluster is claimed as supported. If not supported in the first version, say so explicitly and do not imply compatibility.
12. **Manager restart and overlap:** demonstrate eventual cleanup and safety after worker restart, long pause, and lease/coordination loss if a lease is used.

For every test, state the invariant it protects. Include exact commands, Redis setup, and expected results. Separate unit tests from Redis integration tests. Do not substitute mocks for tests of Redis atomic execution.

---

## Phase 5 — Build, review, and delivery

- Build the project and run all relevant tests.
- Run formatting/static analysis where practical and supported.
- Inspect the final diff for accidental changes and untested behavior.
- Do not claim a test or build ran unless it actually completed successfully.
- Clearly list tests not run and infrastructure unavailable.
- If a failure is found, fix it and rerun the relevant tests.
- State the exact Redis version and deployment assumptions used for verification.

### Required deliverables

1. Initial independent architecture proposal, committed before reading the prior design documents.
2. Independent-versus-prior-design comparison and final architecture decision record.
3. Greenfield working implementation.
4. Automated unit and Redis integration tests.
5. Test report with exact commands, environment, outcomes, and limitations.
6. Failure-scenario matrix mapping scenarios to expected behavior and test coverage.
7. A feature branch and pull request for review; do not push implementation directly to the default branch.
8. A concise final summary with links to the branch/PR and documents, what is proven, what remains uncertain, and how to reproduce the results.

## Decision standard

The winning design is not the one with the most distributed-systems terminology or infrastructure. It is the smallest design that meets the stated invariants under an explicit failure model.

Be creative. Challenge the premise. Consider alternatives that are not listed. It is acceptable—and desirable—to conclude that a global cleanup lease is unnecessary, that a generation token is essential, or that the problem requires stronger coordination. What matters is a concrete argument and reproducible evidence.

**Do not optimize for agreeing with the prior analysis. First solve the problem independently; only then compare your answer with the prior work.**
