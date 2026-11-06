# Redis Cleanup Architecture — Implementation and Verification Request

## Mission

Independently investigate, design, implement, and verify a correct and as-simple-as-possible Redis-based cleanup/heartbeat architecture for the server/job-status system in **`mikegithub99/test2`**.

Do not treat the design notes below as unquestionable requirements or assume the current implementation is correct. They are a handoff of prior architectural reasoning and source inspection. Verify every important claim against the current repository, identify anything that has changed, and improve the design where justified.

**The objective is correctness with the minimum necessary coordination machinery.** Do not add leader election, quorum, distributed locks, fencing-token infrastructure, or other mechanisms merely because they are conventional. Equally, do not omit a mechanism if the actual safety requirements cannot be met without it.

This is an engineering assignment, not just a request for an architecture essay. Explore the repository, state the invariants, propose the best design, implement it, run meaningful tests, and deliver a reviewable Git branch/pull request with evidence. If a critical environmental limitation prevents implementation or testing, complete as much as possible and report the limitation precisely rather than claiming success.

---

## 1. The problem to solve

The system maintains server registrations, heartbeats, jobs, and per-server/type summaries in Redis. A background cleanup manager scans for servers whose heartbeat has expired and removes their associated state and job records.

The critical concurrency scenario is:

1. Cleanup manager A acquires the cleanup scan lease.
2. A pauses (for example, due to process suspension, long scheduling delay, or a network stall) long enough for its lease to expire.
3. Cleanup manager B acquires the lease and begins scanning/cleaning.
4. A resumes and continues executing work based on its earlier scan.
5. A and B may now overlap, and a server may also be registering, refreshing its heartbeat, or changing job state.

The real question is not necessarily how to guarantee that only one cleanup manager is ever active. It is:

**How do we guarantee that stale or overlapping managers cannot successfully perform conflicting lifecycle or cleanup operations on the same server, while allowing harmless duplicate attempts and recoverable retries?**

The design should distinguish scan coordination (an efficiency mechanism) from per-resource correctness (the safety mechanism).

## 2. Repository and prior source-review handoff

The implementation to investigate is in **`mikegithub99/test2`**, primarily:

- `2-colocated/RedisHeartbeatManager.cc`
- `2-colocated/RedisHeartbeatManager.h`
- `2-colocated/RedisJobScripts.inc`
- `2-colocated/RedisJobStatus.cc`
- `2-colocated/HeartbeatRunner.cc`
- `2-colocated/HeartbeatRunner.h`

Inspect related code, tests, build configuration, examples, key naming helpers, Redis client behavior, and all call sites as needed. Do not limit the review to these files.

A previous source-level review (not a runtime verification) found the following. **Treat each item as a hypothesis to re-check against the current branch and exact Lua/C++ code:**

- `RedisHeartbeatManager` starts a background cleanup thread.
- Previously observed defaults were a 30-second heartbeat TTL, a 10-second heartbeat refresh interval, a 30-second cleanup lease TTL, a 5-second lease renewal interval, and a 5-second cleanup scan interval. Confirm current values and whether the relationships are safe.
- A cleanup lease used a unique owner token and Redis `SET ... NX PX`; renewal and release compared the stored token before extending or deleting the lease.
- The lease was not passed into the per-server cleanup scripts. Thus, lease expiry does not physically stop a paused former owner from resuming and issuing commands. Lease ownership alone must not be presented as the safety guarantee.
- The cleanup path used per-server Lua scripts referred to in source as `kFenceExpiredServerScript` and `kCleanupExpiredServerScript`.
- The fencing script appeared to atomically check heartbeat/state/registration information and transition an expired, previously live server from `ACTIVE` to `DELETING`.
- The cleanup script appeared to require `DELETING` and an absent heartbeat, validate relevant records/counters before mutation, perform cleanup atomically, and recognize already-completed cleanup.
- A terminal `DELETING` tombstone appeared to be retained to prevent an old server identity from becoming active again.
- Lifecycle scripts appeared to reject writes unless the server state was `ACTIVE`; heartbeat refresh and fencing appeared to operate atomically in the same Redis Cluster hash slot.
- The design appeared to rely on server IDs being unique and never reused, and on all lifecycle/destructive writes going through the guarded scripts.
- The previous review concluded that the current per-server Lua guards might make a separate leader-election/quorum subsystem unnecessary for this single-resource cleanup problem. That conclusion has **not** been established by Redis runtime tests, formal verification, or exhaustive failure injection.

Re-read the code and correct any inaccurate or outdated statements in this handoff.

## 3. Required reasoning: start with invariants, not mechanisms

Before changing code, define the system's safety and liveness requirements in precise, testable terms.

At minimum, analyze these properties:

### Safety

1. A server cannot successfully publish a heartbeat or perform an ordinary lifecycle/job mutation after its identity has irreversibly entered the terminal deleting state.
2. A cleanup operation cannot delete state belonging to a live server.
3. A stale cleanup manager cannot bypass the per-server state guard merely because it once held a scan lease.
4. Two cleanup managers may attempt the same server, but they cannot both successfully perform conflicting destructive transitions.
5. Job/type counters are not decremented twice, and cleanup does not silently corrupt indexes or summaries.
6. Partial, malformed, missing, or inconsistent Redis data cannot cause unsafe destructive mutation.
7. Redis Cluster routing and atomicity boundaries are correct for every multi-key Lua script.
8. If identities can ever be reused, stale commands from an earlier incarnation cannot affect the new incarnation. If the design depends on IDs never being reused, make that a documented and enforced invariant rather than an implicit assumption.

### Liveness and recovery

1. A genuinely expired server can eventually be cleaned.
2. A transient failure during cleanup can be retried safely.
3. A crash before, during, or after cleanup does not leave permanent unrepairable state.
4. A live server can continue normal operation when cleanup managers overlap or restart.
5. Cleanup lease loss, renewal delay, manager pause, and Redis command failures have clearly defined behavior.

Explain which properties are guaranteed by Redis Lua atomicity, which depend on Redis persistence/failover configuration, which depend on application-wide discipline, and which cannot be guaranteed by the current architecture.

## 4. Explicitly challenge the architecture

Compare at least these design choices:

- Per-server atomic lifecycle state machine with a terminal `DELETING` tombstone.
- Lease-only cleanup ownership.
- Lease plus a fencing/generation token checked by every affected operation.
- Leader election or quorum-based coordination.
- Per-resource compare-and-set / Lua transitions without global leader election.
- Any simpler or stronger alternative you discover.

Do not straw-man alternatives. Explain their costs, failure behavior, and the exact conditions under which each is warranted.

In particular, answer:

- Is the cleanup lease needed for correctness, or only to reduce duplicate scanning and work?
- Is it safe for a stale manager to continue after its lease expires? Under exactly what conditions?
- Are all operations that can race with cleanup guarded by the same authoritative lifecycle state?
- Can any code path write a heartbeat, create/change/delete a job, alter an index/counter, or reactivate a server without checking the lifecycle state atomically?
- Does a terminal tombstone actually prevent resurrection across every registration and cleanup path?
- Is the “all keys in one Redis Cluster hash slot” requirement satisfied by every script, including scripts that touch type-level summaries or registry structures?
- Does Redis failover, asynchronous replication, persistence loss, or acknowledged-write loss weaken the claimed guarantees? State the deployment assumptions explicitly.
- Would generation numbers or fencing tokens materially improve safety, or merely duplicate the existing per-server state transition?
- Is exactly-once behavior being claimed where only idempotent/effectively-once behavior is achievable?

The preferred direction is **not** predetermined. Keep the existing guarded state-machine approach if it is demonstrably sufficient; simplify it if possible; strengthen it if a concrete counterexample requires it.

## 5. Implementation expectations

Once you have completed the initial source review and identified the recommended design, proceed with implementation rather than stopping at a proposal.

- Preserve existing public APIs and compatibility unless a change is necessary; explain any breaking change.
- Keep state transitions explicit and monotonic wherever possible.
- Put correctness checks and mutations in the same atomic Redis operation when they must not race.
- Validate argument counts, key types, expected record relationships, and counters before performing destructive writes.
- Make cleanup retries safe and detect already-completed work without repeating counter changes.
- Make the behavior for malformed or inconsistent state explicit. Prefer fail-closed behavior for destructive operations, with useful diagnostics or a safe recovery path.
- Ensure every lifecycle operation that can conflict with deletion obeys the same authoritative guard.
- Avoid broad scans or global coordination mechanisms when a per-resource atomic operation is sufficient.
- Preserve Redis Cluster slot constraints and document the key/slot design.
- Do not silently change key schemas or data formats. If migration is required, design and test it.
- Add comments that explain invariants and non-obvious concurrency decisions, not merely restate the code.
- Follow the repository's existing C++ conventions and build/test structure.

If you conclude that a fencing token, generation number, stronger lease protocol, or leader-election mechanism is necessary, identify the concrete execution trace that defeats the simpler design and add a test reproducing it.

## 6. Required adversarial tests

Tests are a central deliverable. Add automated tests wherever the repository's infrastructure permits. Do not claim that source inspection alone proves concurrency correctness.

At minimum, cover:

1. **Stale manager:** A acquires the lease and pauses; lease expires; B acquires it and fences/cleans; A resumes and attempts its old work.
2. **Heartbeat/fence race:** heartbeat refresh and expiry fencing execute concurrently. Exactly one legal outcome must result from their atomic ordering.
3. **Job mutation/fence race:** job creation, transition, or completion races with the transition to `DELETING`.
4. **Duplicate cleanup:** two managers attempt to fence and clean the same server.
5. **Retry after partial workflow:** failures between fencing, reading job references, cleanup, and registry/index maintenance; retries must be safe.
6. **Repeated cleanup:** a second cleanup attempt must not decrement counters or delete unrelated data again.
7. **Never-live identity:** a server ID that was allocated but never successfully published its first heartbeat must not be incorrectly treated as an expired live server.
8. **Tombstone/resurrection:** attempts to register, heartbeat, or mutate a server after deletion must fail.
9. **Malformed data:** wrong Redis key types, missing referenced jobs, inconsistent membership/index counts, malformed script arguments, and invalid counters must fail safely.
10. **Cluster routing:** exercise scripts against Redis Cluster (or a faithful integration environment) and verify all keys used by each script share the required slot.
11. **Lease loss and renewal delay:** old owner continues running after lease expiry; verify that resource-level guards preserve safety.
12. **Restart/recovery:** restart cleanup manager at different points in the lifecycle and verify eventual completion without double effects.

Use real Redis integration tests for Lua semantics and concurrency where possible. Unit tests or mocks are useful but do not substitute for integration tests of scripts, Redis key types, atomic execution, and Cluster slot behavior. If Redis Cluster is unavailable, clearly label that coverage as untested and provide a reproducible test plan.

For every test, state the invariant it protects. Include repeatable commands and required Redis setup.

## 7. Verification and engineering quality

- Build the affected targets using the repository's documented toolchain.
- Run the relevant existing tests as well as new tests.
- Run static analysis, sanitizers, or formatting checks if already supported and practical.
- Inspect the final diff for accidental changes, API breakage, key-schema changes, and untested paths.
- Do not claim a build, test, sanitizer, or Redis Cluster run unless it actually completed successfully.
- Distinguish clearly between tests run, tests not run, and tests that require external infrastructure.
- If a test exposes a defect, fix it and rerun the relevant suite.
- If a property cannot be proved or tested, describe the remaining uncertainty honestly.

## 8. Deliverables

Produce all of the following:

1. **Architecture decision record** describing the chosen design, invariants, alternatives rejected, assumptions, Redis Cluster slot strategy, and failure model.
2. **Implementation** in `mikegithub99/test2`, with focused changes and appropriate tests.
3. **Test report** with exact commands, environment, results, and limitations.
4. **Failure-scenario table** showing each important scenario, expected behavior, and test coverage.
5. **Git branch and pull request** for review. Do not push unreviewed changes directly to the default branch.
6. A concise summary of what changed, why it is correct, what remains unverified, and how to reproduce the tests.

Put the architecture decision record and test report in the repository in sensible locations. Link them from the pull request description.

## 9. Decision standard

A successful result is not the most elaborate distributed architecture. It is the **simplest design that satisfies explicitly stated safety and liveness requirements under clearly stated Redis and application assumptions**.

Use this principle as the central test of every proposed mechanism:

> Coordinate scans for efficiency; enforce correctness at the resource.

And use this question to challenge every additional layer of coordination:

> What concrete failure does this mechanism prevent that the existing atomic per-resource state transition does not already prevent?

If there is no precise answer supported by an execution trace or test, do not add the mechanism.

Do not optimize for agreeing with the previous analysis. Independently verify it, find counterexamples if they exist, and deliver a better solution if the evidence supports one.
