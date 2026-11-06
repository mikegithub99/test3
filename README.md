# Guarded coordination for distributed cleanup and maintenance

## Overview

This document describes a coordination pattern for distributed maintenance work where multiple coordinator processes may scan or attempt the same work, but correctness is enforced at the individual resource. It is based on the Redis heartbeat cleanup design and identifies when the same approach may be suitable for other systems.

The central principle is:

> **Coordinate scans for efficiency; enforce correctness at the resource.**

A lease can reduce duplicate work, but it should not be the only thing preventing corruption. The resource's authoritative state transition should be atomic, monotonic, and retry-safe, and every actor that can mutate the resource must obey the same rules.

This is a candidate pattern for certain distributed tasks—not a universal replacement for consensus, leader election, quorum, or fencing.

## Cleanup coordination and stale managers

### Key question

What happens if a cleanup manager acquires the Redis cleanup lease, pauses long enough for that lease to expire, and then resumes after another manager has acquired the lease? Could both managers continue cleanup and delete the same server's data?

### Analysis

The cleanup lease coordinates normal scan ownership; it is not the sole correctness mechanism. It is acquired with `SET NX PX`, renewed only when the stored owner token matches, and released only by its current owner. If a manager loses the lease, its next renewal attempt fails. A paused manager can nevertheless resume and execute code before it reaches that renewal check, so lease expiry alone does not stop an old process.

Correctness is protected at the per-server level by the Redis Lua lifecycle scripts:

- **Atomic fencing:** `kFenceExpiredServerScript` checks that the heartbeat is absent, that the server has published its first heartbeat, and that its state is `ACTIVE`. It then changes the state to `DELETING` atomically. Heartbeat refresh and this transition operate in the same per-type Redis Cluster slot, so Redis serializes the competing scripts. Whichever runs first determines whether the heartbeat is refreshed or the server is fenced.
- **No reactivation or new work:** Once the state is `DELETING`, heartbeat refresh and job creation/transition operations that require an `ACTIVE` server are rejected. Registration also does not reactivate a terminal server ID.
- **Atomic, repeat-safe cleanup:** `kCleanupExpiredServerScript` rechecks the `DELETING` state and absence of the heartbeat before making changes. Its validation and deletions/counter updates execute in one Lua script. If another manager has already completed Phase A, the script recognizes the completed shape and returns the already-cleaned result rather than decrementing counters again.
- **Recoverable registry cleanup:** The manager removes the ID from the registry after Phase A reports completion. The retained `DELETING` tombstone makes a cleanup interrupted between phases discoverable and prevents accidental reuse of that server ID.

Therefore, two managers may briefly overlap after a lease expires, but their per-server operations are serialized and guarded by lifecycle state. For the operations currently implemented, a separate leader-election protocol, quorum, or cleanup fencing-token subsystem is not required. The lease reduces unnecessary concurrent scans; the per-server Lua scripts protect data correctness.

### Assumptions and limits

This safety argument depends on all lifecycle mutations and destructive cleanup remaining behind the guarded Lua scripts, on the scripts continuing to operate on the appropriate per-type Redis slot, and on server IDs being unique and never reused. Any future destructive operation performed outside those checks must be reviewed for stale-manager safety. The lease token is not passed to the per-server scripts, so it should not be treated as a hard execution fence.

This is a source-level analysis, not a demonstrated failure-injection result. Before deployment, test lease expiry while a manager is paused, takeover by a second manager, concurrent fencing/heartbeat refresh, overlapping Phase A cleanup, and recovery between Phase A and Phase B.

## When to consider this coordination pattern for other problems

The cleanup design suggests a reusable alternative to adding a general-purpose leader-election or quorum protocol: allow coordinators to overlap, but make each entity's state transition and destructive operation safe through a small set of atomic, guarded, repeat-safe operations. This is a candidate pattern for certain distributed maintenance tasks—not a universal replacement for consensus, leases, or fencing.

### Conditions that should be met

A similar problem is a good candidate only when the following conditions can be demonstrated:

1. **There is a clear unit of ownership.** Work can be partitioned by a stable entity or resource key (for example, one server, task, partition, or session). Correctness can be decided independently for each unit, rather than requiring one atomic decision across many unrelated units.
2. **The authoritative state is shared and atomically mutable.** All contenders can consult and change the entity's authoritative state through a storage primitive that serializes the relevant operation (for example, a Redis Lua script operating in one Cluster slot, or an equivalent transactional/conditional-write mechanism). A read followed by an unguarded write is not sufficient.
3. **The state machine is explicit and monotonic for the operation.** There is a well-defined transition such as `ACTIVE -> DELETING -> DELETED`, and stale actors cannot move the entity backwards or reactivate it. Terminal markers or tombstones can be retained for as long as needed to prevent ambiguity or identifier reuse.
4. **Liveness can be distinguished from mere absence.** If eligibility depends on an owner being dead or expired, the system has a reliable-enough liveness signal (heartbeat, expiry, lease, or equivalent), and the atomic transition rechecks that signal at the point where it fences the entity. Missing registration or missing heartbeat must not accidentally be interpreted as proof of prior activity or safe deletion.
5. **Every competing writer obeys the same guard.** Creation, renewal, state changes, completion, and destructive operations that could conflict must all check the authoritative state in the same atomic boundary. It is not enough for the cleanup path alone to check it. A stale process must not be able to bypass the guard with a direct write or an external side effect.
6. **The destructive operation is idempotent or made effectively once by atomic state.** Repeating it must not double-decrement counters, duplicate charges, emit duplicate irreversible actions, or corrupt indexes. Related data changes and their validation should be in the same transaction/script where atomicity is required.
7. **Partial progress is recoverable.** If work spans phases or stores, each phase has a durable completion condition, retries can determine what remains, and the order of phases cannot expose an invalid state. If a single atomic operation cannot cover the whole workflow, document the invariants at each boundary.
8. **The consistency boundary is understood.** All invariants that must hold together fit within the transaction/script/partition boundary being used. If data spans Redis Cluster slots or independent databases, do not assume a global atomic snapshot or transaction; use explicit reconciliation, an outbox/saga, or a stronger coordination design as appropriate.
9. **Identity and retention rules are explicit.** Entity identifiers are not reused while stale actors, delayed messages, retries, or tombstones could still refer to them. If reuse is required, introduce a generation/epoch and ensure every mutation validates it.
10. **Failure behavior is safe by default.** Malformed or inconsistent state should stop the destructive operation and surface an error for investigation, rather than trigger best-effort deletion. The system should have observable outcomes for acquired, lost, fenced, completed, already-completed, and rejected work.

### Practical review checklist

Before adopting the pattern, answer **yes** to each of these questions:

- Can two coordinators run the same scan without causing incorrect results?
- At the final mutation point, does one atomic operation verify eligibility and perform the state transition?
- Can a stale coordinator resume after a long pause and still be prevented from changing an entity that has already been fenced?
- Do all ordinary writers and lifecycle operations reject the terminal state?
- Can cleanup be retried after a crash at every step without repeating non-idempotent effects?
- Are counters, indexes, ownership records, and primary data updated atomically where their invariants require it?
- Are cross-partition or cross-store limitations explicitly handled rather than assumed away?
- Are entity IDs never reused, or is a generation/epoch checked by every relevant operation?
- Have pause, timeout, takeover, duplicate execution, malformed data, and crash-recovery cases been tested?

A **no** or **unknown** answer is a design gap to resolve, not a reason to assume the pattern is safe.

### When this pattern is not enough

Do not use this approach as the sole coordination mechanism when correctness requires a globally agreed ordering or decision across multiple independent authorities; when a mutation spans resources that cannot share an atomic boundary and partial outcomes are unsafe; when stale actors can perform irreversible external actions that cannot be guarded or deduplicated; when entity identifiers can be reused without generation checks; or when the system needs quorum-based availability or consensus under network partitions. Those cases may require consensus/leader election, fencing epochs enforced by every downstream resource, transactional messaging, sagas with compensation, or another protocol designed for the actual failure model.

### Core principle

**Coordinate scans for efficiency; enforce correctness at the resource.** A lease can reduce duplicate work, but it should not be the only thing preventing corruption. Make the resource's state transition authoritative, atomic, monotonic, and retry-safe, and ensure every actor that can mutate that resource obeys the same rules. The current heartbeat cleanup implementation is one concrete instance of this pattern; each new use still requires a fresh invariant and failure-mode analysis.
