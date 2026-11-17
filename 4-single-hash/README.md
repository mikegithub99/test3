# Single-hash-slot Redis Job Status and Heartbeat Services

This directory is the independent C++17 implementation area for the per-server-type Redis Cluster design. It preserves the public lifecycle/API concepts and operational guidance from `3-colocated`, while replacing the global `{registry}` data path with a durable manifest and per-type server-ID index.

## Files

- `RedisClient.h/.cc`: lazy Redis++ standalone/Cluster client.
- `RedisJobStatus.h/.cc`: registration, job lifecycle, counters, and read APIs.
- `RedisJobScripts.inc`: embedded Lua scripts used by status and heartbeat code.
- `RedisHeartbeatManager.h/.cc`: heartbeat publication and expired-server cleanup.
- `HeartbeatRunner.h/.cc`: background heartbeat refresh thread.
- `heartbeat.cc`: host-application startup example.
- `SINGLE_HASHSLOT.md`: detailed key layout, discovery, atomicity, snapshots, migration, and test design.

## Configuration and lifetime

The default `RedisJobStatus::Instance()` provider reads host registry values through `get_reg_val(const std::string&)`:

| Registry key | Purpose |
|---|---|
| `redisJobStatusEnabled` | Enable/disable status reporting; accepts true/false, 1/0, yes/no, case-insensitively; empty defaults enabled. |
| `redisJobStatusServerType` | Server type and Redis hash-tag value. |
| `redisJobStatusServerId` | Stable worker identity within that type. |
| `redisUrl` | Redis connection URL. |
| `isRedisCluster` | Selects RedisCluster when true. |

Singleton configuration is fixed on first construction. Initialize host configuration before calling either singleton. Tests may inject a provider on the first call; later providers are ignored. Disabled mode avoids Redis initialization, mutators are no-ops or return `Disabled`, and reads return empty collections.

## Worker startup and API

```cpp
const auto status = RedisJobStatus::Instance();
status->registerServer(typeMetadata, serverMetadata);
const auto manager = RedisHeartbeatManager::Instance();
manager->HeartbeatStart();

status->createPendingJob("job-100", jobMetadata);
status->transitionJob("job-100", RedisJobStatus::State::Active);
status->transitionJob("job-100", RedisJobStatus::State::Suspended);
status->transitionJob("job-100", RedisJobStatus::State::Active);
status->completeJob("job-100");

auto types = status->getServerTypes();
auto counters = status->getServerTypeSummary("orders");
auto servers = status->getServerSummaries("orders");
auto serverJobs = status->getServerJobs("orders", "worker-01");
auto typeJobs = status->getServerTypeJobs("orders");
```

Supported transitions are PENDING→ACTIVE, ACTIVE→SUSPENDED, and SUSPENDED→ACTIVE. Repeating the current state returns `NoOp`; transition to PENDING is invalid. Completion removes the job and both index references and decrements its status and total counters. Result values: Success=0, NoOp=1, JobNotFound=2, InvalidTransition=3, InconsistentData=4, JobAlreadyExists=5, Disabled=6, ServerNotActive=7.

## Redis key layout

Every key for a type uses the same hash tag. For type `orders`:

| Redis type | Key | Purpose |
|---|---|---|
| HASH | `servers:{orders}:manifest` | Durable type marker: type name, creation time, lifecycle, schema version. |
| HASH | `servers:{orders}:summary` | Type-wide aggregate counters and type metadata. |
| HASH | `servers:{orders}:state` | Server ID to ACTIVE/DELETING state. |
| SET | `servers:{orders}:ids` | Server IDs, retained through tombstone retention. |
| HASH | `servers:{orders}:server:<id>` | Per-server counters and metadata. |
| STRING + TTL | `servers:{orders}:heartbeat:<id>` | Heartbeat lease. |
| SET | `servers:{orders}:live` | IDs that published at least one heartbeat. |
| ZSET | `servers:{orders}:retired` | Fenced IDs and Redis-time retirement timestamps. |
| SET | `servers:{orders}:jobs` | Type-wide job-reference index. |
| SET | `servers:{orders}:server:<id>:jobs` | Per-server job-reference index. |
| HASH | `servers:{orders}:job:<jobRef>` | Job state, original ID, owner, and custom metadata. |

Job references use a deterministic length-prefixed form: `<serverId-length>:<serverId>:<applicationJobId>`. The original job ID remains in the job hash. Server IDs need only be unique within a type and must not be reused while a retirement tombstone remains.

## Atomicity and counter invariant

Registration must initialize the manifest, type summary, server hash, state, and ID membership in one per-type Lua script. Job creation, transition, completion, reassignment, and cleanup must update the job hash, both job indexes, server counters, and type counters in the same per-type slot.

For each counter field:

`typeSummary.counter = sum(server.counter for every server ID of that type)`

The type summary is authoritative. Validate key types, state, ownership, index membership, arguments, and signed-64-bit counter bounds before the first write. Redis Lua prevents command interleaving but is not a rollback transaction: a runtime error after a write does not undo earlier writes.

## Metadata

Metadata values are strings. Type metadata belongs in `meta:<field>` fields on the type summary; server metadata belongs in `meta:<field>` fields on the server hash. Job metadata is stored on the job hash. API-owned job fields are `jobId`, `status`, `serverType`, and `serverId`. Type `createdAt` and `createdByServerId`, and server `registeredAt`, are reserved and generated internally. Empty metadata field names are rejected. Updates replace supplied fields and leave unspecified fields unchanged.

## Heartbeat timing and cleanup

Defaults in `RedisHeartbeatManager::Config`:

| Setting | Default | Meaning |
|---|---:|---|
| Heartbeat TTL | 30 seconds | Worker liveness-key expiry. |
| Heartbeat refresh interval | 10 seconds | Background refresh cadence. |
| Cleanup scan interval | 5 seconds | Manifest/server discovery cadence. |
| First-heartbeat grace | 60 seconds | Allowed registration-to-first-heartbeat interval. |
| Tombstone retention | 24 hours | Minimum retention before server ID reuse. |

Heartbeat TTL expiry alone does not fence a worker. Cleanup first changes ACTIVE to DELETING in the per-type slot. Heartbeat refresh and fencing must inspect/update the same state so they serialize. Cleanup must validate job ownership and both indexes, reconcile type counters, remove job data and active metadata, and retain the tombstone until retention expires. For potentially large job indexes, use bounded idempotent cleanup batches; a single all-at-once script is only appropriate when job counts are bounded.

## Type discovery and Redis Cluster

There is no global registry set. The durable manifest represents type existence. Discovery searches for `servers:*:manifest`, parses exact `servers:{<type>}:manifest` names, and validates each manifest's `serverType` field.

Redis Cluster SCAN is node-local. A correct cluster-wide discovery adapter must enumerate all current primaries, scan each cursor to zero, deduplicate results, refresh topology, retry failed/replaced nodes, and repeat periodically. A SCAN routed to one primary is not cluster-wide. Verify that the deployed Redis++ Cluster API actually fans out to all primaries; if it does not, implement primary enumeration explicitly before using discovery for the UI or cleanup. SCAN is eventual and not a point-in-time snapshot.

## UI getters and consistency

The UI hierarchy remains type → servers → server jobs, with a type-wide jobs view. Prefer read-only per-type Lua snapshots when a screen needs mutually consistent values from several keys. Use bounded pagination; do not return unbounded server/job collections in one script.

One per-type Lua snapshot is coherent for the keys it reads at execution time. Several ordinary reads or pages are best-effort over time. Different type snapshots are not globally atomic. The UI should show freshness/snapshot timestamps where useful and must not reconstruct type counters by summing paginated server rows.

## Migration and rollout

The key layout differs from `3-colocated`; do not run old and new writers concurrently without a migration protocol. Coordinate writers, create/validate manifests and per-type ID indexes, verify counters and job indexes, switch readers/discovery and writers together, and define rollback/mixed-version behavior. Do not stop writing the old registry while old readers still depend on it.

## Build and validation

Requires C++17, Redis++, the host `get_reg_val` implementation, and the platform thread library. Add the implementation `.cc` files to the host build. Treat `heartbeat.cc` as an example entry point; do not link it when the host supplies `main()`.

Test registration retries and multi-server registration; same-slot KEYS for every Lua script; create/transition/complete across servers; counter invariant and int64 boundaries; wrong-type keys and malformed indexes; heartbeat/fence races; idempotent cleanup and tombstone retention; cluster-wide scan during concurrent creation, failover, and resharding; coherent per-type snapshots and pagination under mutation.

See [SINGLE_HASHSLOT.md](SINGLE_HASHSLOT.md) for the full architecture proposal and detailed consistency analysis.


## Administrative utilities and interactive browser

The `util/` directory contains:

- `RedisStatusCli.cc`: standalone SCAN-based keyspace inspector. Set `REDIS_URL`, optionally `REDIS_PATTERN` and `REDIS_SCAN_COUNT`. In Redis Cluster, run it against each primary endpoint when a complete cluster inventory is required; a node-local SCAN is not a cluster-wide inventory.
- `RedisJobStatusCleanup.cc`: dry-run key lister for the single-slot namespace. It recognizes per-type manifests, summaries, server hashes, ID/state indexes, heartbeats, job indexes, job hashes, live sets, and retired tombstones. It deletes only after `--delete` and interactive `DELETE` confirmation. Use a standalone Redis endpoint or run per primary; do not assume a single-node scan covers the whole cluster.

The `browser/` directory contains `redis_browser.py`, a Textual terminal UI. It discovers types by scanning `servers:*:manifest`, validates each manifest's `serverType`, and reads the per-type summary, ID set, server/state/heartbeat records, and server job indexes. It supports metadata/job inspection and administrative deletion. Type deletion cleans indexed servers and jobs, then marks the durable manifest `RETIRED`; it intentionally retains the manifest and server tombstones for discovery and ID-reuse safety.

Browser dependencies: Python 3.10+, `redis` (redis-py), `textual`, and `rich`. Configure `REDIS_URL`; set `REDIS_CLUSTER=1` to use redis-py's `RedisCluster` client. Run with:

```sh
python3 browser/redis_browser.py
```

Build the C++ utilities with C++17 and Redis++ using the same include/link configuration as the service. The cleanup utility is destructive only after both explicit command-line opt-in and interactive confirmation.
