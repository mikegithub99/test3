/**
 * @file RedisJobScripts.inc
 * @brief Redis Lua contracts shared by the job-status and heartbeat components.
 *
 * Each script is passed to Redis EVAL with an explicit KEYS list and ARGV list.
 * Keep key order, argument order, return codes, and executable statements in
 * sync with the C++ callers. Redis scripts are atomic relative to other
 * commands, but runtime errors do not roll back writes already performed.
 */



/**
 * Creates a PENDING job and updates its indexes and counters.
 * KEYS: job hash, type job set, server job set, type summary, server summary,
 * server-state hash. ARGV: job ID, type, server ID, composite reference,
 * metadata pair count, then metadata field/value pairs.
 * Returns 0 success, 5 duplicate job, 4 inconsistent data, or 7 inactive server.
 */
const char* const kCreatePendingScript = R"lua(

local RESULT_OK = 0
local RESULT_DATA_INCONSISTENT = 4
local RESULT_ALREADY_EXISTS = 5
local jobKey = KEYS[1]
local typeJobsKey = KEYS[2]
local serverJobsKey = KEYS[3]
local typeSummaryKey = KEYS[4]
local serverSummaryKey = KEYS[5]
local serverStateKey = KEYS[6]

if redis.call('HGET', state, serverId) ~= 'DELETING' or
   redis.call('EXISTS', heartbeat) ~= 0 then
  return 0
end

-- Another manager may have completed Phase A after this worker fenced/read
-- the same candidate. Recognize the committed shape without decrementing twice.
if keytype(serverJobs) == 'none' and keytype(serverSummary) == 'none' and
   redis.call('SISMEMBER', live, serverId) == 0 then
  return 2
end

local refs = redis.call('SMEMBERS', serverJobs)
local supplied = {}
for i = 1, count do supplied[ARGV[2 + i]] = true end
end

local pending, active, suspended, total = 0, 0, 0, 0
local jobKeys = {}
for i = 1, count do
  local ref = ARGV[2 + i]
  local jobKey = KEYS[7 + i]
    local jobType = redis.call('HGET', jobKey, 'serverType')
  local owner = redis.call('HGET', jobKey, 'serverId')
  local status = redis.call('HGET', jobKey, 'status')
  if owner ~= serverId or jobType ~= serverType then
    return redis.error_reply('job ownership mismatch')
  end
    if status == 'PENDING' then pending = pending + 1
  elseif status == 'ACTIVE' then active = active + 1
  elseif status == 'SUSPENDED' then suspended = suspended + 1
  else return redis.error_reply('unsupported job status') end
  jobKeys[i] = jobKey
  total = total + 1
end


-- All validations precede the first write. Every key is in this type's slot.
if pending > 0 then redis.call('HINCRBY', typeSummary, 'pending', -pending) end
if active > 0 then redis.call('HINCRBY', typeSummary, 'active', -active) end
if suspended > 0 then redis.call('HINCRBY', typeSummary, 'suspended', -suspended) end
if total > 0 then redis.call('HINCRBY', typeSummary, 'total', -total) end
for i = 1, count do
  redis.call('SREM', typeJobs, ARGV[2 + i])
  redis.call('DEL', jobKeys[i])
end
redis.call('DEL', serverJobs)
redis.call('DEL', serverSummary, heartbeat)
redis.call('SREM', live, serverId)
-- Retain DELETING until Phase C; IDs may be reused after tombstone collection.
return 1
)lua";


// Reads a best-effort job snapshot. All declared keys share {serverType}.
//
// KEYS[1] is the server- or type-wide job index; KEYS[2..] are job hashes
// corresponding to the references C++ enumerated before invoking this script.
// ARGV[1] is the number of references; ARGV[2..] are those references.
//
// Index changes are normal under concurrent job creation/completion. A
// reference no longer in the index, or whose hash has already disappeared, is
// omitted. Newly added references are naturally outside this candidate list.
// Reads server summaries: KEYS are summary hashes and ARGV are server IDs.
// Cleanup lease scripts use the registry slot.

const char* const kAcquireCleanupLeaseScript = R"lua(
local key = KEYS[1]
local result = redis.call('SET', key, ARGV[1], 'NX', 'PX', ARGV[2])
if result then return 1 end
return 0
)lua";

const char* const kRenewCleanupLeaseScript = R"lua(
local key = KEYS[1]
if redis.call('GET', key) ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', key, ARGV[2])
)lua";

const char* const kReleaseCleanupLeaseScript = R"lua(
local key = KEYS[1]
if redis.call('GET', key) ~= ARGV[1] then return 0 end
return redis.call('DEL', key)
)lua";


// Indexes state-hash tombstones in the retired sorted set. HSCAN is incremental;
// ZADD NX preserves the first recorded retirement timestamp.
const char* const kIndexUntrackedTombstonesScript = R"lua(
local state = KEYS[1]
local retired = KEYS[2]

-- Phase C may have removed the last state field, causing Redis to delete the
-- now-empty hash. An absent state hash has no legacy tombstones to backfill.
if st == 'none' then
  return '0'
end

local cursor = ARGV[1]
local result = redis.call('HSCAN', state, cursor, 'COUNT', 256)
local now = redis.call('TIME')
local nowMs = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local fields = result[2]
for i = 1, #fields, 2 do
  if fields[i + 1] == 'DELETING' then
    redis.call('ZADD', retired, 'NX', tostring(nowMs), fields[i])
  end
end
return result[1]
)lua";

// Lists tombstones whose retention period has elapsed, using Redis TIME.
const char* const kListExpiredServerTombstonesScript = R"lua(
local retired = KEYS[1]
local retentionMs = tonumber(ARGV[1])
local now = redis.call('TIME')
local nowMs = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
return redis.call('ZRANGEBYSCORE', retired, '-inf', tostring(nowMs - retentionMs))
)lua";

// Collects one expired tombstone in the per-type slot. The caller checks
// registry absence separately because registry and lifecycle keys use different slots.
const char* const kCollectRetiredServerTombstoneScript = R"lua(
local state = KEYS[1]
local heartbeat = KEYS[2]
local serverJobs = KEYS[3]
local serverSummary = KEYS[4]
local live = KEYS[5]
local retired = KEYS[6]
local serverId = ARGV[1]
local retentionMs = tonumber(ARGV[2])
local function keytype(key)
  return redis.call('TYPE', key).ok
end
if redis.call('HGET', state, serverId) ~= 'DELETING' then return 0 end
local retiredAt = redis.call('ZSCORE', retired, serverId)
if not retiredAt then return 0 end
local now = redis.call('TIME')
local nowMs = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if tonumber(retiredAt) > nowMs - retentionMs then return 0 end
if redis.call('EXISTS', heartbeat) ~= 0 or
   keytype(serverJobs) ~= 'none' or
   keytype(serverSummary) ~= 'none' or
   redis.call('SISMEMBER', live, serverId) ~= 0 then
  return 0
end
redis.call('HDEL', state, serverId, 'registeredAt:' .. serverId)
redis.call('ZREM', retired, serverId)
return 1
)lua";
