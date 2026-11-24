/**
 * @file RedisHeartbeatScripts.inc
 * @brief Lua scripts used by Redis heartbeat and server-lifecycle management.
 *
 * Each script is passed to Redis EVAL with an explicit KEYS list and ARGV list.
 * Keep key order, argument order, return codes, and executable statements in
 * sync with the C++ callers. Redis scripts are atomic relative to other
 * commands, but runtime errors do not roll back writes already performed.
 */

// Initializes type/server metadata and activates one server in the {serverType} slot.
// KEYS: type summary, server summary, server-state hash.
// ARGV: server ID, caller timestamp, type metadata count/pairs, server metadata count/pairs.
// Returns 0 when the ID already has lifecycle state, 1 when activated; validation failures are Redis errors.
const char* const kRegisterAndActivateServerScript = R"lua(
local typeSummaryKey = KEYS[1]
local serverSummaryKey = KEYS[2]
local stateKey = KEYS[3]

local function validHashOrNone(key)
  local t = redis.call('TYPE', key).ok
  return t == 'none' or t == 'hash'
end

if not validHashOrNone(typeSummaryKey) or
   not validHashOrNone(serverSummaryKey) or
   not validHashOrNone(stateKey) then
  return redis.error_reply('wrong registration key type')
end

-- ARGV: serverId, type-createdAt, type metadata count/pairs,
--       server metadata count/pairs.
local typeCount = tonumber(ARGV[3])
if not typeCount or typeCount < 0 or typeCount ~= math.floor(typeCount) then
  return redis.error_reply('invalid server-type metadata count')
end
local serverCountIndex = 4 + typeCount * 2
if serverCountIndex > #ARGV then
  return redis.error_reply('truncated server-type metadata arguments')
end
local serverCount = tonumber(ARGV[serverCountIndex])
if not serverCount or serverCount < 0 or
   serverCount ~= math.floor(serverCount) or
   #ARGV ~= serverCountIndex + serverCount * 2 then
  return redis.error_reply('invalid server metadata arguments')
end

-- Do not write metadata or activate an identity that already has state,
-- including a DELETING retirement tombstone.
local serverId = ARGV[1]
if redis.call('HGET', stateKey, serverId) ~= false then
  return 0
end

-- Generate the activation timestamp before the first write. Redis TIME keeps
-- this timestamp in the same clock domain as the other lifecycle timestamps.
local now = redis.call('TIME')
local nowMs = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

redis.call('HSETNX', typeSummaryKey, 'meta:createdAt', ARGV[2])
redis.call('HSETNX', typeSummaryKey, 'meta:createdByServerId', serverId)

local cursor = 4
for i = 1, typeCount do
  redis.call('HSET', typeSummaryKey, 'meta:' .. ARGV[cursor], ARGV[cursor + 1])
  cursor = cursor + 2
end

cursor = serverCountIndex + 1
for i = 1, serverCount do
  redis.call('HSET', serverSummaryKey, 'meta:' .. ARGV[cursor], ARGV[cursor + 1])
  cursor = cursor + 2
end

redis.call('HSETNX', serverSummaryKey, 'meta:registeredAt', tostring(nowMs))
redis.call('HSET', stateKey, serverId, 'ACTIVE')
return 1
)lua";

// Adds a server type and server ID to the registry sets in the {registry} slot.
// KEYS: global server-type set, current type's server-ID set.
// ARGV: server type, server ID. Returns 0 when already registered, 1 when added.
// Key-type and key-layout violations are Redis errors; validation precedes both writes.
const char* const kRegisterServerScript = R"lua(
local serverTypesKey = KEYS[1]
local currentTypeKey = KEYS[2]

local function validtype(key)
  local t = redis.call('TYPE', key).ok
  return t == 'none' or t == 'set'
end

local serverType = ARGV[1]
local serverId = ARGV[2]

-- Both registry keys are supplied explicitly and share the {registry} hash
-- tag. The type-prefixed server ID is unique across types, so only the
-- current type's registry set needs a duplicate-membership check.
if not validtype(serverTypesKey) then
  return redis.error_reply('wrong server-types key type')
end
if currentTypeKey ~= 'servers:{registry}:type:' .. serverType then
  return redis.error_reply('current server registry key does not match server type')
end
if not validtype(currentTypeKey) then
  return redis.error_reply('wrong current server registry key type')
end
if redis.call('SISMEMBER', currentTypeKey, serverId) == 1 then
  return 0
end

redis.call('SADD', serverTypesKey, serverType)
redis.call('SADD', currentTypeKey, serverId)
return 1
)lua";



// Refreshes one server's heartbeat and live-set membership atomically.
// KEYS: heartbeat key, live-server set, lifecycle-state hash.
// ARGV: server ID, heartbeat TTL in seconds. Returns 1 on refresh, 0 if not ACTIVE.
const char* const kRefreshHeartbeatScript = R"lua(
local heartbeatKey = KEYS[1]
local liveKey = KEYS[2]
local stateKey = KEYS[3]
local heartbeatType = redis.call('TYPE', heartbeatKey).ok
local liveType = redis.call('TYPE', liveKey).ok
local stateType = redis.call('TYPE', stateKey).ok
if (heartbeatType ~= 'none' and heartbeatType ~= 'string') or
   (liveType ~= 'none' and liveType ~= 'set') or stateType ~= 'hash' then
  return redis.error_reply('wrong heartbeat key type')
end
if redis.call('HGET', stateKey, ARGV[1]) ~= 'ACTIVE' then return 0 end
redis.call('SET', heartbeatKey, ARGV[1], 'EX', ARGV[2])
redis.call('SADD', liveKey, ARGV[1])
return 1
)lua";


// Stale-server fencing.
// KEYS: heartbeat, state hash, server jobs, server summary, live set, retired index.
// ARGV: server ID, first-heartbeat grace period in milliseconds.
// Returns 0 if not eligible, 1 if fenced, 2 if fencing already completed.
// ---------------------------------------------------------------------------

const char* const kFenceExpiredServerScript = R"lua(
local heartbeat = KEYS[1]
local state = KEYS[2]
local serverJobs = KEYS[3]
local serverSummary = KEYS[4]
local live = KEYS[5]
local retired = KEYS[6]
local ht = redis.call('TYPE', heartbeat).ok
local st = redis.call('TYPE', state).ok
local jt = redis.call('TYPE', serverJobs).ok
local mt = redis.call('TYPE', serverSummary).ok
local lt = redis.call('TYPE', live).ok
local rt = redis.call('TYPE', retired).ok

if (ht ~= 'none' and ht ~= 'string') or
   (st ~= 'none' and st ~= 'hash') or
   (jt ~= 'none' and jt ~= 'set') or
   (mt ~= 'none' and mt ~= 'hash') or
   (lt ~= 'none' and lt ~= 'set') or
   (rt ~= 'none' and rt ~= 'zset') then
  return redis.error_reply('wrong type during heartbeat fencing')
end

if redis.call('EXISTS', heartbeat) == 1 then return 0 end

local current = redis.call('HGET', state, ARGV[1])
local function redisTimeMs()
  local now = redis.call('TIME')
  return tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
end

if current == 'DELETING' then
  -- Backfill older tombstones if the retired index did not exist yet.
  if not redis.call('ZSCORE', retired, ARGV[1]) then
    redis.call('ZADD', retired, tostring(redisTimeMs()), ARGV[1])
  end
  if jt == 'none' and mt == 'none' and
     redis.call('SISMEMBER', live, ARGV[1]) == 0 then return 2 end
  return 1
end
if current ~= 'ACTIVE' then return 0 end

-- A live-set member has published at least one heartbeat. If it has not,
-- allow startup only until the registration grace period expires. New
-- registrations store the Redis-generated timestamp in server metadata;
-- fall back to the old state-hash field for rolling upgrades.
local hasHeartbeated = redis.call('SISMEMBER', live, ARGV[1]) == 1
if not hasHeartbeated then
  local registeredAt = tonumber(redis.call(
      'HGET', serverSummary, 'meta:registeredAt') or
      redis.call('HGET', state, 'registeredAt:' .. ARGV[1]) or '')
  local timeoutMs = tonumber(ARGV[2])
  if not registeredAt or not timeoutMs or timeoutMs <= 0 then return 0 end
  if redisTimeMs() - registeredAt < timeoutMs then return 0 end
end
redis.call('HSET', state, ARGV[1], 'DELETING')
redis.call('ZADD', retired, tostring(redisTimeMs()), ARGV[1])
return 1
)lua";

// Removes a fenced server and its jobs.
// KEYS[1..7]: live set, state hash, heartbeat, server jobs, type jobs,
// server summary, type summary; KEYS[8..] are candidate job hashes.
// ARGV: server ID, job count, composite references, server type.
const char* const kCleanupExpiredServerScript = R"lua(
local live = KEYS[1]
local state = KEYS[2]
local heartbeat = KEYS[3]
local serverJobs = KEYS[4]
local typeJobs = KEYS[5]
local serverSummary = KEYS[6]
local typeSummary = KEYS[7]
local serverId = ARGV[1]
local count = tonumber(ARGV[2])
local serverType = ARGV[3 + (count or 0)]

local function keytype(key)
  return redis.call('TYPE', key).ok
end
local function valid(key, expected)
  local t = keytype(key)
  return t == 'none' or t == expected
end

if not valid(live, 'set') or keytype(state) ~= 'hash' or
   not valid(heartbeat, 'string') or not valid(serverJobs, 'set') or
   not valid(typeJobs, 'set') or not valid(serverSummary, 'hash') or
   not valid(typeSummary, 'hash') then
  return redis.error_reply('inconsistent cleanup key type')
end
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
if #refs ~= count then
  return redis.error_reply('server job index changed during cleanup preflight')
end
local supplied = {}
for i = 1, count do supplied[ARGV[2 + i]] = true end
for _, ref in ipairs(refs) do
  if not supplied[ref] then
    return redis.error_reply('server job reference missing from cleanup arguments')
  end
end

local pending, active, suspended, total = 0, 0, 0, 0
local jobKeys = {}
for i = 1, count do
  local ref = ARGV[2 + i]
  local jobKey = KEYS[7 + i]
  if keytype(jobKey) ~= 'hash' then
    return redis.error_reply('missing or wrong-type job hash')
  end
  local jobType = redis.call('HGET', jobKey, 'serverType')
  local owner = redis.call('HGET', jobKey, 'serverId')
  local status = redis.call('HGET', jobKey, 'status')
  if owner ~= serverId or jobType ~= serverType then
    return redis.error_reply('job ownership mismatch')
  end
  if redis.call('SISMEMBER', serverJobs, ref) ~= 1 or
     redis.call('SISMEMBER', typeJobs, ref) ~= 1 then
    return redis.error_reply('job index membership missing')
  end
  if status == 'PENDING' then pending = pending + 1
  elseif status == 'ACTIVE' then active = active + 1
  elseif status == 'SUSPENDED' then suspended = suspended + 1
  else return redis.error_reply('unsupported job status') end
  jobKeys[i] = jobKey
  total = total + 1
end

local function normalizeDigits(value)
  if not value or not string.match(value, '^%d+$') then return nil end
  local digits = string.gsub(value, '^0+', '')
  if digits == '' then return '0' end
  return digits
end
local maxCounter = '9223372036854775807'
local function enough(field, decrement)
  if decrement == 0 then return true end
  local raw = redis.call('HGET', typeSummary, field)
  local value = normalizeDigits(raw)
  if not value then return false end
  -- Match the exact signed 64-bit range accepted by Redis HINCRBY.
  if #value > #maxCounter or
     (#value == #maxCounter and value > maxCounter) then
    return false
  end
  -- Compare decimal strings; Lua numbers cannot exactly represent all int64s.
  local required = normalizeDigits(tostring(decrement))
  if not required then return false end
  if #required > #maxCounter or
     (#required == #maxCounter and required > maxCounter) then
    return false
  end
  if #value < #required then return false end
  if #value == #required and value < required then return false end
  return true
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
local t = redis.call('TYPE', key).ok
if t ~= 'none' and t ~= 'string' then
  return redis.error_reply('cleanup lease key has wrong type')
end
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
local st, rt = redis.call('TYPE', state).ok, redis.call('TYPE', retired).ok
if (st ~= 'none' and st ~= 'hash') or
   (rt ~= 'none' and rt ~= 'zset') then
  return redis.error_reply('wrong legacy tombstone index key type')
end

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
local t = redis.call('TYPE', retired).ok
if t ~= 'none' and t ~= 'zset' then
  return redis.error_reply('wrong retired index type')
end
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
local st, ht, jt, mt, lt, rt =
  keytype(state), keytype(heartbeat), keytype(serverJobs),
  keytype(serverSummary), keytype(live), keytype(retired)
if st ~= 'hash' or (ht ~= 'none' and ht ~= 'string') or
   (jt ~= 'none' and jt ~= 'set') or
   (mt ~= 'none' and mt ~= 'hash') or
   (lt ~= 'none' and lt ~= 'set') or rt ~= 'zset' then
  return redis.error_reply('inconsistent retired-server cleanup key type')
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
