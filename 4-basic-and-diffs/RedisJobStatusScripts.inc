/**
 * @file RedisJobStatusScripts.inc
 * @brief Lua scripts used by RedisJobStatus.
 *
 * Each script is passed to Redis EVAL with an explicit KEYS list and ARGV list.
 * Keep key order, argument order, return codes, and executable statements in
 * sync with the C++ callers. Redis scripts are atomic relative to other
 * commands, but runtime errors do not roll back writes already performed.
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

local function keytype(key)
  return redis.call('TYPE', key).ok
end
local function validtype(key, expected)
  local t = keytype(key)
  return t == 'none' or t == expected
end

local existingJobKeyType = keytype(jobKey)
if existingJobKeyType == 'hash' then
  return RESULT_ALREADY_EXISTS
elseif existingJobKeyType ~= 'none' then
  -- A job key must be a HASH. A different existing type is corrupted data,
  -- not a duplicate job ID.
  return RESULT_DATA_INCONSISTENT
end
if not validtype(typeJobsKey, 'set') or
   not validtype(serverJobsKey, 'set') or
   not validtype(typeSummaryKey, 'hash') or
   not validtype(serverSummaryKey, 'hash') or
   keytype(serverStateKey) ~= 'hash' then
  return RESULT_DATA_INCONSISTENT
end
if redis.call('HGET', serverStateKey, ARGV[3]) ~= 'ACTIVE' then
  return 7 -- RESULT_SERVER_NOT_ACTIVE
end
-- A missing job hash with an existing index reference is inconsistent. Do not
-- let SADD silently become a no-op while counters are incremented.
local reference = ARGV[4]
if redis.call('SISMEMBER', typeJobsKey, reference) == 1 or
   redis.call('SISMEMBER', serverJobsKey, reference) == 1 then
  return RESULT_DATA_INCONSISTENT
end
-- Preflight every counter before the first mutation. HINCRBY then cannot fail
-- because of malformed, negative, or overflowing counter values.
local jobId = ARGV[1]
local serverType = ARGV[2]
local serverId = ARGV[3]
local jobFieldCount = tonumber(ARGV[5])
local fields = {'jobId', jobId, 'status', 'PENDING',
                'serverType', serverType, 'serverId', serverId}
local cursor = 6
for i = 1, jobFieldCount do
  table.insert(fields, ARGV[cursor])
  table.insert(fields, ARGV[cursor + 1])
  cursor = cursor + 2
end

redis.call('HSET', jobKey, unpack(fields))
redis.call('SADD', typeJobsKey, reference)
redis.call('SADD', serverJobsKey, reference)
redis.call('HINCRBY', typeSummaryKey, 'pending', 1)
redis.call('HINCRBY', typeSummaryKey, 'total', 1)
redis.call('HINCRBY', serverSummaryKey, 'pending', 1)
redis.call('HINCRBY', serverSummaryKey, 'total', 1)

return RESULT_OK
)lua";

/**
 * Transitions a job and updates its indexes and counters.
 * KEYS: job hash, type job set, server job set, type summary, server summary,
 * server-state hash. ARGV: job ID, type, server ID, target state, composite reference.
 * Returns 0 success, 1 no-op, 2 missing job, 3 invalid transition,
 * 4 inconsistent data, or 7 inactive server.
 */
const char* const kTransitionScript = R"lua(

local RESULT_OK = 0
local RESULT_NO_OP = 1
local RESULT_NOT_FOUND = 2
local RESULT_INVALID_TRANSITION = 3
local RESULT_DATA_INCONSISTENT = 4
local jobKey = KEYS[1]
local typeJobsKey = KEYS[2]
local serverJobsKey = KEYS[3]
local typeSummaryKey = KEYS[4]
local serverSummaryKey = KEYS[5]
local serverStateKey = KEYS[6]

local function keytype(key)
  return redis.call('TYPE', key).ok
end
local function validtype(key, expected)
  local t = keytype(key)
  return t == 'none' or t == expected
end

if keytype(serverStateKey) ~= 'hash' then
  return RESULT_DATA_INCONSISTENT
end
if redis.call('HGET', serverStateKey, ARGV[3]) ~= 'ACTIVE' then
  return 7 -- RESULT_SERVER_NOT_ACTIVE
end
if redis.call('EXISTS', jobKey) == 0 then
  return RESULT_NOT_FOUND
end
if not validtype(typeJobsKey, 'set') or
   not validtype(serverJobsKey, 'set') or
   not validtype(typeSummaryKey, 'hash') or
   not validtype(serverSummaryKey, 'hash') then
  return RESULT_DATA_INCONSISTENT
end

local old = redis.call('HGET', jobKey, 'status')
local storedType = redis.call('HGET', jobKey, 'serverType')
local storedServer = redis.call('HGET', jobKey, 'serverId')
local jobId = ARGV[1]
local serverType = ARGV[2]
local serverId = ARGV[3]
local newState = ARGV[4]
local reference = ARGV[5]
local metadataCount = tonumber(ARGV[6])
if storedType ~= serverType or storedServer ~= serverId or not old then
  return RESULT_DATA_INCONSISTENT
end
if redis.call('SISMEMBER', typeJobsKey, reference) ~= 1 or
   redis.call('SISMEMBER', serverJobsKey, reference) ~= 1 then
  return RESULT_DATA_INCONSISTENT
end

local function applyMetadata()
  local cursor = 7
  for i = 1, metadataCount do
    redis.call('HSET', jobKey, ARGV[cursor], ARGV[cursor + 1])
    cursor = cursor + 2
  end
end

if old == newState then
  applyMetadata()
  return RESULT_NO_OP
end

local oldCounter = string.lower(old)
local newCounter = string.lower(newState)
local allowed =
  (old == 'PENDING' and newState == 'ACTIVE') or
  (old == 'ACTIVE' and newState == 'SUSPENDED') or
  (old == 'SUSPENDED' and newState == 'ACTIVE')
if not allowed then
  return RESULT_INVALID_TRANSITION
end

redis.call('HSET', jobKey, 'status', newState)
applyMetadata()
redis.call('HINCRBY', typeSummaryKey, oldCounter, -1)
redis.call('HINCRBY', typeSummaryKey, newCounter, 1)
redis.call('HINCRBY', serverSummaryKey, oldCounter, -1)
redis.call('HINCRBY', serverSummaryKey, newCounter, 1)
return RESULT_OK
)lua";

/**
 * Completes a job and decrements its state and total counters.
 * KEYS: job hash, type job set, server job set, type summary, server summary,
 * server-state hash. ARGV: job ID, type, server ID, composite reference.
 * Returns 0 success, 2 missing job, 4 inconsistent data, or 7 inactive server.
 */
const char* const kCompleteScript = R"lua(

local RESULT_OK = 0
local RESULT_NOT_FOUND = 2
local RESULT_DATA_INCONSISTENT = 4
local jobKey = KEYS[1]
local typeJobsKey = KEYS[2]
local serverJobsKey = KEYS[3]
local typeSummaryKey = KEYS[4]
local serverSummaryKey = KEYS[5]
local serverStateKey = KEYS[6]

local function keytype(key)
  return redis.call('TYPE', key).ok
end
local function validtype(key, expected)
  local t = keytype(key)
  return t == 'none' or t == expected
end

if keytype(serverStateKey) ~= 'hash' then
  return RESULT_DATA_INCONSISTENT
end
if redis.call('HGET', serverStateKey, ARGV[3]) ~= 'ACTIVE' then
  return 7 -- RESULT_SERVER_NOT_ACTIVE
end
if redis.call('EXISTS', jobKey) == 0 then
  return RESULT_NOT_FOUND
end
if not validtype(typeJobsKey, 'set') or
   not validtype(serverJobsKey, 'set') or
   not validtype(typeSummaryKey, 'hash') or
   not validtype(serverSummaryKey, 'hash') then
  return RESULT_DATA_INCONSISTENT
end

local status = redis.call('HGET', jobKey, 'status')
local storedType = redis.call('HGET', jobKey, 'serverType')
local storedServer = redis.call('HGET', jobKey, 'serverId')
local jobId = ARGV[1]
local serverType = ARGV[2]
local serverId = ARGV[3]
local reference = ARGV[4]
if storedType ~= serverType or storedServer ~= serverId or not status then
  return RESULT_DATA_INCONSISTENT
end
if redis.call('SISMEMBER', typeJobsKey, reference) ~= 1 or
   redis.call('SISMEMBER', serverJobsKey, reference) ~= 1 then
  return RESULT_DATA_INCONSISTENT
end

local counter = string.lower(status)
if status ~= 'PENDING' and status ~= 'ACTIVE' and status ~= 'SUSPENDED' then
  return RESULT_DATA_INCONSISTENT
end
redis.call('HINCRBY', typeSummaryKey, counter, -1)
redis.call('HINCRBY', typeSummaryKey, 'total', -1)
redis.call('HINCRBY', serverSummaryKey, counter, -1)
redis.call('HINCRBY', serverSummaryKey, 'total', -1)
redis.call('SREM', typeJobsKey, reference)
redis.call('SREM', serverJobsKey, reference)
redis.call('DEL', jobKey)
return RESULT_OK
)lua";

