/**
 * @file RedisHeartbeatManager.cc
 * @brief Per-type manifest discovery and heartbeat-expiry cleanup.
 */
#include "RedisHeartbeatManager.h"
#include "HeartbeatRunner.h"
#include "RedisJobStatus.h"
#include "RedisJobScripts.inc"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using std::int64_t;
using std::string;
using std::vector;

namespace {
string typeKey(const string& type, const string& suffix) {
  return "servers:{" + type + "}:" + suffix;
}
string serverKey(const string& type, const string& id, const string& suffix) {
  return typeKey(type, "server:" + id + suffix);
}
template<class Redis>
int64_t eval(Redis& redis, const char* script,
             const vector<string>& keys, const vector<string>& args) {
  return redis.template eval<int64_t>(script, keys.begin(), keys.end(),
                                      args.begin(), args.end());
}
}

RedisHeartbeatManager::RedisHeartbeatManager(ConfigProvider provider) {
  const auto status = RedisJobStatus::Instance();
  if (!status->isEnabled()) return;
  m_config = provider ? provider() : Config{};
  if (m_config.heartbeatTtlSeconds <= 0 ||
      m_config.heartbeatRefreshIntervalSeconds <= 0 ||
      m_config.cleanupScanIntervalSeconds <= 0 ||
      m_config.firstHeartbeatTimeoutSeconds <= 0 ||
      m_config.retiredTombstoneRetentionSeconds <= 0)
    throw std::invalid_argument("RedisHeartbeatManager: durations must be positive");
  m_client = RedisClient::Instance();
  m_enabled = true;
  m_cleanupThread = std::thread(&RedisHeartbeatManager::cleanupLoop, this);
}
RedisHeartbeatManager::~RedisHeartbeatManager() { stop(); }
std::shared_ptr<RedisHeartbeatManager> RedisHeartbeatManager::Instance(ConfigProvider provider) {
  static const std::shared_ptr<RedisHeartbeatManager> instance(
      new RedisHeartbeatManager(std::move(provider)));
  return instance;
}
void RedisHeartbeatManager::HeartbeatStart() {
  const auto status = RedisJobStatus::Instance();
  if (!m_enabled || !status->isEnabled()) return;
  std::lock_guard<std::mutex> lock(m_heartbeatMutex);
  if (!m_heartbeatRunner)
    m_heartbeatRunner.reset(new HeartbeatRunner(
        status, m_config.heartbeatTtlSeconds,
        m_config.heartbeatRefreshIntervalSeconds));
}
void RedisHeartbeatManager::stop() {
  { std::lock_guard<std::mutex> lock(m_mutex); m_stopping = true; }
  m_wakeup.notify_all();
  if (m_cleanupThread.joinable()) m_cleanupThread.join();
}
void RedisHeartbeatManager::cleanupLoop() {
  std::unique_lock<std::mutex> lock(m_mutex);
  while (!m_stopping) {
    lock.unlock();
    try { scanOnce(); }
    catch (const std::exception& e) {
      std::cerr << "RedisHeartbeatManager: cleanup scan failed: " << e.what() << '\n';
    } catch (...) {
      std::cerr << "RedisHeartbeatManager: cleanup scan failed: unknown exception\n";
    }
    lock.lock();
    m_wakeup.wait_for(lock, std::chrono::seconds(m_config.cleanupScanIntervalSeconds),
                      [this] { return m_stopping; });
  }
}
void RedisHeartbeatManager::scanOnce() {
  if (!m_enabled) return;
  m_client->execute([&](auto& redis) {
    // Redis SCAN is node-local. In Cluster mode this call is valid only when
    // the Redis++ adapter fans the scan out across all primaries. Deployments
    // that do not provide that behavior must add primary enumeration here.
    vector<string> manifestKeys;
    long long cursor = 0;
    do { redis.scan(cursor, std::back_inserter(manifestKeys), "servers:*:manifest", 500); }
    while (cursor != 0);

    const string prefix = "servers:{", suffix = "}:manifest";
    vector<string> types;
    for (const auto& name : manifestKeys) {
      if (name.rfind(prefix, 0) != 0 || name.size() <= prefix.size()+suffix.size() ||
          name.compare(name.size()-suffix.size(), suffix.size(), suffix) != 0) continue;
      const string type = name.substr(prefix.size(), name.size()-prefix.size()-suffix.size());
      if (redis.hget(typeKey(type, "manifest"), "serverType").value_or("") == type)
        types.push_back(type);
    }
    std::sort(types.begin(), types.end());
    types.erase(std::unique(types.begin(), types.end()), types.end());

    for (const auto& type : types) {
      vector<string> ids;
      redis.smembers(typeKey(type, "ids"), std::back_inserter(ids));
      for (const auto& id : ids) {
        const vector<string> fenceKeys = {
          typeKey(type, "heartbeat:" + id), typeKey(type, "state"),
          serverKey(type, id, ":jobs"), serverKey(type, id, ""),
          typeKey(type, "live"), typeKey(type, "retired")};
        const vector<string> fenceArgs = {
          id, std::to_string(m_config.firstHeartbeatTimeoutSeconds * 1000)};
        const auto fenced = eval(redis, kFenceExpiredServerScript, fenceKeys, fenceArgs);
        if (fenced == 0 || (fenced != 1 && fenced != 2)) continue;

        const string serverJobs = serverKey(type, id, ":jobs");
        vector<string> refs;
        redis.smembers(serverJobs, std::back_inserter(refs));
        vector<string> keys = {
          typeKey(type, "live"), typeKey(type, "state"),
          typeKey(type, "heartbeat:" + id), serverJobs,
          typeKey(type, "jobs"), serverKey(type, id, ""),
          typeKey(type, "summary")};
        for (const auto& ref : refs)
          keys.push_back(typeKey(type, "job:" + ref));
        vector<string> args = {id, std::to_string(refs.size())};
        args.insert(args.end(), refs.begin(), refs.end());
        args.push_back(type);
        eval(redis, kCleanupExpiredServerScript, keys, args);
      }

      // Backfill tombstones from state; the script is idempotent and HSCAN-bounded.
      const vector<string> indexKeys = {typeKey(type, "state"), typeKey(type, "retired")};
      string hcursor = "0";
      do {
        const vector<string> args = {hcursor};
        hcursor = redis.template eval<string>(
            kIndexUntrackedTombstonesScript, indexKeys.begin(), indexKeys.end(),
            args.begin(), args.end());
      } while (hcursor != "0");

      const vector<string> retiredKeys = {typeKey(type, "retired")};
      const vector<string> retentionArgs = {
        std::to_string(m_config.retiredTombstoneRetentionSeconds * 1000)};
      const auto expired = redis.template eval<vector<string>>(
          kListExpiredServerTombstonesScript, retiredKeys.begin(), retiredKeys.end(),
          retentionArgs.begin(), retentionArgs.end());
      for (const auto& id : expired) {
        const vector<string> collectKeys = {
          typeKey(type, "state"), typeKey(type, "heartbeat:" + id),
          serverKey(type, id, ":jobs"), serverKey(type, id, ""),
          typeKey(type, "live"), typeKey(type, "retired"), typeKey(type, "ids")};
        const vector<string> collectArgs = {
          id, std::to_string(m_config.retiredTombstoneRetentionSeconds * 1000)};
        eval(redis, kCollectRetiredServerTombstoneScript, collectKeys, collectArgs);
      }
    }
  });
}
