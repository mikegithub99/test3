/**
 * @file RedisStatusCli.cc
 * @brief Standalone Redis keyspace inspector for current Redis Job Status state.
 *
 * Configuration:
 *   REDIS_URL=redis://127.0.0.1:6379
 *
 * Optional:
 *   REDIS_PATTERN='*'       Key pattern passed to SCAN.
 *   REDIS_SCAN_COUNT=500    SCAN count hint.
 */

#include <sw/redis++/redis++.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using std::cerr;
using std::cout;
using std::getenv;
using std::pair;
using std::string;
using std::vector;
using sw::redis::Redis;
using sw::redis::RedisCluster;

namespace
{
string environmentValue(const char* name, const string& fallback)
{
  const char* value = getenv(name);
  return value == nullptr || *value == '\0' ? fallback : value;
}

long long scanCount()
{
  const string value = environmentValue("REDIS_SCAN_COUNT", "500");
  try
  {
    const auto count = std::stoll(value);
    return count > 0 ? count : 500;
  }
  catch (...)
  {
    return 500;
  }
}

void printKey(Redis& redis, const string& key)
{
  const string type = redis.type(key);
  cout << "\n[" << type << "] " << key << '\n';

  if (type == "set")
  {
    vector<string> members;
    redis.smembers(key, std::back_inserter(members));
    cout << "  members (" << members.size() << "):\n";
    for (const auto& member : members)
    {
      cout << "    " << member << '\n';
    }
  }
  else if (type == "hash")
  {
    vector<pair<string, string>> fields;
    redis.hgetall(key, std::back_inserter(fields));
    cout << "  fields (" << fields.size() << "):\n";
    for (const auto& field : fields)
    {
      cout << "    " << field.first << " = " << field.second << '\n';
    }
  }
  else if (type == "string")
  {
    const auto value = redis.get(key);
    cout << "  value: " << (value ? *value : string("<missing>")) << '\n';
    cout << "  TTL: " << redis.ttl(key) << " seconds\n";
  }
  else if (type == "zset")
  {
    vector<pair<string, double>> members;
    redis.zrange(key, 0, -1, std::back_inserter(members));
    cout << "  members (" << members.size()
         << "), member -> score (Redis timestamp in Unix milliseconds):\n";
    for (const auto& member : members)
    {
      cout << "    " << member.first << " -> " << member.second << '\n';
    }
  }
  else
  {
    cout << "  (value display is currently implemented for SET, HASH, STRING, and ZSET)\n";
  }
}
} // namespace

int main()
{
  try
  {
    const string redisUrl = environmentValue(
        "REDIS_URL", "redis://127.0.0.1:6379");
    const string pattern = environmentValue("REDIS_PATTERN", "*");
    const bool cluster = environmentValue("REDIS_CLUSTER", "false") == "true" ||
                         environmentValue("REDIS_CLUSTER", "0") == "1";
    std::unique_ptr<Redis> standalone;
    std::unique_ptr<RedisCluster> clustered;
    if (cluster) clustered = std::make_unique<RedisCluster>(RedisCluster::Options{redisUrl});
    else standalone = std::make_unique<Redis>(redisUrl);
    const long long count = scanCount();
    size_t keyCount = 0;

    cout << "Redis keyspace inspection\n"
         << "URL: " << redisUrl << '\n'
         << "Pattern: " << pattern << '\n';

    auto inspect = [&](auto& client) {
      for (const auto& key : client.scan_iter(pattern, count)) {
        try { printKey(client, key); ++keyCount; }
        catch (const sw::redis::Error& error) {
          cerr << "\n[warning] Could not read key '" << key
               << "': " << error.what() << '\n';
        }
      }
    };
    if (clustered) inspect(*clustered);
    else inspect(*standalone);

    cout << "\nFinished. Read " << keyCount << " keys.\n";
    return EXIT_SUCCESS;
  }
  catch (const sw::redis::Error& error)
  {
    cerr << "RedisStatusCli: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
  catch (const std::exception& error)
  {
    cerr << "RedisStatusCli: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
