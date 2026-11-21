# 4-basic

Minimal Redis-backed job lifecycle and heartbeat implementation.

## Requirements
- C++17
- redis-plus-plus and hiredis
- Redis standalone or Redis Cluster
- Host application configuration registry providing `get_reg_val()`

## Build
Compile the implementation files in this directory and link the dependencies above.

## Behavior
Job lifecycle, heartbeat, fencing, and cleanup operations use Lua scripts for atomic Redis updates. Lifecycle keys are colocated by server type for Redis Cluster. Configure Redis connection settings and server identity before initializing the singleton APIs.
