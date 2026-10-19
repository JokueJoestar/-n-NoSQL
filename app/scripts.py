# Lua scripts - chay nguyen tu tren Redis

RATELIMIT = """
local c = redis.call('INCR', KEYS[1])
if c == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
if c > tonumber(ARGV[2]) then return 0 end
return 1
"""

# KEYS: 1=dedup 2=errgroup 3=errtrend 4=dirty_groups
# ARGV: 1=dedup_ttl 2=now_ms 3=sample_message 4=error_hash 5=group_ttl 6=trend_ttl 7=error_type
GROUP = """
local first = redis.call('SET', KEYS[1], 1, 'NX', 'EX', ARGV[1])
redis.call('HINCRBY', KEYS[2], 'count', 1)
redis.call('HINCRBY', KEYS[2], 'pending', 1)
redis.call('HSETNX', KEYS[2], 'first_seen', ARGV[2])
redis.call('HSETNX', KEYS[2], 'sample_message', ARGV[3])
redis.call('HSETNX', KEYS[2], 'error_type', ARGV[7])
redis.call('HSET',   KEYS[2], 'last_seen', ARGV[2])
redis.call('EXPIRE', KEYS[2], ARGV[5])
redis.call('ZINCRBY', KEYS[3], 1, ARGV[4])
redis.call('EXPIRE', KEYS[3], ARGV[6])
redis.call('SADD', KEYS[4], KEYS[2])
if first then return 1 end
return 0
"""

TAKE_DELTA = """
local d = tonumber(redis.call('HGET', KEYS[1], 'pending') or '0')
if d > 0 then redis.call('HSET', KEYS[1], 'pending', 0) end
return d
"""
