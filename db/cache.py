"""
db/cache.py — Redis connection management and caching utilities.
"""
import json
import time
import redis.asyncio as redis
import config

_redis = None

async def get_redis():
    """Get or create the Redis connection pool."""
    global _redis
    if _redis is None:
        _redis = redis.from_url(config.REDIS_URL, decode_responses=True)
    return _redis

async def close_redis():
    """Close the Redis connection pool."""
    global _redis
    if _redis is not None:
        await _redis.close()
        _redis = None

async def cache_set(key: str, value: any, ttl_seconds: int = 60):
    """Store a value in Redis as JSON."""
    r = await get_redis()
    await r.set(key, json.dumps(value), ex=ttl_seconds)

async def cache_get(key: str):
    """Retrieve a value from Redis and parse JSON."""
    r = await get_redis()
    data = await r.get(key)
    if data:
        return json.loads(data)
    return None

async def cache_delete(key: str):
    """Remove a key from Redis."""
    r = await get_redis()
    await r.delete(key)

async def publish_alert(type: str, message: str, data: dict = None):
    """Publish an alert to Redis for the API to broadcast."""
    r = await get_redis()
    alert = {
        "type": type,
        "message": message,
        "timestamp": time.time()
    }
    if data:
        alert.update(data)
    encoded = json.dumps(alert)
    await r.publish("alerts", encoded)
    # Store history (last 500 events)
    await r.lpush("alert_history", encoded)
    await r.ltrim("alert_history", 0, 499)

async def is_paused() -> bool:
    """Check if the app is paused."""
    r = await get_redis()
    status = await r.get("app_paused")
    return status == "true"

async def set_paused(paused: bool):
    """Set the app paused state."""
    r = await get_redis()
    await r.set("app_paused", "true" if paused else "false")
    await publish_alert("system", "SYSTEM PAUSED" if paused else "SYSTEM RESUMED")
