import asyncio, json, time
import redis.asyncio as redis
from . import config

r = redis.from_url(config.REDIS_URL, decode_responses=True)


async def save_state(acct, state):
    await r.hset(f"acct:{acct}:state", mapping={"data": json.dumps(state), "updated_at": time.time()})


async def get_state(acct):
    raw = await r.hget(f"acct:{acct}:state", "data")
    return json.loads(raw) if raw else None


async def alive(acct, role, hb):
    await r.set(f"acct:{acct}:alive:{role}", json.dumps(hb), ex=15)


async def is_alive(acct, role) -> bool:
    return bool(await r.exists(f"acct:{acct}:alive:{role}"))


async def publish(channel, event, data):
    await r.publish(channel, json.dumps({"event": event, "data": data}))


async def subscribe(channels: list[str], idle=15.0):
    """Yield (event, data); yield (None, None) when idle for `idle` seconds so callers can ping."""
    ps = r.pubsub()
    await ps.subscribe(*channels)
    deadline = time.monotonic() + idle
    try:
        while True:
            msg = await ps.get_message(ignore_subscribe_messages=True, timeout=max(0.0, deadline - time.monotonic()))
            if msg is None:  # redis-py also returns None for ignored subscribe confirmations, so check the clock
                if time.monotonic() >= deadline:
                    deadline = time.monotonic() + idle
                    yield None, None
                continue
            deadline = time.monotonic() + idle
            body = json.loads(msg["data"])
            yield body["event"], body["data"]
    finally:
        await ps.unsubscribe(*channels)
        await ps.aclose()
