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


async def alive_info(acct, role):
    raw = await r.get(f"acct:{acct}:alive:{role}")
    return json.loads(raw) if raw else None


async def deals_push(acct, deals: list[dict], keep=500):
    if deals:
        await r.lpush(f"deals:{acct}", *[json.dumps(d) for d in deals])
        await r.ltrim(f"deals:{acct}", 0, keep - 1)


async def deals_recent(acct, n=50):
    return [json.loads(x) for x in await r.lrange(f"deals:{acct}", 0, n - 1)]


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


async def record_timing(account_id, timing: dict, keep=500):
    async with r.pipeline() as p:
        p.lpush(f"metrics:{account_id}", json.dumps(timing)); p.ltrim(f"metrics:{account_id}", 0, keep - 1)
        await p.execute()


async def recent_timings(account_id, n=100):
    return [json.loads(x) for x in await r.lrange(f"metrics:{account_id}", 0, n - 1)]


# ---- scheduled exits (order.close when=market_open): the gateway is the source of truth, agents hold them in RAM only
async def sched_put(account_id, id_, payload):
    await r.hset(f"sched:{account_id}", id_, json.dumps(payload))


async def sched_all(account_id) -> dict:
    return {k.decode() if isinstance(k, bytes) else k: json.loads(v) for k, v in (await r.hgetall(f"sched:{account_id}")).items()}


async def sched_del(account_id, id_):
    await r.hdel(f"sched:{account_id}", id_)
