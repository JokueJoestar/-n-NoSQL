"""Dong bo dinh ky Redis -> Cassandra (chi day delta). Chay: python worker.py [--once]"""
import sys, time, datetime as dt
import redis
from app.common import *
from app.scripts import TAKE_DELTA
from app.store import Store

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
take_delta = r.register_script(TAKE_DELTA)
store = Store()


def ts(ms):
    return dt.datetime.fromtimestamp(int(ms) / 1000, tz=dt.timezone.utc)


def sync_once():
    keys = r.spop("dirty_groups", 500) or []
    done = 0
    for key in keys:                      # key = errgroup:{app_id}:{error_hash}
        _, app_id, h = key.split(":", 2)
        delta = int(take_delta(keys=[key]))
        if delta <= 0:
            continue
        g = r.hgetall(key)
        store.add_counter(delta, app_id, h)
        store.upsert_group(app_id, h, g.get("error_type", ""), g.get("sample_message", ""), ts(g["first_seen"]), ts(g["last_seen"]))
        done += 1
    # snapshot top-N cua errtrend
    for tk in r.scan_iter(match="errtrend:*"):
        _, app_id, day = tk.split(":", 2)
        d = dt.date.fromisoformat(day)
        for rank, (h, score) in enumerate(r.zrevrange(tk, 0, 99, withscores=True), start=1):
            store.save_trend(app_id, d, rank, h, int(score))
    print(f"[worker] synced {done} groups")


if __name__ == "__main__":
    if "--once" in sys.argv:
        sync_once()
    else:
        while True:
            sync_once()
            time.sleep(60)
