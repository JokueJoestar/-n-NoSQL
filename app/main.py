import time, uuid, datetime as dt
from typing import Dict, Optional
import redis
import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from .common import *
from .scripts import RATELIMIT, GROUP
from .store import Store

app = FastAPI(title="Error Log Collector")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
ratelimit = r.register_script(RATELIMIT)
group = r.register_script(GROUP)
store = Store()


class Log(BaseModel):
    app_id: str
    device_id: str
    error_type: str
    severity: str = "ERROR"
    message: str = ""
    stack_trace: str = ""
    environment: Dict[str, str] = {}


@app.post("/logs")
def ingest(log: Log):
    now = dt.datetime.now(dt.timezone.utc)
    h = error_hash(log.error_type, log.stack_trace)
    first = 1
    try:
        # Buoc 2: rate-limit
        if not ratelimit(keys=[f"ratelimit:{log.app_id}:{log.device_id}"], args=[RL_WINDOW, RL_LIMIT]):
            raise HTTPException(status_code=429, detail="rate limit exceeded")
        # Buoc 3: dedup + gom nhom
        first = group(
            keys=[f"dedup:{log.app_id}:{log.device_id}:{h}", f"errgroup:{log.app_id}:{h}",
                  f"errtrend:{log.app_id}:{now.date().isoformat()}", "dirty_groups"],
            args=[DEDUP_TTL, int(now.timestamp() * 1000), log.message[:500], h, GROUP_TTL, TREND_TTL, log.error_type])
    except redis.RedisError:
        first = 1   # fail-open: Redis loi -> ghi thang Cassandra, khong mat log
    if not first and not NO_DEDUP:
        return {"status": "counted", "error_hash": h}
    # Buoc 4: ghi ben vung
    lid = uuid.uuid1()
    store.write_log(dict(app_id=log.app_id, device_id=log.device_id, error_type=log.error_type,
                         severity=log.severity, message=log.message, stack_trace=log.stack_trace,
                         environment=log.environment, error_hash=h, log_id=lid,
                         date=now.date(), hour=now.hour))
    try:
        k = f"recent_errors:{log.app_id}"
        pipe = r.pipeline()
        pipe.lpush(k, f"{now.isoformat()} {log.device_id} {log.error_type} {log.message[:100]}")
        pipe.ltrim(k, 0, RECENT_MAX - 1)
        pipe.expire(k, 86400)
        pipe.execute()
    except redis.RedisError:
        pass
    return {"status": "stored", "error_hash": h, "log_id": str(lid)}


def today():
    return dt.datetime.now(dt.timezone.utc).date()


def parse_day(day: Optional[str]):
    return dt.date.fromisoformat(day) if day else today()


@app.get("/top/{app_id}")
def top(app_id: str, n: int = 10):
    """Top loi hom nay - doc tu Redis (errtrend) va lam giau bang errgroup."""
    rows = r.zrevrange(f"errtrend:{app_id}:{today().isoformat()}", 0, n - 1, withscores=True)
    out = []
    for h, score in rows:
        g = r.hmget(f"errgroup:{app_id}:{h}", "error_type", "sample_message")
        out.append(dict(error_hash=h, count=int(score), error_type=g[0], sample_message=g[1]))
    return out


@app.get("/recent/{app_id}")
def recent(app_id: str, n: int = 20):
    """Log moi nhat - doc tu Redis (recent_errors)."""
    return r.lrange(f"recent_errors:{app_id}", 0, n - 1)


@app.get("/groups/{app_id}")
def groups(app_id: str):
    """Nhom loi + tong so lan - doc tu Cassandra (error_groups + error_group_counter)."""
    return store.groups(app_id)


@app.get("/device/{device_id}")
def device(device_id: str, day: Optional[str] = None, n: int = 50):
    """Log cua mot thiet bi trong ngay - doc tu Cassandra (error_logs_by_device)."""
    return store.device_logs(device_id, parse_day(day), n)


@app.get("/trend/{app_id}")
def trend(app_id: str, day: Optional[str] = None, n: int = 20):
    """Top loi da luu - doc tu Cassandra (error_trend_daily)."""
    return store.trend(app_id, parse_day(day), n)


@app.get("/")
def home():
    return FileResponse(os.path.join(os.path.dirname(__file__), "..", "static", "index.html"))
