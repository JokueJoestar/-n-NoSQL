"""Kiem tra logic Redis (Lua) khong can Docker: pip install fakeredis[lua] pytest"""
import time, fakeredis
from app.scripts import RATELIMIT, GROUP, TAKE_DELTA
from app.common import error_hash

r = fakeredis.FakeRedis(decode_responses=True)
rl, grp, td = r.register_script(RATELIMIT), r.register_script(GROUP), r.register_script(TAKE_DELTA)


def ingest(dev, h="H", ttl=1):
    first = grp(keys=[f"dedup:a:{dev}:{h}", f"errgroup:a:{h}", "errtrend:a:d", "dirty_groups"],
                args=[ttl, 1000, "m", h, 600, 600, "NPE"])
    return first


def test_ratelimit():
    ok = sum(rl(keys=["ratelimit:a:d1"], args=[60, 100]) for _ in range(150))
    assert ok == 100 and r.ttl("ratelimit:a:d1") > 0


def test_dedup_same_device():
    r.flushall()
    res = [ingest("d1") for _ in range(25)]
    assert sum(res) == 1                      # chi 1 ban ghi chi tiet
    assert int(r.hget("errgroup:a:H", "count")) == 25
    time.sleep(1.2)
    assert ingest("d1") == 1                  # het TTL -> ghi mau moi


def test_dedup_multi_device():
    r.flushall()
    assert sum(ingest(f"d{i}") for i in range(50)) == 50
    assert int(r.hget("errgroup:a:H", "count")) == 50


def test_delta_no_double_count():
    r.flushall()
    for _ in range(7):
        ingest("d1")
    assert td(keys=["errgroup:a:H"]) == 7
    assert td(keys=["errgroup:a:H"]) == 0     # chay lai -> khong dem doi
    ingest("d2")
    assert td(keys=["errgroup:a:H"]) == 1
    assert r.zscore("errtrend:a:d", "H") == 8


def test_hash_stable():
    a = "X\n at A.f(A.java:42)\n at M.run(M.java:10)"
    b = "X\n at A.f(A.java:57)\n at M.run(M.java:11)"
    assert error_hash("T", a) == error_hash("T", b)
    assert error_hash("T", a) != error_hash("U", a)
