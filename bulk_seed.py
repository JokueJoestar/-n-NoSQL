"""Nap du lieu mau LON truc tiep vao Cassandra (mac dinh 500.000 log, ghi vao ca 3 bang log).
Chay:  python bulk_seed.py                 -> 500.000 log
       python bulk_seed.py 1000000         -> 1.000.000 log
       python bulk_seed.py 500000 --reset  -> xoa sach 6 bang truoc khi nap (khuyen dung)
Sau do: python bulk_seed.py --verify       -> dem so dong tung bang

Luu y: script ghi thang vao Cassandra (khong di qua API/Redis) de nap nhanh; so lieu gop nhom
(error_groups, error_group_counter, error_trend_daily) duoc tinh san roi ghi mot lan.
"""
import random, sys, time, warnings, datetime as dt
warnings.filterwarnings('ignore')
from collections import defaultdict
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.util import uuid_from_time
from app.common import CASS_HOST, CASS_PORT, KEYSPACE, error_hash

APPS = ["shop_app", "chat_app", "pay_app", "game_app", "map_app"]
DEVICES_PER_APP = 1000          # 5 app x 1000 = 5000 thiet bi
DAYS = 7                        # du lieu rai deu 7 ngay gan nhat
CONCURRENCY = 128
CHUNK = 15000                   # so log moi dot (gioi han RAM)

ERRORS = {   # loai loi: (message, severity, [3 bien the stack trace])
    "NullPointerException": ("cart is null", "ERROR", ["com.shop.Cart.add", "com.shop.Order.total", "com.shop.User.load"]),
    "ConnectionTimeout": ("connect timed out after 30s", "WARNING", ["com.net.Http.call", "com.net.Sync.run", "com.net.Api.fetch"]),
    "OutOfMemoryError": ("Java heap space", "FATAL", ["com.ui.Image.decode", "com.ui.Gallery.load", "com.ui.Video.play"]),
    "SQLException": ("database is locked", "ERROR", ["com.db.Dao.exec", "com.db.Repo.save", "com.db.Cache.flush"]),
    "IllegalStateException": ("fragment not attached", "ERROR", ["com.ui.Main.show", "com.ui.Tab.switch", "com.ui.Nav.go"]),
    "IOException": ("disk full", "WARNING", ["com.io.File.write", "com.io.Log.flush", "com.io.Cache.put"]),
    "ArrayIndexOutOfBounds": ("index 7 out of bounds", "ERROR", ["com.util.List.get", "com.util.Page.slice", "com.util.Row.cell"]),
    "SecurityException": ("permission denied", "WARNING", ["com.sec.Perm.check", "com.sec.Token.read", "com.sec.Key.load"]),
}
WEIGHTS = [30, 20, 5, 12, 10, 8, 10, 5]
OS = ["android 14", "android 13", "ios 17", "ios 16"]
VERSIONS = ["2.3.1", "2.4.0", "2.5.2"]


def make_trace(etype, method, n1, n2):
    return f"java.lang.{etype}\n at {method}(X.java:{n1})\n at com.app.Main.run(Main.java:{n2})"


def generate(n, rnd):
    """Sinh n log. Tra ve danh sach (3 tuple tham so) va thong ke gop nhom."""
    now = time.time()
    start = now - DAYS * 86400
    names = list(ERRORS)
    for _ in range(n):
        app = rnd.choice(APPS)
        device = f"{app}_dev_{rnd.randint(1, DEVICES_PER_APP):03d}"
        etype = rnd.choices(names, WEIGHTS)[0]
        msg, sev, methods = ERRORS[etype]
        trace = make_trace(etype, rnd.choice(methods), rnd.randint(10, 99), rnd.randint(10, 99))
        h = error_hash(etype, trace)
        ts = rnd.uniform(start, now)
        t = dt.datetime.fromtimestamp(ts, dt.timezone.utc)
        yield dict(app=app, device=device, etype=etype, msg=msg, sev=sev, trace=trace, h=h, ts=ts,
                   date=t.date(), hour=t.hour, log_id=uuid_from_time(ts),
                   env={"os": rnd.choice(OS), "app_version": rnd.choice(VERSIONS)})


def log_params(rows):
    a = [(r["app"], r["date"], r["hour"], r["log_id"], r["device"], r["etype"], r["sev"], r["msg"], r["trace"], r["env"], r["h"]) for r in rows]
    b = [(r["device"], r["date"], r["log_id"], r["app"], r["etype"], r["sev"], r["msg"]) for r in rows]
    c = [(r["app"], r["h"], r["date"], r["log_id"], r["device"], r["sev"]) for r in rows]
    return a, b, c


def run_batch(session, stmt, params):
    execute_concurrent_with_args(session, stmt, params, concurrency=CONCURRENCY, raise_on_first_error=True)


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}
    cluster = Cluster([CASS_HOST], port=CASS_PORT)
    session = cluster.connect(KEYSPACE)
    session.default_timeout = 60
    session.default_consistency_level = ConsistencyLevel.LOCAL_ONE

    if "--verify" in flags:
        # COUNT(*) toan bang de bi timeout khi co hang trieu dong -> dem theo tung partition (nhanh, chinh xac)
        today = dt.datetime.now(dt.timezone.utc).date()
        days = [today - dt.timedelta(days=i) for i in range(DAYS + 1)]
        def total(stmt, params):
            # dem it yeu cau song song + chia dot + thu lai de tranh "Coordinator node overloaded"
            n = 0
            for i in range(0, len(params), 1000):
                chunk = params[i:i + 1000]
                for attempt in range(5):
                    try:
                        res = execute_concurrent_with_args(session, stmt, chunk, concurrency=8, raise_on_first_error=True)
                        n += sum(r.one()[0] for ok, r in res)
                        break
                    except Exception as e:
                        if attempt == 4:
                            raise
                        time.sleep(3)
            return n
        c1 = total(session.prepare("SELECT COUNT(*) FROM error_logs WHERE app_id=? AND log_date=? AND log_hour=?"),
                   [(a, d, h) for a in APPS for d in days for h in range(24)])
        c2 = total(session.prepare("SELECT COUNT(*) FROM error_logs_by_device WHERE device_id=? AND log_date=?"),
                   [(f"{a}_dev_{i:03d}", d) for a in APPS for i in range(1, DEVICES_PER_APP + 1) for d in days])
        hashes = [(r.app_id, r.error_hash) for r in session.execute("SELECT app_id, error_hash FROM error_groups")]
        c3 = total(session.prepare("SELECT COUNT(*) FROM error_logs_by_hash WHERE app_id=? AND error_hash=? AND log_date=?"),
                   [(a, h, d) for a, h in hashes for d in days])
        cnt = sum(r.occurrence_count for r in session.execute("SELECT occurrence_count FROM error_group_counter"))
        print(f"error_logs            {c1:>10,}")
        print(f"error_logs_by_device  {c2:>10,}")
        print(f"error_logs_by_hash    {c3:>10,}")
        print(f"error_groups          {len(hashes):>10,} nhom")
        print(f"tong occurrence_count {cnt:>10,}")
        cluster.shutdown()
        return

    n = int(args[0]) if args else 500_000
    if "--reset" in flags:
        for t in ["error_logs", "error_logs_by_device", "error_logs_by_hash", "error_groups", "error_group_counter", "error_trend_daily"]:
            session.execute(f"TRUNCATE {t}", timeout=120)
        print("Da xoa sach 6 bang.")

    p = session.prepare
    ins_log = p("INSERT INTO error_logs (app_id, log_date, log_hour, log_id, device_id, error_type, severity, message, stack_trace, environment, error_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?)")
    ins_dev = p("INSERT INTO error_logs_by_device (device_id, log_date, log_id, app_id, error_type, severity, message) VALUES (?,?,?,?,?,?,?)")
    ins_hash = p("INSERT INTO error_logs_by_hash (app_id, error_hash, log_date, log_id, device_id, severity) VALUES (?,?,?,?,?,?)")

    rnd = random.Random(42)                       # co dinh seed de lap lai duoc
    groups = {}                                   # (app, hash) -> [count, first_ts, last_ts, etype, msg]
    daily = defaultdict(int)                      # (app, date, hash) -> count
    done, t0, buf = 0, time.time(), []

    def flush(rows):
        a, b, c = log_params(rows)
        run_batch(session, ins_log, a)
        run_batch(session, ins_dev, b)
        run_batch(session, ins_hash, c)

    for r in generate(n, rnd):
        g = groups.get((r["app"], r["h"]))
        if g is None:
            groups[(r["app"], r["h"])] = [1, r["ts"], r["ts"], r["etype"], r["msg"]]
        else:
            g[0] += 1
            g[1] = min(g[1], r["ts"])
            g[2] = max(g[2], r["ts"])
        daily[(r["app"], r["date"], r["h"])] += 1
        buf.append(r)
        if len(buf) >= CHUNK:
            flush(buf); done += len(buf); buf = []
            el = time.time() - t0
            print(f"  {done:>9,}/{n:,} log  |  {done / el:,.0f} log/giay  |  {el:.0f}s", flush=True)
    if buf:
        flush(buf); done += len(buf)

    # --- bang thong ke: tinh san trong RAM roi ghi mot lan
    upd_counter = p("UPDATE error_group_counter SET occurrence_count = occurrence_count + ? WHERE app_id = ? AND error_hash = ?")
    ins_group = p("INSERT INTO error_groups (app_id, error_hash, error_type, sample_message, first_seen, last_seen) VALUES (?,?,?,?,?,?)")
    ins_trend = p("INSERT INTO error_trend_daily (app_id, log_date, rank, error_hash, occurrence_count) VALUES (?,?,?,?,?)")
    ts_ = lambda x: dt.datetime.fromtimestamp(x, dt.timezone.utc)
    run_batch(session, upd_counter, [(g[0], app, h) for (app, h), g in groups.items()])
    run_batch(session, ins_group, [(app, h, g[3], g[4], ts_(g[1]), ts_(g[2])) for (app, h), g in groups.items()])
    per_day = defaultdict(list)
    for (app, d, h), c in daily.items():
        per_day[(app, d)].append((c, h))
    trend = []
    for (app, d), lst in per_day.items():
        for rank, (c, h) in enumerate(sorted(lst, reverse=True)[:100], 1):
            trend.append((app, d, rank, h, c))
    run_batch(session, ins_trend, trend)

    el = time.time() - t0
    print(f"\nXONG: {done:,} log (x3 bang = {done * 3:,} dong) | {len(groups)} nhom loi | {len(trend)} dong xu huong")
    print(f"Thoi gian {el:.0f}s ({done / el:,.0f} log/giay)")
    print("Kiem tra: python bulk_seed.py --verify")
    cluster.shutdown()


if __name__ == "__main__":
    main()