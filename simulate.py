"""Gia lap thiet bi. Vi du: python simulate.py ratelimit | dedup | multi | burst"""
import sys, time, requests
URL = "http://localhost:8000/logs"
TRACE = "java.lang.NullPointerException\n at com.shop.Cart.add(Cart.java:{n})\n at com.shop.Main.run(Main.java:10)"


def send(device, line=42, app="shop_app"):
    body = dict(app_id=app, device_id=device, error_type="NullPointerException",
                message="cart is null", stack_trace=TRACE.format(n=line),
                environment={"os": "android 14", "app_version": "2.3.1"})
    return requests.post(URL, json=body).json()


def run(n, per_sec, device="device_882"):
    stats = {"stored": 0, "counted": 0, "blocked": 0}
    for i in range(n):
        r = requests.post(URL, json=dict(app_id="shop_app", device_id=device, error_type="NullPointerException",
                          message="cart is null", stack_trace=TRACE.format(n=40 + i % 7)))
        key = "blocked" if r.status_code == 429 else r.json()["status"]
        stats[key] += 1
        time.sleep(1 / per_sec)
    return stats


if __name__ == "__main__":
    sc = sys.argv[1] if len(sys.argv) > 1 else "ratelimit"
    if sc == "ratelimit":      # 150 log / 30s, 1 thiet bi
        print(run(150, 5))
    elif sc == "dedup":        # 100 log / 20s
        print(run(100, 5, "device_dedup"))
    elif sc == "multi":        # 50 thiet bi cung 1 loi
        print([send(f"dev_{i}")["status"] for i in range(50)].count("stored"), "stored / 50")
    elif sc == "burst":        # tai: 10.000 log tu 100 thiet bi, khong sleep
        t = time.time(); ok = 0
        for i in range(10000):
            r = requests.post(URL, json=dict(app_id="shop_app", device_id=f"bench_{i % 100}", error_type="Timeout",
                              message="x", stack_trace=TRACE.format(n=i % 3)))
            ok += r.status_code == 200
        print(f"{ok} ok trong {time.time() - t:.1f}s")
