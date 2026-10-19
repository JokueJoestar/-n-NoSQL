"""Nhap du lieu mau qua API (de di qua day du rate-limit, dedup, gom nhom).
Chay: python seed.py   (API phai dang chay o cong 8000)  -> sau do: python worker.py --once"""
import random, requests

URL = "http://localhost:8000/logs"
APPS = ["shop_app", "chat_app", "pay_app"]
DEVICES_PER_APP = 20
LOGS_PER_DEVICE = 5
ERRORS = {
    "NullPointerException": ("cart is null", "java.lang.NullPointerException\n at com.demo.Cart.add(Cart.java:{a})\n at com.demo.Main.run(Main.java:{b})"),
    "ConnectionTimeout": ("connect timed out after 30s", "java.net.SocketTimeoutException\n at com.demo.Http.call(Http.java:{a})\n at com.demo.Sync.run(Sync.java:{b})"),
    "OutOfMemoryError": ("Java heap space", "java.lang.OutOfMemoryError\n at com.demo.Image.decode(Image.java:{a})\n at com.demo.Gallery.load(Gallery.java:{b})"),
    "SQLException": ("database is locked", "java.sql.SQLException\n at com.demo.Db.exec(Db.java:{a})\n at com.demo.Repo.save(Repo.java:{b})"),
}
SEVERITY = {"NullPointerException": "ERROR", "ConnectionTimeout": "WARNING", "OutOfMemoryError": "FATAL", "SQLException": "ERROR"}

random.seed(42)
tot = {"stored": 0, "counted": 0, "blocked": 0}
for app in APPS:
    for d in range(1, DEVICES_PER_APP + 1):
        for _ in range(LOGS_PER_DEVICE):
            et = random.choices(list(ERRORS), weights=[5, 3, 1, 2])[0]
            msg, tr = ERRORS[et]
            body = dict(app_id=app, device_id=f"{app}_dev_{d:03d}", error_type=et, severity=SEVERITY[et], message=msg,
                        stack_trace=tr.format(a=random.randint(10, 99), b=random.randint(10, 99)),
                        environment={"os": random.choice(["android 14", "ios 17"]), "app_version": random.choice(["2.3.1", "2.4.0"])})
            r = requests.post(URL, json=body)
            tot["blocked" if r.status_code == 429 else r.json()["status"]] += 1
print("Da gui", sum(tot.values()), "log:", tot)
print("Tiep theo: python worker.py --once")
