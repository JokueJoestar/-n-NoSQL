import os, re, hashlib

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CASS_HOST = os.getenv("CASS_HOST", "127.0.0.1")
CASS_PORT = int(os.getenv("CASS_PORT", "9042"))
KEYSPACE = "error_collector"

RL_WINDOW = int(os.getenv("RL_WINDOW", "60"))      # giay
RL_LIMIT = int(os.getenv("RL_LIMIT", "100"))       # log / cua so
DEDUP_TTL = int(os.getenv("DEDUP_TTL", "5"))       # giay
NO_DEDUP = os.getenv("NO_DEDUP") == "1"           # chi de benchmark so sanh
GROUP_TTL = 7 * 24 * 3600
TREND_TTL = 2 * 24 * 3600
RECENT_MAX = 100


def normalize(trace: str, k: int = 5):
    frames = []
    for line in trace.splitlines():
        line = re.sub(r"0x[0-9a-fA-F]+", "", line)
        line = re.sub(r"[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}", "", line)
        line = re.sub(r":\d+", "", line)
        line = re.sub(r"\d+", "N", line)
        line = line.strip()
        if line:
            frames.append(line)
    return frames[:k]


def error_hash(error_type: str, trace: str) -> str:
    raw = error_type + "|" + "\n".join(normalize(trace))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()
