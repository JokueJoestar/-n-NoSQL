from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from .common import CASS_HOST, CASS_PORT, KEYSPACE


class Store:
    def __init__(self):
        self.cluster = Cluster([CASS_HOST], port=CASS_PORT)
        self.s = self.cluster.connect(KEYSPACE)
        self.s.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
        p = self.s.prepare
        self.ins_log = p("INSERT INTO error_logs (app_id, log_date, log_hour, log_id, device_id, error_type, severity, message, stack_trace, environment, error_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?)")
        self.ins_dev = p("INSERT INTO error_logs_by_device (device_id, log_date, log_id, app_id, error_type, severity, message) VALUES (?,?,?,?,?,?,?)")
        self.ins_hash = p("INSERT INTO error_logs_by_hash (app_id, error_hash, log_date, log_id, device_id, severity) VALUES (?,?,?,?,?,?)")
        self.upd_counter = p("UPDATE error_group_counter SET occurrence_count = occurrence_count + ? WHERE app_id = ? AND error_hash = ?")
        self.ins_group = p("INSERT INTO error_groups (app_id, error_hash, error_type, sample_message, first_seen) VALUES (?,?,?,?,?) IF NOT EXISTS")
        self.upd_last = p("UPDATE error_groups SET last_seen = ? WHERE app_id = ? AND error_hash = ?")
        self.ins_trend = p("INSERT INTO error_trend_daily (app_id, log_date, rank, error_hash, occurrence_count) VALUES (?,?,?,?,?)")
        self.sel_groups = p("SELECT error_hash, error_type, sample_message, first_seen, last_seen FROM error_groups WHERE app_id = ?")
        self.sel_counter = p("SELECT error_hash, occurrence_count FROM error_group_counter WHERE app_id = ?")
        self.sel_device = p("SELECT toTimestamp(log_id) AS log_time, app_id, error_type, severity, message FROM error_logs_by_device WHERE device_id = ? AND log_date = ? LIMIT ?")
        self.sel_trend = p("SELECT rank, error_hash, occurrence_count FROM error_trend_daily WHERE app_id = ? AND log_date = ? LIMIT ?")
        self.sel_hash = p("SELECT toTimestamp(log_id) AS log_time, device_id, severity FROM error_logs_by_hash WHERE app_id = ? AND error_hash = ? AND log_date = ? LIMIT ?")
        self.count_groups = p("SELECT COUNT(*) FROM error_groups WHERE app_id = ?")

    def write_log(self, d):
        """Ghi 3 bang song song. Cung log_id nen retry an toan (idempotent)."""
        fs = [
            self.s.execute_async(self.ins_log, (d["app_id"], d["date"], d["hour"], d["log_id"], d["device_id"], d["error_type"], d["severity"], d["message"], d["stack_trace"], d["environment"], d["error_hash"])),
            self.s.execute_async(self.ins_dev, (d["device_id"], d["date"], d["log_id"], d["app_id"], d["error_type"], d["severity"], d["message"])),
            self.s.execute_async(self.ins_hash, (d["app_id"], d["error_hash"], d["date"], d["log_id"], d["device_id"], d["severity"])),
        ]
        for f in fs:
            f.result()

    # ---- dung cho worker
    def add_counter(self, delta, app_id, h):
        self.s.execute(self.upd_counter, (delta, app_id, h))

    def upsert_group(self, app_id, h, error_type, sample, first_seen, last_seen):
        self.s.execute(self.ins_group, (app_id, h, error_type, sample, first_seen))   # chi ghi neu chua ton tai
        self.s.execute(self.upd_last, (last_seen, app_id, h))

    def save_trend(self, app_id, day, rank, h, count):
        self.s.execute(self.ins_trend, (app_id, day, rank, h, count))

    # ---- dung cho API doc
    def groups(self, app_id):
        counts = {r.error_hash: r.occurrence_count for r in self.s.execute(self.sel_counter, (app_id,))}
        out = [dict(error_hash=r.error_hash, error_type=r.error_type, sample_message=r.sample_message,
                    first_seen=r.first_seen, last_seen=r.last_seen, occurrence_count=counts.get(r.error_hash, 0))
               for r in self.s.execute(self.sel_groups, (app_id,))]
        return sorted(out, key=lambda x: -x["occurrence_count"])

    def device_logs(self, device_id, day, limit=50):
        return [dict(r._asdict()) for r in self.s.execute(self.sel_device, (device_id, day, limit))]

    def trend(self, app_id, day, limit=20):
        return [dict(r._asdict()) for r in self.s.execute(self.sel_trend, (app_id, day, limit))]

    def hash_logs(self, app_id, error_hash, day, limit=50):
        """Log cua mot nhom loi (tat ca thiet bi gap cung 1 loi) - tu error_logs_by_hash."""
        rows = self.s.execute(self.sel_hash, (app_id, error_hash, day, limit))
        return [{"log_time": r.log_time, "device_id": r.device_id, "severity": r.severity} for r in rows]

    def count_error_groups(self, app_id):
        """Dem so nhom loi cua mot app trong Cassandra."""
        row = self.s.execute(self.count_groups, (app_id,)).one()
        return int(row[0]) if row else 0
