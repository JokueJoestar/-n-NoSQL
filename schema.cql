-- Dev: 1 node nen RF=1. Production: NetworkTopologyStrategy, RF=3
CREATE KEYSPACE IF NOT EXISTS error_collector
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}

CREATE TABLE IF NOT EXISTS error_collector.error_logs (
  app_id text, log_date date, log_hour tinyint, log_id timeuuid,
  device_id text, error_type text, severity text, message text,
  stack_trace text, environment map<text, text>, error_hash text,
  PRIMARY KEY ((app_id, log_date, log_hour), log_id)
) WITH CLUSTERING ORDER BY (log_id DESC)
  AND default_time_to_live = 7776000
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS', 'compaction_window_size': 1}

CREATE TABLE IF NOT EXISTS error_collector.error_logs_by_device (
  device_id text, log_date date, log_id timeuuid,
  app_id text, error_type text, severity text, message text,
  PRIMARY KEY ((device_id, log_date), log_id)
) WITH CLUSTERING ORDER BY (log_id DESC)
  AND default_time_to_live = 7776000
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS', 'compaction_window_size': 1}

CREATE TABLE IF NOT EXISTS error_collector.error_logs_by_hash (
  app_id text, error_hash text, log_date date, log_id timeuuid,
  device_id text, severity text,
  PRIMARY KEY ((app_id, error_hash, log_date), log_id)
) WITH CLUSTERING ORDER BY (log_id DESC)
  AND default_time_to_live = 7776000
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS', 'compaction_window_size': 1}

CREATE TABLE IF NOT EXISTS error_collector.error_groups (
  app_id text, error_hash text, error_type text, sample_message text,
  first_seen timestamp, last_seen timestamp,
  PRIMARY KEY (app_id, error_hash)
)

CREATE TABLE IF NOT EXISTS error_collector.error_group_counter (
  app_id text, error_hash text, occurrence_count counter,
  PRIMARY KEY (app_id, error_hash)
)

CREATE TABLE IF NOT EXISTS error_collector.error_trend_daily (
  app_id text, log_date date, rank int, error_hash text, occurrence_count bigint,
  PRIMARY KEY ((app_id, log_date), rank)
) WITH default_time_to_live = 31536000
