-- Databricks Delta Lake Schema for Network Analytics

-- Create database
CREATE DATABASE IF NOT EXISTS network_analytics
COMMENT 'Real-time network traffic analytics'
LOCATION '/mnt/delta/network_analytics';

USE network_analytics;

-- Raw Packets Table
CREATE TABLE IF NOT EXISTS raw_packets (
    packet_timestamp TIMESTAMP,
    capture_timestamp TIMESTAMP,
    processing_time TIMESTAMP,
    kafka_timestamp TIMESTAMP,
    length INT,
    src_ip STRING,
    dst_ip STRING,
    protocol STRING,
    ttl INT,
    src_port INT,
    dst_port INT,
    tcp_flags STRING,
    transport STRING,
    dns_query STRING,
    http_host STRING,
    http_method STRING,
    is_outbound BOOLEAN,
    packet_size_category STRING,
    src_ip_class STRING,
    service STRING
)
USING DELTA
PARTITIONED BY (DATE(packet_timestamp))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Traffic Statistics Table (Aggregated)
CREATE TABLE IF NOT EXISTS traffic_stats (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    src_ip STRING,
    dst_ip STRING,
    service STRING,
    packet_count BIGINT,
    total_bytes BIGINT,
    avg_packet_size DOUBLE,
    max_packet_size INT,
    min_packet_size INT,
    processed_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(window_start))
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Enable Change Data Feed for downstream consumers
ALTER TABLE raw_packets SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE traffic_stats SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Create views for common analytics queries

-- Top talkers by traffic volume
CREATE OR REPLACE VIEW top_talkers_by_volume AS
SELECT 
    src_ip,
    dst_ip,
    service,
    date_trunc('hour', window_start) as hour,
    SUM(total_bytes) as total_bytes,
    SUM(packet_count) as total_packets
FROM traffic_stats
WHERE window_start >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY src_ip, dst_ip, service, date_trunc('hour', window_start)
ORDER BY total_bytes DESC;

-- Service distribution
CREATE OR REPLACE VIEW service_distribution AS
SELECT 
    service,
    date_trunc('hour', window_start) as hour,
    SUM(packet_count) as packet_count,
    SUM(total_bytes) as total_bytes
FROM traffic_stats
WHERE window_start >= current_timestamp() - INTERVAL 1 DAY
GROUP BY service, date_trunc('hour', window_start)
ORDER BY hour DESC, total_bytes DESC;

-- Anomaly detection using Z-score
CREATE OR REPLACE VIEW traffic_anomalies AS
WITH stats AS (
    SELECT 
        src_ip,
        dst_ip,
        AVG(total_bytes) as avg_bytes,
        STDDEV(total_bytes) as stddev_bytes
    FROM traffic_stats
    WHERE window_start >= current_timestamp() - INTERVAL 7 DAYS
    GROUP BY src_ip, dst_ip
)
SELECT 
    t.window_start,
    t.src_ip,
    t.dst_ip,
    t.service,
    t.total_bytes,
    s.avg_bytes,
    (t.total_bytes - s.avg_bytes) / NULLIF(s.stddev_bytes, 0) as z_score
FROM traffic_stats t
JOIN stats s ON t.src_ip = s.src_ip AND t.dst_ip = s.dst_ip
WHERE t.window_start >= current_timestamp() - INTERVAL 1 DAY
    AND ABS((t.total_bytes - s.avg_bytes) / NULLIF(s.stddev_bytes, 0)) > 3
ORDER BY z_score DESC;

-- Real-time dashboard metrics
CREATE OR REPLACE VIEW realtime_metrics AS
SELECT 
    date_trunc('minute', window_start) as minute,
    COUNT(DISTINCT src_ip) as unique_sources,
    COUNT(DISTINCT dst_ip) as unique_destinations,
    SUM(packet_count) as total_packets,
    SUM(total_bytes) as total_bytes,
    AVG(avg_packet_size) as avg_packet_size
FROM traffic_stats
WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY date_trunc('minute', window_start)
ORDER BY minute DESC;

-- Protocol distribution
CREATE OR REPLACE VIEW protocol_distribution AS
SELECT 
    transport,
    service,
    COUNT(*) as connection_count,
    SUM(length) as total_bytes
FROM raw_packets
WHERE packet_timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY transport, service
ORDER BY total_bytes DESC;

-- Optimize tables periodically (run this as a scheduled job)
-- OPTIMIZE raw_packets WHERE DATE(packet_timestamp) >= current_date() - INTERVAL 7 DAYS;
-- OPTIMIZE traffic_stats WHERE DATE(window_start) >= current_date() - INTERVAL 7 DAYS;

-- Vacuum old files (run this periodically to remove old data)
-- VACUUM raw_packets RETAIN 168 HOURS; -- 7 days
-- VACUUM traffic_stats RETAIN 168 HOURS;