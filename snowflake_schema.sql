-- Snowflake Schema for Network Analytics
-- Create database and schema

CREATE DATABASE IF NOT EXISTS network_analytics;
USE DATABASE network_analytics;
CREATE SCHEMA IF NOT EXISTS public;
USE SCHEMA public;

-- Create staging area for Spark Streaming
CREATE STAGE IF NOT EXISTS streaming_stage;

-- Raw Packets Table
CREATE TABLE IF NOT EXISTS raw_packets (
    packet_timestamp TIMESTAMP_NTZ,
    capture_timestamp TIMESTAMP_NTZ,
    processing_time TIMESTAMP_NTZ,
    kafka_timestamp TIMESTAMP_NTZ,
    length INTEGER,
    src_ip VARCHAR(45),
    dst_ip VARCHAR(45),
    protocol VARCHAR(10),
    ttl INTEGER,
    src_port INTEGER,
    dst_port INTEGER,
    tcp_flags VARCHAR(50),
    transport VARCHAR(10),
    dns_query VARCHAR(500),
    http_host VARCHAR(500),
    http_method VARCHAR(10),
    is_outbound BOOLEAN,
    packet_size_category VARCHAR(20),
    src_ip_class VARCHAR(10),
    service VARCHAR(50)
);

-- Traffic Statistics Table (Aggregated)
CREATE TABLE IF NOT EXISTS traffic_stats (
    window_start TIMESTAMP_NTZ,
    window_end TIMESTAMP_NTZ,
    src_ip VARCHAR(45),
    dst_ip VARCHAR(45),
    service VARCHAR(50),
    packet_count INTEGER,
    total_bytes BIGINT,
    avg_packet_size FLOAT,
    max_packet_size INTEGER,
    min_packet_size INTEGER,
    processed_at TIMESTAMP_NTZ
);

-- Create time-series clustering key for better query performance
ALTER TABLE raw_packets CLUSTER BY (TO_DATE(packet_timestamp));
ALTER TABLE traffic_stats CLUSTER BY (TO_DATE(window_start));

-- Create views for common analytics queries

-- Top talkers by traffic volume
CREATE OR REPLACE VIEW top_talkers_by_volume AS
SELECT 
    src_ip,
    dst_ip,
    service,
    DATE_TRUNC('hour', window_start) as hour,
    SUM(total_bytes) as total_bytes,
    SUM(packet_count) as total_packets
FROM traffic_stats
WHERE window_start >= DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY src_ip, dst_ip, service, DATE_TRUNC('hour', window_start)
ORDER BY total_bytes DESC;

-- Service distribution
CREATE OR REPLACE VIEW service_distribution AS
SELECT 
    service,
    DATE_TRUNC('hour', window_start) as hour,
    SUM(packet_count) as packet_count,
    SUM(total_bytes) as total_bytes
FROM traffic_stats
WHERE window_start >= DATEADD(day, -1, CURRENT_TIMESTAMP())
GROUP BY service, DATE_TRUNC('hour', window_start)
ORDER BY hour DESC, total_bytes DESC;

-- Anomaly detection - unusually high traffic
CREATE OR REPLACE VIEW traffic_anomalies AS
WITH stats AS (
    SELECT 
        src_ip,
        dst_ip,
        AVG(total_bytes) as avg_bytes,
        STDDEV(total_bytes) as stddev_bytes
    FROM traffic_stats
    WHERE window_start >= DATEADD(day, -7, CURRENT_TIMESTAMP())
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
WHERE t.window_start >= DATEADD(day, -1, CURRENT_TIMESTAMP())
    AND ABS((t.total_bytes - s.avg_bytes) / NULLIF(s.stddev_bytes, 0)) > 3
ORDER BY z_score DESC;

-- Real-time dashboard metrics
CREATE OR REPLACE VIEW realtime_metrics AS
SELECT 
    DATE_TRUNC('minute', window_start) as minute,
    COUNT(DISTINCT src_ip) as unique_sources,
    COUNT(DISTINCT dst_ip) as unique_destinations,
    SUM(packet_count) as total_packets,
    SUM(total_bytes) as total_bytes,
    AVG(avg_packet_size) as avg_packet_size
FROM traffic_stats
WHERE window_start >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
GROUP BY DATE_TRUNC('minute', window_start)
ORDER BY minute DESC;

-- Grant permissions (adjust as needed)
-- GRANT SELECT ON ALL VIEWS IN SCHEMA public TO ROLE analyst_role;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO ROLE analyst_role;