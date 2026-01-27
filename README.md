# Real-Time Network Analytics Pipeline

A complete streaming pipeline for capturing network packets from Wireshark, processing them with Spark Structured Streaming, and loading them into Snowflake or Databricks for real-time analytics.

## Architecture

```
Wireshark/TShark -> Kafka -> Spark Structured Streaming -> Snowflake/Databricks -> Real-time Analytics
```

### Components

1. **Packet Capture**: Uses PyShark (TShark wrapper) to capture network packets
2. **Message Queue**: Apache Kafka for reliable data streaming
3. **Stream Processing**: Spark Structured Streaming for transformations and aggregations
4. **Data Warehouse**: Snowflake or Databricks Delta Lake for analytics

## Features

-  Real-time packet capture from network interfaces
-  Support for reading from existing PCAP files
-  Kafka-based streaming for reliability and scalability
-  Spark Structured Streaming with windowed aggregations
-  Support for both Snowflake and Databricks as targets
-  Protocol identification (HTTP, HTTPS, DNS, SSH, etc.)
-  Traffic anomaly detection
-  Pre-built analytics views and dashboards

## Prerequisites

- Docker and Docker Compose (for local setup)
- Python 3.8+
- Apache Spark 3.5+ (included in Docker setup)
- Snowflake account OR Databricks workspace
- Network access for packet capture (requires elevated permissions)

## Quick Start

### 1. Clone and Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# For packet capture (Linux/Mac)
# You may need to run with sudo for packet capture
sudo apt-get install tshark  # Ubuntu/Debian
brew install wireshark       # macOS
```

### 2. Start Infrastructure

```bash
# Start Kafka, Zookeeper, and Spark
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 3. Configure Database

#### For Snowflake:
```bash
# Run the Snowflake schema script
snowsql -c your_connection -f snowflake_schema.sql

# Update credentials in spark_streaming_job.py
```

#### For Databricks:
```bash
# Run the Databricks schema script in a Databricks notebook
# Or use databricks-sql-cli
databricks-sql -e "$(cat databricks_schema.sql)"
```

### 4. Start Packet Capture

```bash
# Option 1: Live capture (requires root/sudo)
sudo python packet_producer.py

# Option 2: Using Docker (network_mode: host required)
docker-compose up packet-producer

# Option 3: Read from PCAP file
python packet_producer.py --pcap-file /path/to/capture.pcap
```

### 5. Start Spark Streaming Job

#### For Databricks:
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  spark_streaming_job.py
```

#### For Snowflake:
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3 \
  spark_streaming_job.py
```

## Configuration

### Packet Producer Configuration

Edit `packet_producer.py`:

```python
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'network-packets'
NETWORK_INTERFACE = 'eth0'  # or 'any' for all interfaces

# Optional: Filter specific traffic
DISPLAY_FILTER = 'tcp.port == 80 or tcp.port == 443'  # HTTP/HTTPS only
# DISPLAY_FILTER = 'ip.src == 192.168.1.0/24'        # Specific subnet
```

### Spark Streaming Configuration

Edit `spark_streaming_job.py`:

```python
# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "network-packets"

# Snowflake settings (if using Snowflake)
SNOWFLAKE_OPTIONS = {
    "sfURL": "your-account.snowflakecomputing.com",
    "sfUser": "your-username",
    "sfPassword": "your-password",
    "sfDatabase": "network_analytics",
    "sfSchema": "public",
    "sfWarehouse": "COMPUTE_WH"
}

# Databricks settings (if using Databricks)
RAW_TABLE = "network_analytics.raw_packets"
AGG_TABLE = "network_analytics.traffic_stats"
```

## Data Schema

### Raw Packets Table

| Column | Type | Description |
|--------|------|-------------|
| packet_timestamp | TIMESTAMP | When packet was captured |
| src_ip | STRING | Source IP address |
| dst_ip | STRING | Destination IP address |
| src_port | INT | Source port |
| dst_port | INT | Destination port |
| protocol | STRING | Protocol (TCP/UDP) |
| length | INT | Packet size in bytes |
| service | STRING | Identified service (HTTP, HTTPS, DNS, etc.) |
| is_outbound | BOOLEAN | Whether traffic is outbound from private network |

### Aggregated Traffic Statistics

| Column | Type | Description |
|--------|------|-------------|
| window_start | TIMESTAMP | Start of 1-minute window |
| window_end | TIMESTAMP | End of window |
| src_ip | STRING | Source IP |
| dst_ip | STRING | Destination IP |
| service | STRING | Service type |
| packet_count | BIGINT | Number of packets |
| total_bytes | BIGINT | Total bytes transferred |
| avg_packet_size | DOUBLE | Average packet size |

## Analytics Queries

### Top Traffic Sources
```sql
SELECT src_ip, dst_ip, service, 
       SUM(total_bytes) as total_traffic,
       SUM(packet_count) as total_packets
FROM traffic_stats
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY src_ip, dst_ip, service
ORDER BY total_traffic DESC
LIMIT 20;
```

### Detect Traffic Anomalies
```sql
-- Use the pre-built view
SELECT * FROM traffic_anomalies
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY z_score DESC;
```

### Service Distribution
```sql
SELECT service, 
       COUNT(*) as connection_count,
       SUM(total_bytes) as total_bytes
FROM traffic_stats
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY service
ORDER BY total_bytes DESC;
```

## Monitoring

### Spark Streaming UI
- Master: http://localhost:8080
- Worker: http://localhost:8081
- Application UI: http://localhost:4040 (when job is running)

### Check Streaming Status
```python
# In spark shell or notebook
spark.streams.active  # List active streams
spark.streams.get(stream_id).status  # Get specific stream status
```

### Kafka Monitoring
```bash
# List topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Check consumer lag
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group spark-streaming-group
```

## Performance Tuning

### Kafka
- Increase `maxOffsetsPerTrigger` for higher throughput
- Adjust `kafka.fetch.max.bytes` for larger messages
- Use multiple partitions for the topic

### Spark
- Tune `spark.sql.shuffle.partitions` based on data volume
- Adjust trigger interval (`processingTime`)
- Increase worker memory and cores
- Use appropriate watermark delays for late data

### Database
- **Snowflake**: Use appropriate warehouse size (X-Small to X-Large)
- **Databricks**: Enable auto-optimization for Delta tables
- Partition tables by date for better query performance
- Run OPTIMIZE and VACUUM periodically

## Troubleshooting

### Issue: "Permission denied" when capturing packets
**Solution**: Run with sudo or add capabilities:
```bash
sudo setcap cap_net_raw,cap_net_admin=eip /usr/bin/dumpcap
```

### Issue: Kafka connection refused
**Solution**: Ensure Kafka is running and accessible:
```bash
docker-compose logs kafka
```

### Issue: Spark streaming job fails with OOM
**Solution**: Increase memory allocation:
```bash
spark-submit --driver-memory 4g --executor-memory 4g ...
```

### Issue: No data appearing in target table
**Solution**: Check:
1. Kafka topic has data: `kafka-console-consumer --topic network-packets`
2. Spark streaming query is running: Check Spark UI
3. Database credentials are correct
4. Checkpoint location is writable

## Advanced Features

### Custom Filters

Add custom packet filters in `packet_producer.py`:
```python
# Capture only HTTPS traffic to specific domain
DISPLAY_FILTER = 'ssl.handshake.type == 1 and ssl.handshake.extensions_server_name contains "example.com"'
```

### Additional Transformations

Add custom transformations in `spark_streaming_job.py`:
```python
def enrich_with_geolocation(df):
    # Add geo IP lookup
    # Add threat intelligence feeds
    # Calculate additional metrics
    return enriched_df
```

### Machine Learning Integration

```python
# Add anomaly detection model
from pyspark.ml import PipelineModel

model = PipelineModel.load("/path/to/model")
predictions = model.transform(transformed_packets)
```

## Security Considerations

- Packet capture requires elevated permissions
- Use encryption for Kafka (SSL/TLS) in production
- Store database credentials in environment variables or secrets manager
- Implement proper network segmentation
- Log access to sensitive packet data
- Mask or redact PII in captured data

## Production Deployment

1. Use managed Kafka service (Confluent Cloud, AWS MSK)
2. Deploy Spark on Databricks or EMR
3. Use IAM roles instead of hardcoded credentials
4. Implement monitoring and alerting (Prometheus, Grafana)
5. Set up automated backup and disaster recovery
6. Use infrastructure as code (Terraform, CloudFormation)

## License

This project is provided as-is for educational and commercial purposes.

## Contributing

Contributions welcome! Please submit pull requests or open issues for bugs and feature requests.

## Support

For questions or issues:
- Check the troubleshooting section
- Review Spark Structured Streaming documentation
- Check Kafka consumer logs
- Verify database connectivity

## Roadmap

- Add support for more protocols (MQTT, AMQP)
- Implement ML-based anomaly detection
- Add Grafana dashboard templates
- [ ] Support for Apache Pulsar
- [ ] GeoIP enrichment
- [ ] Threat intelligence integration
