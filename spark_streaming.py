"""
Spark Structured Streaming Job for Network Packet Processing
Reads from Kafka, transforms data, and writes to Snowflake/Databricks
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os


# Define schema for incoming packet data
packet_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("length", IntegerType(), True),
    StructField("capture_timestamp", StringType(), True),
    StructField("src_ip", StringType(), True),
    StructField("dst_ip", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("ttl", IntegerType(), True),
    StructField("src_port", IntegerType(), True),
    StructField("dst_port", IntegerType(), True),
    StructField("tcp_flags", StringType(), True),
    StructField("transport", StringType(), True),
    StructField("dns_query", StringType(), True),
    StructField("http_host", StringType(), True),
    StructField("http_method", StringType(), True)
])


def create_spark_session(app_name="NetworkPacketStreaming"):
    """Create Spark session with necessary configurations"""
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .config("spark.sql.shuffle.partitions", "4")
    
    # Add Snowflake connector (if using Snowflake)
    # builder = builder.config("spark.jars.packages", 
    #     "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3")
    
    return builder.getOrCreate()


def read_from_kafka(spark, kafka_bootstrap_servers, kafka_topic):
    """Read streaming data from Kafka"""
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .load()
    
    # Parse the JSON value from Kafka
    parsed_df = df.select(
        from_json(col("value").cast("string"), packet_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    return parsed_df


def transform_packet_data(df):
    """Apply transformations to packet data"""
    
    # Convert timestamp strings to timestamp type
    transformed_df = df \
        .withColumn("packet_timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("capture_timestamp", to_timestamp(col("capture_timestamp"))) \
        .withColumn("processing_time", current_timestamp())
    
    # Add derived columns
    transformed_df = transformed_df \
        .withColumn("is_outbound", 
                   when(col("src_ip").startswith("192.168."), True)
                   .when(col("src_ip").startswith("10."), True)
                   .otherwise(False)) \
        .withColumn("packet_size_category",
                   when(col("length") < 100, "small")
                   .when(col("length") < 1000, "medium")
                   .otherwise("large"))
    
    # Extract IP class
    transformed_df = transformed_df \
        .withColumn("src_ip_class",
                   when(col("src_ip").startswith("10."), "A")
                   .when(col("src_ip").startswith("172."), "B")
                   .when(col("src_ip").startswith("192.168."), "C")
                   .otherwise("Public"))
    
    # Add common service identification based on port
    transformed_df = transformed_df \
        .withColumn("service",
                   when(col("dst_port") == 80, "HTTP")
                   .when(col("dst_port") == 443, "HTTPS")
                   .when(col("dst_port") == 53, "DNS")
                   .when(col("dst_port") == 22, "SSH")
                   .when(col("dst_port") == 21, "FTP")
                   .when(col("dst_port") == 25, "SMTP")
                   .when(col("dst_port") == 3306, "MySQL")
                   .when(col("dst_port") == 5432, "PostgreSQL")
                   .otherwise("Other"))
    
    return transformed_df


def create_aggregations(df):
    """Create windowed aggregations for analytics"""
    
    # 1-minute tumbling window aggregations
    traffic_stats = df \
        .withWatermark("packet_timestamp", "5 minutes") \
        .groupBy(
            window(col("packet_timestamp"), "1 minute"),
            col("src_ip"),
            col("dst_ip"),
            col("service")
        ) \
        .agg(
            count("*").alias("packet_count"),
            sum("length").alias("total_bytes"),
            avg("length").alias("avg_packet_size"),
            max("length").alias("max_packet_size"),
            min("length").alias("min_packet_size")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("src_ip"),
            col("dst_ip"),
            col("service"),
            col("packet_count"),
            col("total_bytes"),
            col("avg_packet_size"),
            col("max_packet_size"),
            col("min_packet_size"),
            current_timestamp().alias("processed_at")
        )
    
    return traffic_stats


def write_to_snowflake(df, snowflake_options, table_name):
    """Write stream to Snowflake"""
    
    query = df \
        .writeStream \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", table_name) \
        .option("streaming_stage", "streaming_stage") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    return query


def write_to_databricks(df, table_name, checkpoint_location):
    """Write stream to Databricks Delta table"""
    
    query = df \
        .writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime='10 seconds') \
        .toTable(table_name)
    
    return query


def write_to_console(df, output_mode="append", truncate=False):
    """Write to console for debugging"""
    
    query = df \
        .writeStream \
        .format("console") \
        .outputMode(output_mode) \
        .option("truncate", truncate) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    return query


def main():
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "network-packets"
    
    # Snowflake configuration (if using Snowflake)
    SNOWFLAKE_OPTIONS = {
        "sfURL": "your-account.snowflakecomputing.com",
        "sfUser": "your-username",
        "sfPassword": "your-password",
        "sfDatabase": "network_analytics",
        "sfSchema": "public",
        "sfWarehouse": "COMPUTE_WH"
    }
    
    # Databricks configuration
    RAW_TABLE = "network_analytics.raw_packets"
    AGG_TABLE = "network_analytics.traffic_stats"
    CHECKPOINT_RAW = "/tmp/checkpoints/raw_packets"
    CHECKPOINT_AGG = "/tmp/checkpoints/traffic_stats"
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Starting Spark Structured Streaming job...")
    
    # Read from Kafka
    raw_packets = read_from_kafka(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    
    # Transform data
    transformed_packets = transform_packet_data(raw_packets)
    
    # Create aggregations
    aggregated_traffic = create_aggregations(transformed_packets)
    
    # Write raw packets to target (choose one)
    # Option 1: Databricks Delta Lake
    raw_query = write_to_databricks(
        transformed_packets, 
        RAW_TABLE, 
        CHECKPOINT_RAW
    )
    
    # Option 2: Snowflake
    # raw_query = write_to_snowflake(
    #     transformed_packets,
    #     SNOWFLAKE_OPTIONS,
    #     "raw_packets"
    # )
    
    # Option 3: Console (for testing)
    # raw_query = write_to_console(transformed_packets)
    
    # Write aggregated data
    agg_query = write_to_databricks(
        aggregated_traffic,
        AGG_TABLE,
        CHECKPOINT_AGG
    )
    
    print("Streaming queries started. Waiting for data...")
    print(f"Raw packets -> {RAW_TABLE}")
    print(f"Aggregated traffic -> {AGG_TABLE}")
    
    # Wait for termination
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()