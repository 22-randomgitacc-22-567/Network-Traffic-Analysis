#!/bin/bash

# Network Analytics Pipeline - Helper Script
# Provides convenient commands for managing the pipeline

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
KAFKA_CONTAINER="kafka"
SPARK_MASTER_CONTAINER="spark-master"
TOPIC_NAME="network-packets"

# Helper functions
print_header() {
    echo -e "${GREEN}============================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}============================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Command functions
start_infrastructure() {
    print_header "Starting Infrastructure"
    docker-compose up -d
    echo ""
    print_success "Infrastructure started"
    echo ""
    docker-compose ps
}

stop_infrastructure() {
    print_header "Stopping Infrastructure"
    docker-compose down
    print_success "Infrastructure stopped"
}

restart_infrastructure() {
    print_header "Restarting Infrastructure"
    docker-compose restart
    print_success "Infrastructure restarted"
}

status_check() {
    print_header "Pipeline Status"
    echo ""
    echo "Docker Containers:"
    docker-compose ps
    echo ""
    
    echo "Kafka Topics:"
    docker exec -it $KAFKA_CONTAINER kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null || print_error "Cannot connect to Kafka"
    echo ""
}

create_topic() {
    print_header "Creating Kafka Topic: $TOPIC_NAME"
    docker exec -it $KAFKA_CONTAINER kafka-topics \
        --create \
        --if-not-exists \
        --bootstrap-server localhost:9092 \
        --topic $TOPIC_NAME \
        --partitions 3 \
        --replication-factor 1
    print_success "Topic created or already exists"
}

delete_topic() {
    print_header "Deleting Kafka Topic: $TOPIC_NAME"
    read -p "Are you sure you want to delete topic '$TOPIC_NAME'? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
        docker exec -it $KAFKA_CONTAINER kafka-topics \
            --delete \
            --bootstrap-server localhost:9092 \
            --topic $TOPIC_NAME
        print_success "Topic deleted"
    else
        print_warning "Operation cancelled"
    fi
}

describe_topic() {
    print_header "Topic Details: $TOPIC_NAME"
    docker exec -it $KAFKA_CONTAINER kafka-topics \
        --describe \
        --bootstrap-server localhost:9092 \
        --topic $TOPIC_NAME
}

consume_messages() {
    print_header "Consuming Messages from: $TOPIC_NAME"
    echo "Press Ctrl+C to stop"
    echo ""
    docker exec -it $KAFKA_CONTAINER kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic $TOPIC_NAME \
        --from-beginning
}

consumer_groups() {
    print_header "Consumer Groups"
    docker exec -it $KAFKA_CONTAINER kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --list
}

consumer_lag() {
    print_header "Consumer Group Lag"
    local group_id=${1:-"spark-streaming-group"}
    docker exec -it $KAFKA_CONTAINER kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --describe \
        --group $group_id
}

test_pipeline() {
    print_header "Running Pipeline Tests"
    python3 test_pipeline.py
}

start_producer() {
    print_header "Starting Packet Producer"
    print_warning "This requires root/sudo access for packet capture"
    echo ""
    read -p "Network interface (default: any): " interface
    interface=${interface:-any}
    
    sudo NETWORK_INTERFACE=$interface python3 packet_producer.py
}

start_spark_job() {
    print_header "Starting Spark Streaming Job"
    echo ""
    echo "Choose target:"
    echo "1) Databricks (Delta Lake)"
    echo "2) Snowflake"
    echo "3) Console (testing)"
    read -p "Enter choice (1-3): " choice
    
    case $choice in
        1)
            print_success "Starting with Databricks target"
            spark-submit \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
                --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
                --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
                spark_streaming_job.py
            ;;
        2)
            print_success "Starting with Snowflake target"
            spark-submit \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3 \
                spark_streaming_job.py
            ;;
        3)
            print_success "Starting with console output (testing)"
            spark-submit \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                spark_streaming_job.py
            ;;
        *)
            print_error "Invalid choice"
            exit 1
            ;;
    esac
}

view_logs() {
    print_header "Container Logs"
    echo ""
    echo "Available containers:"
    echo "1) Kafka"
    echo "2) Zookeeper"
    echo "3) Spark Master"
    echo "4) Spark Worker"
    echo "5) Packet Producer"
    read -p "Enter choice (1-5): " choice
    
    case $choice in
        1) docker-compose logs -f kafka ;;
        2) docker-compose logs -f zookeeper ;;
        3) docker-compose logs -f spark-master ;;
        4) docker-compose logs -f spark-worker ;;
        5) docker-compose logs -f packet-producer ;;
        *) print_error "Invalid choice" ;;
    esac
}

clean_checkpoints() {
    print_header "Cleaning Spark Checkpoints"
    read -p "This will delete checkpoint data. Continue? (yes/no): " confirm
    if [ "$confirm" = "yes" ]; then
        rm -rf /tmp/spark-checkpoint/*
        docker-compose exec spark-master rm -rf /tmp/checkpoints/*
        print_success "Checkpoints cleaned"
    else
        print_warning "Operation cancelled"
    fi
}

setup_environment() {
    print_header "Environment Setup"
    
    # Check Python installation
    if command -v python3 &> /dev/null; then
        print_success "Python3 is installed"
    else
        print_error "Python3 is not installed"
    fi
    
    # Check Docker
    if command -v docker &> /dev/null; then
        print_success "Docker is installed"
    else
        print_error "Docker is not installed"
    fi
    
    # Check Docker Compose
    if command -v docker-compose &> /dev/null; then
        print_success "Docker Compose is installed"
    else
        print_error "Docker Compose is not installed"
    fi
    
    # Install Python dependencies
    echo ""
    read -p "Install Python dependencies? (yes/no): " install_deps
    if [ "$install_deps" = "yes" ]; then
        pip3 install -r requirements.txt
        print_success "Python dependencies installed"
    fi
    
    # Create .env file
    if [ ! -f .env ]; then
        echo ""
        read -p "Create .env file from template? (yes/no): " create_env
        if [ "$create_env" = "yes" ]; then
            cp .env.template .env
            print_success ".env file created - please edit with your settings"
        fi
    else
        print_warning ".env file already exists"
    fi
}

show_help() {
    echo "Network Analytics Pipeline - Helper Script"
    echo ""
    echo "Usage: ./pipeline.sh [command]"
    echo ""
    echo "Infrastructure Management:"
    echo "  start              Start all containers"
    echo "  stop               Stop all containers"
    echo "  restart            Restart all containers"
    echo "  status             Show pipeline status"
    echo "  logs               View container logs"
    echo ""
    echo "Kafka Operations:"
    echo "  create-topic       Create the network-packets topic"
    echo "  delete-topic       Delete the network-packets topic"
    echo "  describe-topic     Show topic details"
    echo "  consume            Consume messages from topic"
    echo "  consumer-groups    List consumer groups"
    echo "  consumer-lag       Show consumer group lag"
    echo ""
    echo "Pipeline Operations:"
    echo "  test               Run pipeline component tests"
    echo "  start-producer     Start packet capture producer"
    echo "  start-spark        Start Spark streaming job"
    echo "  clean-checkpoints  Clean Spark checkpoint data"
    echo ""
    echo "Setup:"
    echo "  setup              Setup environment and dependencies"
    echo "  help               Show this help message"
    echo ""
}

# Main script logic
case "$1" in
    start)
        start_infrastructure
        ;;
    stop)
        stop_infrastructure
        ;;
    restart)
        restart_infrastructure
        ;;
    status)
        status_check
        ;;
    create-topic)
        create_topic
        ;;
    delete-topic)
        delete_topic
        ;;
    describe-topic)
        describe_topic
        ;;
    consume)
        consume_messages
        ;;
    consumer-groups)
        consumer_groups
        ;;
    consumer-lag)
        consumer_lag "$2"
        ;;
    test)
        test_pipeline
        ;;
    start-producer)
        start_producer
        ;;
    start-spark)
        start_spark_job
        ;;
    logs)
        view_logs
        ;;
    clean-checkpoints)
        clean_checkpoints
        ;;
    setup)
        setup_environment
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac