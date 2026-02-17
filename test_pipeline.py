#!/usr/bin/env python3
"""
Test script to verify the streaming pipeline components using confluent-kafka
"""

import json
import time
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_kafka_connection(bootstrap_servers='localhost:9092'):
    """Test Kafka connectivity"""
    print("Testing Kafka connection...")
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        metadata = admin_client.list_topics(timeout=5)
        print(f"✓ Kafka is accessible. Found {len(metadata.topics)} topics.")
        return True
    except Exception as e:
        print(f"✗ Kafka connection failed: {e}")
        return False


def create_test_topic(bootstrap_servers='localhost:9092', topic_name='network-packets'):
    """Create test topic if it doesn't exist"""
    print(f"Creating topic '{topic_name}'...")
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        
        # Check if topic exists
        metadata = admin_client.list_topics(timeout=5)
        if topic_name in metadata.topics:
            print(f"✓ Topic '{topic_name}' already exists.")
            return True
        
        # Create topic
        topic = NewTopic(
            topic=topic_name,
            num_partitions=3,
            replication_factor=1
        )
        
        fs = admin_client.create_topics([topic])
        
        # Wait for topic creation
        for topic, f in fs.items():
            try:
                f.result()
                print(f"✓ Topic '{topic}' created successfully.")
            except Exception as e:
                print(f"✗ Failed to create topic {topic}: {e}")
                return False
        
        return True
    except Exception as e:
        print(f"✗ Failed to create topic: {e}")
        return False


def delivery_report(err, msg):
    """Callback for producer delivery reports"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def send_test_packets(bootstrap_servers='localhost:9092', topic_name='network-packets', count=10):
    """Send test packet data to Kafka"""
    print(f"Sending {count} test packets to Kafka...")
    
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'test-producer'
    }
    
    producer = Producer(producer_config)
    
    test_packets = []
    for i in range(count):
        packet = {
            'timestamp': f'2024-01-01 12:00:{i:02d}',
            'capture_timestamp': datetime.utcnow().isoformat(),
            'length': 100 + i * 10,
            'src_ip': f'192.168.1.{i % 255}',
            'dst_ip': f'8.8.8.{i % 255}',
            'protocol': 'TCP',
            'ttl': 64,
            'src_port': 50000 + i,
            'dst_port': 443 if i % 2 == 0 else 80,
            'tcp_flags': '0x002',
            'transport': 'TCP'
        }
        test_packets.append(packet)
        
        try:
            # Produce message
            producer.produce(
                topic_name,
                key=str(i),
                value=json.dumps(packet),
                callback=delivery_report
            )
            producer.poll(0)  # Trigger delivery callbacks
            print(f"  ✓ Sent packet {i+1}/{count}")
        except BufferError:
            print(f"  ⚠ Buffer full, waiting...")
            producer.poll(1)
            producer.produce(topic_name, key=str(i), value=json.dumps(packet))
        except Exception as e:
            print(f"  ✗ Failed to send packet {i+1}: {e}")
    
    # Wait for all messages to be delivered
    producer.flush(timeout=10)
    print(f"✓ Sent {len(test_packets)} test packets.")
    return True


def consume_test_packets(bootstrap_servers='localhost:9092', topic_name='network-packets', max_messages=5):
    """Consume and display test packets from Kafka"""
    print(f"Consuming up to {max_messages} messages from Kafka...")
    
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'test-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    consumer = Consumer(consumer_config)
    
    try:
        consumer.subscribe([topic_name])
        
        message_count = 0
        start_time = time.time()
        timeout = 10  # seconds
        
        while message_count < max_messages and (time.time() - start_time) < timeout:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"\n  Reached end of partition")
                    break
                else:
                    print(f"\n  ✗ Consumer error: {msg.error()}")
                    break
            
            # Successfully received message
            try:
                value = json.loads(msg.value().decode('utf-8'))
                print(f"\n  Message {message_count + 1}:")
                print(f"    Partition: {msg.partition()}, Offset: {msg.offset()}")
                print(f"    Data: {json.dumps(value, indent=6)}")
                message_count += 1
            except Exception as e:
                print(f"\n  ✗ Error parsing message: {e}")
        
        consumer.close()
        
        if message_count > 0:
            print(f"\n✓ Successfully consumed {message_count} messages.")
            return True
        else:
            print("\n⚠ No messages found in topic.")
            return False
            
    except Exception as e:
        print(f"✗ Failed to consume messages: {e}")
        consumer.close()
        return False


def check_spark_master(spark_master_url='http://localhost:8080'):
    """Check if Spark master is accessible"""
    print("Checking Spark master...")
    try:
        import requests
        response = requests.get(spark_master_url, timeout=5)
        if response.status_code == 200:
            print("✓ Spark master is accessible.")
            return True
        else:
            print(f"⚠ Spark master returned status code: {response.status_code}")
            return False
    except ImportError:
        print("⚠ requests library not installed, skipping Spark check")
        print("  Install with: pip3 install requests")
        return None
    except Exception as e:
        print(f"✗ Cannot reach Spark master: {e}")
        return False


def run_all_tests():
    """Run all pipeline tests"""
    print("="*60)
    print("Network Analytics Pipeline - Component Tests")
    print("="*60)
    print()
    
    results = {}
    
    # Test 1: Kafka connectivity
    results['kafka_connection'] = test_kafka_connection()
    print()
    
    if results['kafka_connection']:
        # Test 2: Create topic
        results['topic_creation'] = create_test_topic()
        print()
        
        if results['topic_creation']:
            # Test 3: Send test data
            results['send_data'] = send_test_packets(count=10)
            print()
            
            # Small delay to allow messages to be written
            print("Waiting 2 seconds for messages to be committed...")
            time.sleep(2)
            
            # Test 4: Consume test data
            results['consume_data'] = consume_test_packets(max_messages=5)
            print()
    
    # Test 5: Check Spark (optional)
    print("Checking optional components...")
    results['spark_master'] = check_spark_master()
    print()
    
    # Summary
    print("="*60)
    print("Test Summary")
    print("="*60)
    for test_name, result in results.items():
        if result is None:
            status = "⊘ SKIP"
        elif result:
            status = "✓ PASS"
        else:
            status = "✗ FAIL"
        print(f"{test_name:20s}: {status}")
    
    print()
    all_critical_passed = all([
        results.get('kafka_connection', False),
        results.get('topic_creation', False),
        results.get('send_data', False),
        results.get('consume_data', False)
    ])
    
    if all_critical_passed:
        print("✓ All critical tests passed! Pipeline is ready.")
        print("\nNext steps:")
        print("1. Configure your database in .env file")
        print("2. Start packet capture: sudo python3 packet_producer.py")
        print("3. Start Spark streaming: spark-submit spark_streaming_job.py")
        print("4. Check your database for incoming data")
    else:
        print("✗ Some tests failed. Please check the errors above.")
        print("\nTroubleshooting:")
        print("- Ensure Docker containers are running: docker compose ps")
        print("- Check Kafka logs: docker compose logs kafka")
        print("- Verify network connectivity")


if __name__ == "__main__":
    run_all_tests()