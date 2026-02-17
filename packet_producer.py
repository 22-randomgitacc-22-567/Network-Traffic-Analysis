"""
Wireshark/TShark to Kafka Producer
Captures network packets and streams them to Kafka
"""

import pyshark
import json
from kafka import KafkaProducer
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PacketProducer:
    def __init__(self, kafka_bootstrap_servers, kafka_topic, interface='eth0'):
        """
        Initialize the packet producer
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses (e.g., 'localhost:9092')
            kafka_topic: Topic to publish packets to
            interface: Network interface to capture from (or use 'any' for all)
        """
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='gzip'
        )
        self.topic = kafka_topic
        self.interface = interface
        
    def extract_packet_info(self, packet):
        """
        Extract relevant information from a packet
        """
        try:
            packet_data = {
                'timestamp': str(packet.sniff_time),
                'length': int(packet.length),
                'capture_timestamp': datetime.utcnow().isoformat()
            }
            
            # IP Layer information
            if hasattr(packet, 'ip'):
                packet_data.update({
                    'src_ip': packet.ip.src,
                    'dst_ip': packet.ip.dst,
                    'protocol': packet.ip.proto,
                    'ttl': int(packet.ip.ttl)
                })
            
            # TCP Layer information
            if hasattr(packet, 'tcp'):
                packet_data.update({
                    'src_port': int(packet.tcp.srcport),
                    'dst_port': int(packet.tcp.dstport),
                    'tcp_flags': str(packet.tcp.flags),
                    'transport': 'TCP'
                })
            
            # UDP Layer information
            elif hasattr(packet, 'udp'):
                packet_data.update({
                    'src_port': int(packet.udp.srcport),
                    'dst_port': int(packet.udp.dstport),
                    'transport': 'UDP'
                })
            
            # DNS information
            if hasattr(packet, 'dns'):
                if hasattr(packet.dns, 'qry_name'):
                    packet_data['dns_query'] = packet.dns.qry_name
            
            # HTTP information
            if hasattr(packet, 'http'):
                if hasattr(packet.http, 'host'):
                    packet_data['http_host'] = packet.http.host
                if hasattr(packet.http, 'request_method'):
                    packet_data['http_method'] = packet.http.request_method
            
            return packet_data
            
        except Exception as e:
            logger.error(f"Error extracting packet info: {e}")
            return None
    
    def start_capture(self, display_filter=None, packet_count=None):
        """
        Start capturing packets and streaming to Kafka
        
        Args:
            display_filter: Wireshark display filter (e.g., 'tcp.port == 80')
            packet_count: Number of packets to capture (None for continuous)
        """
        logger.info(f"Starting packet capture on interface: {self.interface}")
        
        capture = pyshark.LiveCapture(
            interface=self.interface,
            display_filter=display_filter
        )
        
        try:
            for packet in capture.sniff_continuously(packet_count=packet_count):
                packet_data = self.extract_packet_info(packet)
                
                if packet_data:
                    # Send to Kafka
                    self.producer.send(self.topic, value=packet_data)
                    logger.debug(f"Sent packet: {packet_data.get('src_ip', 'N/A')} -> {packet_data.get('dst_ip', 'N/A')}")
                    
        except KeyboardInterrupt:
            logger.info("Capture stopped by user")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed")


# Alternative: Read from existing pcap file
class PcapFileProducer(PacketProducer):
    def __init__(self, kafka_bootstrap_servers, kafka_topic, pcap_file):
        super().__init__(kafka_bootstrap_servers, kafka_topic)
        self.pcap_file = pcap_file
    
    def start_capture(self, display_filter=None):
        """Read from pcap file and stream to Kafka"""
        logger.info(f"Reading from pcap file: {self.pcap_file}")
        
        capture = pyshark.FileCapture(
            self.pcap_file,
            display_filter=display_filter
        )
        
        try:
            for packet in capture:
                packet_data = self.extract_packet_info(packet)
                
                if packet_data:
                    self.producer.send(self.topic, value=packet_data)
                    logger.debug(f"Sent packet from file")
                    
        except Exception as e:
            logger.error(f"Error reading pcap file: {e}")
        finally:
            capture.close()
            self.producer.flush()
            self.producer.close()


if __name__ == "__main__":
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
    KAFKA_TOPIC = 'network-packets'
    NETWORK_INTERFACE = 'eth0'  # Change to 'any' for all interfaces
    
    # Optional: Filter only HTTP traffic
    DISPLAY_FILTER = 'tcp.port == 80 or tcp.port == 443'
    
    # Start live capture
    producer = PacketProducer(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic=KAFKA_TOPIC,
        interface=NETWORK_INTERFACE
    )
    
    producer.start_capture(display_filter=DISPLAY_FILTER)
    
    # Or read from a pcap file:
    # file_producer = PcapFileProducer(
    #     kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    #     kafka_topic=KAFKA_TOPIC,
    #     pcap_file='/path/to/capture.pcap'
    # )
    # file_producer.start_capture()