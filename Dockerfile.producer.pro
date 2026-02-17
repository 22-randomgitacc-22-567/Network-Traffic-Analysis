FROM python:3.9-slim

# Install system dependencies for packet capture
RUN apt-get update && apt-get install -y \
    tshark \
    libpcap-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyshark \
    kafka-python

# Copy application code
COPY packet_producer.py /app/

# Run the producer
CMD ["python", "packet_producer.py"]