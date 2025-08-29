FROM python:3.13.3-slim

RUN apt-get update && \
    apt-get install -y git nano vim python3-pip\
    libpcap0.8 libpcap0.8-dev wget\
    adduser libfontconfig1 musl\
    net-tools iputils-ping  tcpdump socat && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip


# Download and install Prometheus
RUN wget https://github.com/prometheus/prometheus/releases/download/v2.49.1/prometheus-2.49.1.linux-amd64.tar.gz && \
    tar -xvf prometheus-2.49.1.linux-amd64.tar.gz && \
    cp prometheus-2.49.1.linux-amd64/prometheus /usr/local/bin/ && \
    cp prometheus-2.49.1.linux-amd64/promtool /usr/local/bin/

# Install Kafka pre-requisites
RUN apt-get update && \
    apt-get install -y default-jre default-jdk && \
    rm -rf /var/lib/apt/lists/*

# Download Kafka binaries
RUN wget https://archive.apache.org/dist/kafka/3.6.1/kafka_2.13-3.6.1.tgz && \
    tar -xzf kafka_2.13-3.6.1.tgz && \
    rm kafka_2.13-3.6.1.tgz && \
    mv kafka_2.13-3.6.1 /opt/kafka

# Download and install Grafana
RUN wget https://dl.grafana.com/oss/release/grafana_10.3.1_amd64.deb && \
    dpkg -i grafana_10.3.1_amd64.deb
    
# Expose the Prometheus port
EXPOSE 9090
# Expose Kafka port
EXPOSE 9092
# Expose Grafana port
EXPOSE 3000

# Set environment variables
ENV PATH="/opt/kafka/bin:${PATH}"

# Clean up unnecessary files
RUN rm -rf prometheus-2.34.0.linux-amd64.tar.gz prometheus-2.34.0.linux-amd64
RUN rm -rf kafka_2.13-3.6.1.tgz

ARG CACHE_BUST=1
RUN git clone https://github.com/DISTA-IoT/smartville-monitor.git /monitor

WORKDIR /monitor

RUN pip install -r requirements.txt


