# Security Group - Allow all inbound and outbound traffic
resource "aws_security_group" "kafka_ec2" {
  name_prefix = "${var.project}-${var.environment}-kafka-ec2-"
  description = "Security group for Kafka EC2 cluster - allows all traffic from/to all IPs"
  vpc_id      = var.vpc_id

  # Allow all inbound traffic from all IPs
  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all inbound traffic from all IPs"
  }

  # Allow all outbound traffic to all IPs
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound traffic to all IPs"
  }

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-kafka-ec2-sg"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# IAM Role for EC2 instances
resource "aws_iam_role" "kafka_ec2" {
  name = "${var.project}-${var.environment}-kafka-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.tags
}

# IAM Policy for EC2 instances (minimal - just for CloudWatch logs)
resource "aws_iam_role_policy" "kafka_ec2" {
  name = "${var.project}-${var.environment}-kafka-ec2-policy"
  role = aws_iam_role.kafka_ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# IAM Instance Profile
resource "aws_iam_instance_profile" "kafka_ec2" {
  name = "${var.project}-${var.environment}-kafka-ec2-profile"
  role = aws_iam_role.kafka_ec2.name

  tags = var.tags
}

# Get latest Amazon Linux 2023 AMI
data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# AWS Key Pair for SSH access
resource "aws_key_pair" "kafka_ec2" {
  count      = var.ec2_key_name == "" ? 1 : 0
  key_name   = "${var.project}-${var.environment}-kafka-ec2-key"
  public_key = var.ssh_public_key

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-kafka-ec2-key"
  })
}

# User data script - each instance runs all services configured as a cluster
locals {
  user_data = <<-EOF
#!/bin/bash
set -e

# Update system
sudo yum update -y

# Install Docker
sudo yum install -y docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -a -G docker ec2-user

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Create Kafka directory
mkdir -p /home/ec2-user/kafka
cd /home/ec2-user/kafka

# Get instance metadata
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
PRIVATE_IP=$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4)
PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 || echo "")

# Determine broker ID from instance tags (1, 2, or 3)
# We'll use a simple approach: get from instance name tag or use a default
BROKER_ID=1
ZOOKEEPER_ID=1

# Use public IP for advertised listeners if available, otherwise use private IP
if [ -z "$PUBLIC_IP" ]; then
  ADVERTISED_HOST="$PRIVATE_IP"
else
  ADVERTISED_HOST="$PUBLIC_IP"
fi

# Create docker-compose.yml - runs all services on each instance
# Use COMPOSE_EOF (no quotes) to allow variable substitution
cat > docker-compose.yml <<COMPOSE_EOF
version: '3.8'

services:
  zookeeper-1:
    image: confluentinc/cp-zookeeper:${var.kafka_version}
    hostname: zookeeper-1
    container_name: zookeeper-1
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
      ZOOKEEPER_SERVER_ID: 1
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888;zookeeper-2:2888:3888;zookeeper-3:2888:3888
    networks:
      - kafka-net
    restart: unless-stopped

  zookeeper-2:
    image: confluentinc/cp-zookeeper:${var.kafka_version}
    hostname: zookeeper-2
    container_name: zookeeper-2
    ports:
      - "2182:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
      ZOOKEEPER_SERVER_ID: 2
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888;zookeeper-2:2888:3888;zookeeper-3:2888:3888
    networks:
      - kafka-net
    restart: unless-stopped

  zookeeper-3:
    image: confluentinc/cp-zookeeper:${var.kafka_version}
    hostname: zookeeper-3
    container_name: zookeeper-3
    ports:
      - "2183:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
      ZOOKEEPER_SERVER_ID: 3
      ZOOKEEPER_SERVERS: zookeeper-1:2888:3888;zookeeper-2:2888:3888;zookeeper-3:2888:3888
    networks:
      - kafka-net
    restart: unless-stopped

  kafka-1:
    image: confluentinc/cp-kafka:${var.kafka_version}
    hostname: kafka-1
    container_name: kafka-1
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3
    ports:
      - "9092:9092"
      - "9093:9093"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:29092,PLAINTEXT_HOST://$${ADVERTISED_HOST}:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    networks:
      - kafka-net
    restart: unless-stopped

  kafka-2:
    image: confluentinc/cp-kafka:${var.kafka_version}
    hostname: kafka-2
    container_name: kafka-2
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3
    ports:
      - "9094:9092"
      - "9095:9093"
      - "9102:9101"
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:29092,PLAINTEXT_HOST://$${ADVERTISED_HOST}:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    networks:
      - kafka-net
    restart: unless-stopped

  kafka-3:
    image: confluentinc/cp-kafka:${var.kafka_version}
    hostname: kafka-3
    container_name: kafka-3
    depends_on:
      - zookeeper-1
      - zookeeper-2
      - zookeeper-3
    ports:
      - "9096:9092"
      - "9097:9093"
      - "9103:9101"
    environment:
      KAFKA_BROKER_ID: 3
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:29092,PLAINTEXT_HOST://$${ADVERTISED_HOST}:9096
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    networks:
      - kafka-net
    restart: unless-stopped

  kafka-connect:
    image: confluentinc/cp-kafka-connect:${var.kafka_version}
    hostname: kafka-connect
    container_name: kafka-connect
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
    ports:
      - "8083:8083"
      - "8084:8084"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka-1:29092,kafka-2:29092,kafka-3:29092
      CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
      CONNECT_REST_PORT: 8083
      CONNECT_GROUP_ID: compose-connect-group
      CONNECT_CONFIG_STORAGE_TOPIC: docker-connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: docker-connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: docker-connect-status
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_INTERNAL_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_INTERNAL_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_REST_ADVERTISED_LISTENERS: http://$${ADVERTISED_HOST}:8083
      CONNECT_PLUGIN_PATH: /usr/share/java,/usr/share/confluent-hub-components
    networks:
      - kafka-net
    restart: unless-stopped

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    hostname: kafka-ui
    container_name: kafka-ui
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-1:29092,kafka-2:29092,kafka-3:29092
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
    networks:
      - kafka-net
    restart: unless-stopped

networks:
  kafka-net:
    driver: bridge
COMPOSE_EOF

# Wait for Docker to be ready
sleep 10

# Export ADVERTISED_HOST for docker-compose
export ADVERTISED_HOST

# Start Kafka cluster
sudo -E docker-compose up -d

# Create startup script
cat > /home/ec2-user/start-kafka.sh <<'SCRIPT_EOF'
#!/bin/bash
cd /home/ec2-user/kafka
sudo docker-compose up -d
SCRIPT_EOF

chmod +x /home/ec2-user/start-kafka.sh

# Create stop script
cat > /home/ec2-user/stop-kafka.sh <<'SCRIPT_EOF'
#!/bin/bash
cd /home/ec2-user/kafka
sudo docker-compose down
SCRIPT_EOF

chmod +x /home/ec2-user/stop-kafka.sh

# Log completion
echo "Kafka cluster setup completed at $(date)" >> /var/log/kafka-setup.log
echo "Instance IP: $PRIVATE_IP" >> /var/log/kafka-setup.log
echo "Public IP: $PUBLIC_IP" >> /var/log/kafka-setup.log
EOF
}

# EC2 Instances for Kafka cluster
resource "aws_instance" "kafka_nodes" {
  count = var.node_count

  ami           = data.aws_ami.amazon_linux.id
  instance_type = var.instance_type
  subnet_id     = var.subnet_ids[count.index % length(var.subnet_ids)]

  vpc_security_group_ids = [aws_security_group.kafka_ec2.id]
  iam_instance_profile   = aws_iam_instance_profile.kafka_ec2.name

  key_name = var.ec2_key_name != "" ? var.ec2_key_name : (length(aws_key_pair.kafka_ec2) > 0 ? aws_key_pair.kafka_ec2[0].key_name : null)

  user_data = local.user_data

  root_block_device {
    volume_type = "gp3"
    volume_size = 50
    encrypted   = true
  }

  tags = merge(var.tags, {
    Name  = "${var.project}-${var.environment}-kafka-node-${count.index + 1}"
    Role  = "kafka"
    Index = count.index + 1
  })

  lifecycle {
    create_before_destroy = true
  }
}
