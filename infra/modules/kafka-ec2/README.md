# Kafka EC2 Cluster Module

This module creates a Docker-based Kafka cluster running on EC2 instances.

## Features

- **3-node Kafka cluster** with Zookeeper ensemble
- **Docker-based deployment** using Docker Compose
- **Kafka Connect** installed and configured
- **Kafka UI (Kafka Drop)** for cluster management
- **Open security groups** - allows all inbound/outbound traffic
- **Auto-startup** - Kafka cluster starts automatically on instance boot

## Architecture

Each EC2 instance runs:
- 3 Zookeeper nodes (ports 2181-2183)
- 3 Kafka brokers (ports 9092, 9094, 9096)
- 1 Kafka Connect instance (port 8083)
- 1 Kafka UI instance (port 8080)

## Usage

```hcl
module "kafka_ec2" {
  source = "./modules/kafka-ec2"

  project     = "f1-streaming-graph"
  environment = "dev"
  vpc_id      = "vpc-xxxxx"
  subnet_ids  = ["subnet-xxxxx", "subnet-yyyyy", "subnet-zzzzz"]

  instance_type = "t3.medium"
  node_count    = 3
  ec2_key_name  = "my-key-pair"

  tags = {
    Environment = "dev"
  }
}
```

## Outputs

- `kafka_bootstrap_servers` - Comma-separated list of bootstrap servers
- `kafka_ui_urls` - List of Kafka UI URLs (http://ip:8080)
- `kafka_connect_urls` - List of Kafka Connect REST API URLs (http://ip:8083)
- `instance_public_ips` - List of public IP addresses
- `instance_private_ips` - List of private IP addresses

## Accessing Services

### Kafka UI (Kafka Drop)
Access at: `http://<instance-public-ip>:8080`

### Kafka Connect REST API
Access at: `http://<instance-public-ip>:8083`

### Kafka Brokers
Connect using bootstrap servers: `<ip1>:9092,<ip2>:9094,<ip3>:9096`

## Security

**Warning**: This module creates security groups that allow ALL inbound traffic (0.0.0.0/0). This is suitable for development/testing but should be restricted for production use.

## Ports

- **9092, 9094, 9096** - Kafka brokers
- **2181-2183** - Zookeeper
- **8080** - Kafka UI
- **8083** - Kafka Connect REST API
- **9101-9103** - JMX ports

## Management Scripts

After SSH'ing into an instance, you can use:
- `/home/ec2-user/start-kafka.sh` - Start Kafka cluster
- `/home/ec2-user/stop-kafka.sh` - Stop Kafka cluster
- `sudo docker-compose -f /home/ec2-user/kafka/docker-compose.yml ps` - Check status

