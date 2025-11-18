output "security_group_id" {
  description = "Security group ID for Kafka EC2 cluster"
  value       = aws_security_group.kafka_ec2.id
}

output "instance_ids" {
  description = "List of EC2 instance IDs"
  value       = aws_instance.kafka_nodes[*].id
}

output "instance_public_ips" {
  description = "List of public IP addresses"
  value       = aws_instance.kafka_nodes[*].public_ip
}

output "instance_private_ips" {
  description = "List of private IP addresses"
  value       = aws_instance.kafka_nodes[*].private_ip
}

output "kafka_bootstrap_servers" {
  description = "Kafka bootstrap servers (comma-separated list of public IPs:9092,9094,9096)"
  value       = join(",", [
    for i, ip in aws_instance.kafka_nodes[*].public_ip : 
    i == 0 ? "${ip}:9092" : (i == 1 ? "${ip}:9094" : "${ip}:9096")
  ])
}

output "kafka_ui_urls" {
  description = "Kafka UI URLs"
  value       = [for ip in aws_instance.kafka_nodes[*].public_ip : "http://${ip}:8080"]
}

output "kafka_connect_urls" {
  description = "Kafka Connect REST API URLs"
  value       = [for ip in aws_instance.kafka_nodes[*].public_ip : "http://${ip}:8083"]
}

output "key_pair_name" {
  description = "Name of the EC2 key pair (if auto-created)"
  value       = var.ec2_key_name == "" ? (length(aws_key_pair.kafka_ec2) > 0 ? aws_key_pair.kafka_ec2[0].key_name : null) : var.ec2_key_name
}

