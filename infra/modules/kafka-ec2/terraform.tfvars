# Terraform variables for Kafka EC2 module
# This file is used when running terraform directly in this module directory
# Update VPC ID and subnet IDs to match your AWS environment

project     = "data228-project"
environment = "dev"
vpc_id      = "vpc-09cf1d42cfe5fc374"
subnet_ids  = ["subnet-0cb79f738d3f87973", "subnet-0ad1d1c4c2b456d83", "subnet-0f6e6b4eddfcf67ac"]

instance_type = "t3.medium"
node_count    = 3
ec2_key_name  = ""  # Optional: provide key pair name for SSH access (leave empty to auto-create)
ssh_public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCYfBpG3AALrHIfayUaIHoxbZByyEqTlNJdeLLdbW64K8DnQGY1ra8gdcx/AoMzjmP2UKJwTaigHoH3mKuUeb7LxX74/Pd2JqAyCMrJtrW/Ll20ApcxrxXI5cUM1mEcsFTcAh9fnkq+boV/NXKnWBkHacc/gBq9Ij6rHsCfObBQLjWCms1TG8jqAj6hxr4fuvrFWgpUUiHHVidHaGd38B10bCpaLYR/O/Z/moRCcvJDdsAJNwHvZcgMluZK33b7RtU/9fHHzbm69qfJXU7cco4wnJVhEa6gVXMr7dM6ExrVaflhdB/D/nvfWYrOGcnIY2H8OK9/w+2KOpELj/VofcnIEuFqVoDuZ9Ur7kFhBvyChdves95Xswgh+ThvSYJqPUeWIlTz/vzuhp1C4tWwvEcxY4utjezUZ6uwKifByDLkvgKkM4baHymhYXAIqY5gcxzjw+RyiFzJz/7bdicnCGMGwYalWfAACImdAZ/pV2w5rW44PfTEXZMie7CaAOZGd4k= hiruzen@Shravankumars-MacBook-Air.local"

allowed_cidr_blocks = ["0.0.0.0/0"]  # Allow all IPs (change for production)

tags = {
  Team        = "big-data-team"
  CostCenter  = "engineering"
  Application = "kafka-cluster"
}

