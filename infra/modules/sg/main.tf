# Security Groups Module - Creates security groups for MSK and EMR

variable "project" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "tags" {
  description = "Resource tags"
  type        = map(string)
}

resource "aws_security_group" "msk" {
  name_prefix = "${var.project}-${var.environment}-msk-"
  description = "Security group for MSK cluster"
  vpc_id      = var.vpc_id

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-msk-sg"
  })
}

resource "aws_security_group" "emr" {
  name_prefix = "${var.project}-${var.environment}-emr-"
  description = "Security group for EMR cluster nodes"
  vpc_id      = var.vpc_id

  tags = merge(var.tags, {
    Name = "${var.project}-${var.environment}-emr-sg"
  })
}

# MSK ingress from EMR
resource "aws_security_group_rule" "msk_from_emr" {
  type                     = "ingress"
  from_port                = 9092
  to_port                  = 9098
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.emr.id
  security_group_id        = aws_security_group.msk.id
  description              = "Allow Kafka access from EMR"
}

# Temporary allowance for external connectivity/testing
resource "aws_security_group_rule" "msk_public_ingress" {
  type              = "ingress"
  from_port         = 9092
  to_port           = 9098
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.msk.id
  description       = "Allow Kafka access from any source (testing)"
}

# EMR egress to MSK
resource "aws_security_group_rule" "emr_to_msk" {
  type                     = "egress"
  from_port                = 9092
  to_port                  = 9098
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.msk.id
  security_group_id        = aws_security_group.emr.id
  description              = "Allow EMR to access MSK"
}

# EMR egress to internet (for AWS services)
resource "aws_security_group_rule" "emr_to_internet" {
  type              = "egress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.emr.id
  description       = "Allow HTTPS egress for AWS services"
}

output "sg_msk_id" {
  description = "MSK security group ID"
  value       = aws_security_group.msk.id
}

output "sg_emr_id" {
  description = "EMR security group ID"
  value       = aws_security_group.emr.id
}
