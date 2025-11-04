#!/usr/bin/env python3
"""
AWS Resource Discovery for F1 Streaming Graph Infrastructure
Discovers existing AWS resources to avoid duplication and generates Terraform variables.
"""

import boto3
import json
import os
import sys
from typing import Dict, List
import argparse
import subprocess
from subprocess import CalledProcessError


class AWSResourceDiscovery:
    """Discovers existing AWS resources for the streaming pipeline."""
    
    def __init__(self, project: str, region: str, debug: bool = False):
        self.project = project
        self.region = region
        self.debug = debug
        self.use_awscli = False
        self.prefer_existing = True
        
        # Initialize AWS clients
        self.ec2 = boto3.client('ec2', region_name=region)
        self.s3 = boto3.client('s3', region_name=region)
        self.kms = boto3.client('kms', region_name=region)
        self.kafka = boto3.client('kafka', region_name=region)
        
        self.discovered = {
            'vpc_id': '',
            'private_subnet_ids': [],
            'security_group_ids': {},
            'existing_bucket_names': {
                'checkpoints': '',
                'artifacts': '',
                'raw': ''
            },
            'existing_kms_keys': {
                'data': '',
                'msk': ''
            },
            'existing_msk_cluster_arn': ''
        }
    
    def log(self, message: str, force: bool = False):
        """Print debug message if debug mode is enabled."""
        if self.debug or force:
            print(f"[DEBUG] {message}")
    
    def discover_vpc(self) -> bool:
        """Discover existing VPC and subnets."""
        self.log("Discovering VPC...")
        
        # Check environment variable first
        vpc_id_env = os.environ.get('VPC_ID')
        if vpc_id_env:
            self.log(f"Using VPC from environment: {vpc_id_env}")
            self.discovered['vpc_id'] = vpc_id_env
            return self._discover_subnets(vpc_id_env)
        
        # Find VPCs tagged with project name
        try:
            response = self.ec2.describe_vpcs(
                Filters=[
                    {'Name': 'tag:Project', 'Values': [self.project]},
                    {'Name': 'state', 'Values': ['available']}
                ]
            )
            
            if response['Vpcs']:
                vpc = response['Vpcs'][0]
                vpc_id = vpc['VpcId']
                self.log(f"Found VPC tagged with project: {vpc_id}")
                self.discovered['vpc_id'] = vpc_id
                return self._discover_subnets(vpc_id)
        except Exception as e:
            self.log(f"Error searching for tagged VPC: {e}")
        
        # Fall back to largest VPC with subnets (including default VPC)
        try:
            if self.use_awscli:
                self.log("Using AWS CLI to describe VPCs")
                out = self._run_awscli(["ec2", "describe-vpcs", "--filters", "Name=state,Values=available", "--output", "json"])
                response = json.loads(out)
            else:
                response = self.ec2.describe_vpcs(
                    Filters=[{'Name': 'state', 'Values': ['available']}]
                )
            
            candidates = []
            for vpc in response['Vpcs']:
                vpc_id = vpc['VpcId']
                is_default = vpc.get('IsDefault', False)
                
                # Count all subnets
                subnet_response = self.ec2.describe_subnets(
                    Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
                )
                
                total_subnets = len(subnet_response['Subnets'])
                
                if total_subnets >= 1:
                    cidr_block = vpc.get('CidrBlock', '')
                    cidr_size = int(cidr_block.split('/')[1]) if cidr_block else 32
                    # Prefer default VPC, then by largest CIDR
                    priority = 0 if is_default else 1
                    candidates.append((vpc_id, priority, cidr_size, total_subnets, is_default))
                    vpc_type = "default" if is_default else "non-default"
                    self.log(f"Candidate VPC: {vpc_id} ({vpc_type}, /{cidr_size}, {total_subnets} subnets)")
            
            if candidates:
                # Sort by: default first (priority 0), then largest CIDR (smallest /xx), then most subnets
                candidates.sort(key=lambda x: (x[1], x[2], -x[3]))
                vpc_id = candidates[0][0]
                is_default = candidates[0][4]
                vpc_type = "default" if is_default else "non-default"
                self.log(f"Selected {vpc_type} VPC: {vpc_id}", force=True)
                self.discovered['vpc_id'] = vpc_id
                return self._discover_subnets(vpc_id)
        except Exception as e:
            self.log(f"Error finding candidate VPC: {e}")

        # If prefer_existing is True we do not want to create a VPC implicitly.
        if self.prefer_existing:
            self.log("No suitable existing VPC found and prefer_existing is enabled. Aborting.", force=True)
            print("Error: no suitable existing VPC found. Provide VPC_ID or disable --prefer-existing to allow creation.", file=sys.stderr)
            sys.exit(2)

        self.log("No suitable existing VPC found. Creation of a new VPC will be performed by Terraform.", force=True)
        return False
    
    def _is_private_subnet(self, subnet_id: str) -> bool:
        """Determine if a subnet is private based on routing table."""
        try:
            # Check tags first
            response = self.ec2.describe_subnets(SubnetIds=[subnet_id])
            if response['Subnets']:
                tags = {tag['Key']: tag['Value'] for tag in response['Subnets'][0].get('Tags', [])}
                tier = tags.get('Tier', '').lower()
                if tier == 'private':
                    return True
                elif tier == 'public':
                    return False
            
            # Check route tables
            rt_response = self.ec2.describe_route_tables(
                Filters=[{'Name': 'association.subnet-id', 'Values': [subnet_id]}]
            )
            
            for rt in rt_response['RouteTables']:
                for route in rt.get('Routes', []):
                    gateway_id = route.get('GatewayId', '')
                    nat_gateway_id = route.get('NatGatewayId', '')
                    
                    # If routes to IGW directly, it's public
                    if gateway_id.startswith('igw-'):
                        return False
                    # If routes to NAT, it's private
                    if nat_gateway_id:
                        return True
            
            # Default to private if no IGW route found
            return True
        except Exception as e:
            self.log(f"Error checking subnet {subnet_id}: {e}")
            return False
    
    def _discover_subnets(self, vpc_id: str) -> bool:
        """Discover subnets in the given VPC."""
        try:
            response = self.ec2.describe_subnets(
                Filters=[{'Name': 'vpc-id', 'Values': [vpc_id]}]
            )
            
            # MSK doesn't support us-east-1e - filter it out
            unsupported_azs = ['us-east-1e']
            
            private_subnets = [
                subnet['SubnetId']
                for subnet in response['Subnets']
                if self._is_private_subnet(subnet['SubnetId']) and 
                   subnet['AvailabilityZone'] not in unsupported_azs
            ]

            if not private_subnets:
                # Fallback: use all subnets except unsupported AZs
                fallback = [
                    subnet['SubnetId'] 
                    for subnet in response['Subnets']
                    if subnet['AvailabilityZone'] not in unsupported_azs
                ]
                self.log(f"No private subnets detected; using all subnets (excluding {unsupported_azs}): {fallback}", force=True)
                private_subnets = fallback

            self.discovered['private_subnet_ids'] = private_subnets
            self.log(f"Private subnets: {private_subnets}")

            return len(private_subnets) >= 2
        except Exception as e:
            self.log(f"Error discovering subnets: {e}")
            return False
    
    def discover_s3_buckets(self):
        """Discover existing S3 buckets."""
        self.log("Discovering S3 buckets...")
        
        bucket_types = ['checkpoints', 'artifacts', 'raw']
        
        try:
            if self.use_awscli:
                out = self._run_awscli(["s3api", "list-buckets", "--output", "json"]) 
                response = json.loads(out)
                all_buckets = [b['Name'] for b in response.get('Buckets', [])]
            else:
                response = self.s3.list_buckets()
                all_buckets = [b['Name'] for b in response['Buckets']]
            
            for bucket_type in bucket_types:
                pattern = f"{self.project}-{bucket_type}".lower()
                
                for bucket_name in all_buckets:
                    if pattern in bucket_name.lower():
                        self.discovered['existing_bucket_names'][bucket_type] = bucket_name
                        self.log(f"Found {bucket_type} bucket: {bucket_name}")
                        break
        except Exception as e:
            self.log(f"Error discovering S3 buckets: {e}")
    
    def discover_kms_keys(self):
        """Discover existing KMS keys."""
        self.log("Discovering KMS keys...")
        
        key_types = ['data', 'msk']
        
        try:
            for key_type in key_types:
                alias_name = f"alias/{self.project}-{key_type}"

                try:
                    if self.use_awscli:
                        out = self._run_awscli(["kms", "list-aliases", "--query", "Aliases[?starts_with(AliasName, `alias/`)].AliasName", "--output", "json"])
                        aliases = json.loads(out)
                        # Look for exact alias
                        if alias_name in aliases:
                            # Describe key by alias
                            out2 = self._run_awscli(["kms", "describe-key", "--key-id", alias_name, "--output", "json"])
                            resp = json.loads(out2)
                            key_state = resp['KeyMetadata']['KeyState']
                            enabled = resp['KeyMetadata']['Enabled']
                            
                            # Only use keys that are enabled and not pending deletion
                            if enabled and key_state == 'Enabled':
                                key_arn = resp['KeyMetadata']['Arn']
                                self.discovered['existing_kms_keys'][key_type] = key_arn
                                self.log(f"Found {key_type} KMS key: {key_arn}")
                            else:
                                self.log(f"KMS key {alias_name} is disabled or in state '{key_state}', skipping", force=True)
                        else:
                            self.log(f"KMS key alias {alias_name} not found")
                    else:
                        response = self.kms.describe_key(KeyId=alias_name)
                        key_state = response['KeyMetadata']['KeyState']
                        enabled = response['KeyMetadata']['Enabled']
                        
                        # Only use keys that are enabled and not pending deletion
                        if enabled and key_state == 'Enabled':
                            key_arn = response['KeyMetadata']['Arn']
                            self.discovered['existing_kms_keys'][key_type] = key_arn
                            self.log(f"Found {key_type} KMS key: {key_arn}")
                        else:
                            self.log(f"KMS key {alias_name} is disabled or in state '{key_state}', skipping", force=True)
                except self.kms.exceptions.NotFoundException:
                    self.log(f"KMS key alias {alias_name} not found")
                except Exception:
                    self.log(f"Error checking alias {alias_name}")
        except Exception as e:
            self.log(f"Error discovering KMS keys: {e}")
    
    def discover_msk_cluster(self):
        """Discover existing MSK cluster."""
        self.log("Discovering MSK cluster...")
        
        try:
            if self.use_awscli:
                out = self._run_awscli(["kafka", "list-clusters-v2", "--output", "json"]) 
                resp = json.loads(out)
                for cluster in resp.get('ClusterInfoList', []):
                    cluster_arn = cluster['ClusterArn']
                    cluster_name = cluster['ClusterName']
                    if self.project.lower() in cluster_name.lower():
                        self.discovered['existing_msk_cluster_arn'] = cluster_arn
                        self.log(f"Found MSK cluster: {cluster_name} ({cluster_arn})")
                        return
            else:
                paginator = self.kafka.get_paginator('list_clusters_v2')
                for page in paginator.paginate():
                    for cluster in page['ClusterInfoList']:
                        cluster_arn = cluster['ClusterArn']
                        cluster_name = cluster['ClusterName']
                        if self.project.lower() in cluster_name.lower():
                            self.discovered['existing_msk_cluster_arn'] = cluster_arn
                            self.log(f"Found MSK cluster: {cluster_name} ({cluster_arn})")
                            return
                        try:
                            tags_response = self.kafka.list_tags_for_resource(ResourceArn=cluster_arn)
                            tags = tags_response.get('Tags', {})
                            if tags.get('Project') == self.project:
                                self.discovered['existing_msk_cluster_arn'] = cluster_arn
                                self.log(f"Found MSK cluster by tag: {cluster_name} ({cluster_arn})")
                                return
                        except Exception:
                            pass
        except Exception as e:
            self.log(f"Error discovering MSK cluster: {e}")
    
    
    def _run_awscli(self, args: List[str]) -> str:
        """Run an AWS CLI command and return stdout as text. Raises on failure."""
        cmd = ["aws"] + args
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return proc.stdout
        except CalledProcessError as e:
            self.log(f"AWS CLI command failed: {' '.join(cmd)} | stderr: {e.stderr}")
            raise
    
    def run_discovery(self) -> Dict:
        """Run all discovery steps."""
        print(f"Starting AWS resource discovery for project: {self.project}")
        print(f"   Region: {self.region}\n")

        self.discover_vpc()
        self.discover_s3_buckets()
        self.discover_kms_keys()
        self.discover_msk_cluster()

        # Limit private_subnet_ids to 3 for MSK compatibility (MSK requires 2-3 subnets)
        if len(self.discovered['private_subnet_ids']) > 3:
            self.log(f"Limiting private_subnet_ids from {len(self.discovered['private_subnet_ids'])} to 3 for MSK compatibility", force=True)
            self.discovered['private_subnet_ids'] = self.discovered['private_subnet_ids'][:3]

        return self.discovered
    
    def print_summary(self):
        """Print a human-readable summary of discovery results."""
        print("\n" + "="*70)
        print("DISCOVERY SUMMARY")
        print("="*70)
        
        if self.discovered['vpc_id']:
            print(f"VPC: {self.discovered['vpc_id']}")
            print(f"   - Private subnets discovered: {len(self.discovered['private_subnet_ids'])}")
        else:
            print("VPC: not found (set vpc_id manually)")
        
        # S3 Buckets
        print(f"\nS3 Buckets:")
        for bucket_type, bucket_name in self.discovered['existing_bucket_names'].items():
            if bucket_name:
                print(f"   {bucket_type}: reuse {bucket_name}")
            else:
                print(f"   {bucket_type}: not found (provide existing bucket)")
        
        # KMS Keys
        print(f"\nKMS Keys:")
        for key_type, key_arn in self.discovered['existing_kms_keys'].items():
            if key_arn:
                print(f"   {key_type}: reuse {key_arn[:60]}...")
            else:
                print(f"   {key_type}: not found (provide existing key)")
        
        # MSK
        if self.discovered['existing_msk_cluster_arn']:
            print(f"\nMSK Cluster: reuse")
            print(f"   {self.discovered['existing_msk_cluster_arn'][:70]}...")
        else:
            print(f"\nMSK Cluster: create new")
        
        print("\n" + "="*70)
        print("Discovery complete. Generated: generated.auto.tfvars.json")
        print("="*70 + "\n")

    def update_terraform_tfvars(self, tfvars_path: str = 'terraform.tfvars') -> bool:
        """Update terraform.tfvars with discovered VPC information and minimal EMR settings."""
        try:
            if not os.path.exists(tfvars_path):
                self.log(f"terraform.tfvars not found at {tfvars_path}", force=True)
                return False

            with open(tfvars_path, 'r') as f:
                content = f.read()

            original_content = content
            import re

            if self.discovered['vpc_id']:
                vpc_id = self.discovered['vpc_id']
                content = re.sub(
                    r'vpc_id\s*=\s*"[^"]*"',
                    f'vpc_id           = "{vpc_id}"',
                    content
                )
                self.log(f"Updated vpc_id to {vpc_id}", force=True)

            if self.discovered['private_subnet_ids']:
                private_subnets = self.discovered['private_subnet_ids'][:3]
                subnet_list = '", "'.join(private_subnets)
                replacement = f'private_subnet_ids = ["{subnet_list}"]'
                if re.search(r'private_subnet_ids\s*=', content):
                    content = re.sub(
                        r'private_subnet_ids\s*=\s*\[.*?\]',
                        replacement,
                        content,
                        flags=re.DOTALL
                    )
                else:
                    content += f'\n{replacement}\n'
                self.log(f"Updated private_subnet_ids with {len(private_subnets)} entries", force=True)

            # Update EMR capacity settings with proper nested brace matching
            emr_initial_pattern = r'emr_initial_capacity\s*=\s*\{(?:[^{}]|\{[^}]*\})*\}'
            emr_initial_replacement = '''emr_initial_capacity = {
  driver = {
    cpu    = "1 vCPU"
    memory = "2 GB"
  }
  executor = {
    cpu    = "1 vCPU"
    memory = "2 GB"
  }
}'''
            content = re.sub(emr_initial_pattern, emr_initial_replacement, content, flags=re.DOTALL)

            emr_max_pattern = r'emr_maximum_capacity\s*=\s*\{(?:[^{}]|\{[^}]*\})*\}'
            emr_max_replacement = '''emr_maximum_capacity = {
  cpu    = "4 vCPU"
  memory = "8 GB"
}'''
            content = re.sub(emr_max_pattern, emr_max_replacement, content, flags=re.DOTALL)

            content = re.sub(
                r'emr_idle_timeout\s*=\s*\d+',
                'emr_idle_timeout       = 60',
                content
            )

            if content != original_content:
                with open(tfvars_path, 'w') as f:
                    f.write(content)
                self.log(f"Updated {tfvars_path}", force=True)
                return True

            self.log("No changes needed to terraform.tfvars", force=True)
            return False

        except Exception as e:
            self.log(f"Error updating terraform.tfvars: {e}", force=True)
            return False


def main():
    parser = argparse.ArgumentParser(
        description='Discover existing AWS resources for F1 Streaming Graph Infrastructure'
    )
    parser.add_argument(
        '--project',
        default=os.environ.get('PROJECT', 'f1-streaming-graph'),
        help='Project name (default: f1-streaming-graph or $PROJECT env var)'
    )
    parser.add_argument(
        '--region',
        default=os.environ.get('AWS_REGION', os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')),
        help='AWS region (default: us-east-1 or $AWS_REGION env var)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug output'
    )
    parser.add_argument(
        '--prefer-existing',
        action='store_true',
        default=True,
        help='Prefer reusing existing resources; abort if critical resources (e.g., VPC) are not found'
    )
    parser.add_argument(
        '--use-awscli',
        action='store_true',
        help='Use AWS CLI for discovery (requires aws CLI v2 configured in PATH)'
    )
    parser.add_argument(
        '--output',
        default='generated.auto.tfvars.json',
        help='Output file path (default: generated.auto.tfvars.json)'
    )
    parser.add_argument(
        '--update-tfvars',
        action='store_true',
        default=True,
        help='Update terraform.tfvars with discovered VPC and minimal EMR settings (default: enabled)'
    )
    parser.add_argument(
        '--tfvars-path',
        default='terraform.tfvars',
        help='Path to terraform.tfvars file (default: terraform.tfvars)'
    )
    
    args = parser.parse_args()
    
    # Validate AWS credentials
    try:
        sts = boto3.client('sts', region_name=args.region)
        identity = sts.get_caller_identity()
        if args.debug:
            print(f"[DEBUG] AWS Identity: {identity['Arn']}")
    except Exception as e:
        print(f"ERROR: Unable to authenticate with AWS: {e}", file=sys.stderr)
        print("Please configure AWS credentials using 'aws configure'", file=sys.stderr)
        sys.exit(1)
    
    # Run discovery
    discovery = AWSResourceDiscovery(args.project, args.region, args.debug)
    discovery.use_awscli = args.use_awscli
    discovery.prefer_existing = args.prefer_existing
    results = discovery.run_discovery()
    
    # Write results to JSON file
    try:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Wrote discovery results to: {args.output}")
    except Exception as e:
        print(f"ERROR writing output file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Update terraform.tfvars if requested
    if args.update_tfvars:
        print(f"\n{'='*70}")
        print("Updating terraform.tfvars with discovered values...")
        print(f"{'='*70}")
        discovery.update_terraform_tfvars(args.tfvars_path)
    
    # Print summary
    discovery.print_summary()


if __name__ == '__main__':
    main()
