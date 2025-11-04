#!/usr/bin/env python3
"""
Discover the most recent MSK cluster and write its bootstrap brokers to a .env file.

The script updates (or creates) the configured .env file with the following keys:
  - MSK_BOOTSTRAP_BROKERS / KAFKA_BOOTSTRAP_BROKERS: IAM-enabled bootstrap broker string
  - MSK_CLUSTER_ARN: ARN of the selected cluster
  - AWS_REGION (optional): Region used for the lookup (unless --skip-region)
  - KAFKA_AUTH_MODE: Set to "iam" for compatibility with the scripts
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import set_key


def _parse_iso_datetime(value: object) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            normalized = value.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None


def _iter_clusters(
    client: boto3.client, name_filter: Optional[str] = None
) -> Iterable[Dict[str, object]]:
    paginator = client.get_paginator("list_clusters_v2")
    for page in paginator.paginate():
        for info in page.get("ClusterInfoList", []):
            if name_filter and info.get("ClusterName") != name_filter:
                continue
            yield info


def _select_cluster(
    client: boto3.client, cluster_arn: Optional[str], cluster_name: Optional[str]
) -> Tuple[str, Dict[str, object]]:
    if cluster_arn:
        try:
            response = client.describe_cluster_v2(ClusterArn=cluster_arn)
        except ClientError as exc:  # pragma: no cover - defensive guard
            raise SystemExit(f"Unable to describe cluster {cluster_arn}: {exc}") from exc
        cluster = response.get("ClusterInfo") or {}
        return cluster_arn, cluster

    latest: Optional[Tuple[datetime, Dict[str, object]]] = None
    for cluster in _iter_clusters(client, name_filter=cluster_name):
        creation = _parse_iso_datetime(cluster.get("CreationTime"))
        if creation is None:
            continue
        if latest is None or creation > latest[0]:
            latest = (creation, cluster)

    if latest is None:
        hint = (
            f"matching cluster name '{cluster_name}'"
            if cluster_name
            else "MSK clusters in the account"
        )
        raise SystemExit(f"Unable to find any {hint}.")

    return latest[1]["ClusterArn"], latest[1]


def _resolve_bootstrap(client: boto3.client, cluster_arn: str) -> Tuple[str, str]:
    try:
        response = client.get_bootstrap_brokers(ClusterArn=cluster_arn)
    except ClientError as exc:  # pragma: no cover - defensive guard
        raise SystemExit(f"Failed to retrieve bootstrap brokers: {exc}") from exc

    preferred_keys = [
        "BootstrapBrokerStringSaslIam",
        "BootstrapBrokerStringTls",
        "BootstrapBrokerString",
    ]
    for key in preferred_keys:
        brokers = response.get(key)
        if brokers:
            return brokers, key
    raise SystemExit("Bootstrap brokers not available for the selected cluster.")


def parse_args() -> argparse.Namespace:
    env_region = os.getenv("AWS_REGION")
    parser = argparse.ArgumentParser(
        description="Update a .env file with the latest MSK bootstrap brokers."
    )
    parser.add_argument(
        "--region",
        default=env_region,
        required=env_region is None,
        help="AWS region to query (defaults to AWS_REGION env var).",
    )
    parser.add_argument(
        "--cluster-arn",
        help="Explicit MSK cluster ARN. Overrides automatic selection.",
    )
    parser.add_argument(
        "--cluster-name",
        help="Filter clusters by name when selecting the latest entry.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the dotenv file to update (default: .env).",
    )
    parser.add_argument(
        "--bootstrap-key",
        default="MSK_BOOTSTRAP_BROKERS",
        help="Environment variable name for the bootstrap brokers.",
    )
    parser.add_argument(
        "--arn-key",
        default="MSK_CLUSTER_ARN",
        help="Environment variable name for the cluster ARN.",
    )
    parser.add_argument(
        "--region-key",
        default="AWS_REGION",
        help="Environment variable name for the AWS region.",
    )
    parser.add_argument(
        "--skip-region",
        action="store_true",
        help="Do not write the region value to the dotenv file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        client = boto3.client("kafka", region_name=args.region)
    except (BotoCoreError, ClientError) as exc:  # pragma: no cover - defensive guard
        raise SystemExit(f"Failed to create Kafka client: {exc}") from exc

    cluster_arn, cluster_info = _select_cluster(
        client, args.cluster_arn, args.cluster_name
    )
    creation = _parse_iso_datetime(cluster_info.get("CreationTime"))
    cluster_name = cluster_info.get("ClusterName", "unknown")

    brokers, source_key = _resolve_bootstrap(client, cluster_arn)

    env_path = Path(args.env_file)
    env_path.touch(exist_ok=True)

    set_key(str(env_path), args.bootstrap_key, brokers)
    set_key(str(env_path), "KAFKA_BOOTSTRAP_BROKERS", brokers)
    set_key(str(env_path), "KAFKA_AUTH_MODE", "iam")
    set_key(str(env_path), args.arn_key, cluster_arn)
    if not args.skip_region and args.region:
        set_key(str(env_path), args.region_key, args.region)

    created_display = creation.isoformat() if creation else "unknown"
    print(
        f"Updated {env_path} with bootstrap brokers ({source_key}) from "
        f"{cluster_name} ({cluster_arn}), created {created_display}."
    )


if __name__ == "__main__":
    main()
