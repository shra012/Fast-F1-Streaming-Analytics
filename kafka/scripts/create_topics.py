#!/usr/bin/env python3
"""
Helper script to create Kafka topics on an MSK cluster using IAM authentication.

Usage example:
    python scripts/create_topics.py \
        --bootstrap <bootstrap_brokers> \
        --cluster-arn <cluster_arn> \
        --region us-east-1
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

import yaml
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka import errors as kafka_errors
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.oauth.abstract import AbstractTokenProvider


@dataclass
class TopicDefinition:
    name: str
    partitions: int
    replication_factor: int
    config: Dict[str, Any]


class IAMSaslTokenProvider(AbstractTokenProvider):
    """Adapter that generates OAUTHBEARER tokens for MSK via the IAM signer."""

    def __init__(self, cluster_arn: str, region: str) -> None:
        self._cluster_arn = cluster_arn
        self._signer = MSKAuthTokenProvider(region_name=region)

    def token(self) -> Tuple[str, float, str, Dict[str, str]]:
        token, expiry = self._signer.get_token(cluster_arn=self._cluster_arn)
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        lifetime_seconds = max(1.0, (expiry - now).total_seconds())
        return token, lifetime_seconds, "", {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create Kafka topics on MSK.")
    parser.add_argument(
        "--bootstrap",
        required=True,
        help="Comma-separated bootstrap brokers with SASL/IAM enabled.",
    )
    parser.add_argument(
        "--cluster-arn",
        required=True,
        help="ARN of the MSK cluster (needed for IAM auth token generation).",
    )
    parser.add_argument(
        "--region",
        required=True,
        help="AWS region of the MSK cluster.",
    )
    parser.add_argument(
        "--topics-file",
        default="topics.yaml",
        help="Path to YAML file describing topics (default: topics.yaml).",
    )
    parser.add_argument(
        "--client-id",
        default="fastf1-topic-admin",
        help="Kafka client ID to use when connecting.",
    )
    return parser.parse_args()


def load_topics(path: str) -> List[TopicDefinition]:
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    topics: List[TopicDefinition] = []
    for entry in raw:
        topics.append(
            TopicDefinition(
                name=entry["name"],
                partitions=int(entry.get("partitions", 1)),
                replication_factor=int(entry.get("replication_factor", 1)),
                config=entry.get("config", {}) or {},
            )
        )
    return topics


def ensure_topics(
    admin: KafkaAdminClient, topics: Iterable[TopicDefinition]
) -> None:
    existing = set(admin.list_topics())
    new_topics: List[NewTopic] = []
    skipped: List[str] = []

    for topic in topics:
        if topic.name in existing:
            skipped.append(topic.name)
            continue
        new_topics.append(
            NewTopic(
                name=topic.name,
                num_partitions=topic.partitions,
                replication_factor=topic.replication_factor,
                topic_configs=topic.config,
            )
        )

    if new_topics:
        print(f"Creating {len(new_topics)} topic(s)...")
        try:
            admin.create_topics(new_topics, validate_only=False)
        except kafka_errors.TopicAlreadyExistsError as exc:
            print(f"Topic already exists: {exc}")
        except kafka_errors.KafkaError as exc:
            raise SystemExit(f"Failed to create topics: {exc}") from exc
        else:
            for topic in new_topics:
                print(f"  ✔ Created {topic.name}")

    if skipped:
        print("Skipped existing topics:")
        for name in skipped:
            print(f"  • {name}")


def main() -> None:
    args = parse_args()
    topics = load_topics(args.topics_file)
    print(
        f"Loaded {len(topics)} topics from {args.topics_file}: "
        f"{', '.join(t.name for t in topics)}"
    )

    token_provider = IAMSaslTokenProvider(
        cluster_arn=args.cluster_arn, region=args.region
    )

    admin = KafkaAdminClient(
        bootstrap_servers=args.bootstrap,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=token_provider,
        client_id=args.client_id,
        api_version_auto_timeout_ms=30000,
    )

    try:
        ensure_topics(admin, topics)
    finally:
        admin.close()


if __name__ == "__main__":
    main()
