#!/usr/bin/env python3
"""
Helper script to create Kafka topics on MSK (IAM auth) or local clusters.

Usage example (MSK with IAM):
    python scripts/create_topics.py \
        --bootstrap <bootstrap_brokers> \
        --cluster-arn <cluster_arn> \
        --region us-east-1

Usage example (local/Docker):
    python scripts/create_topics.py --auth-mode plain --bootstrap localhost:9092
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

import yaml
from kafka import errors as kafka_errors
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.oauth.abstract import AbstractTokenProvider
from dotenv import load_dotenv

load_dotenv()


@dataclass
class TopicDefinition:
    name: str
    partitions: int
    replication_factor: int
    config: Dict[str, Any]


class IAMSaslTokenProvider(AbstractTokenProvider):
    """Adapter that generates OAUTHBEARER tokens for MSK via the IAM signer."""

    def __init__(self, cluster_arn: str, region: str) -> None:
        try:
            from aws_msk_iam_sasl_signer import (  # type: ignore import-not-found
                MSKAuthTokenProvider,
            )
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "aws-msk-iam-sasl-signer is required when --auth-mode=iam. "
                "Install it with `pip install aws-msk-iam-sasl-signer`."
            ) from exc

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
    parser = argparse.ArgumentParser(
        description="Create Kafka topics on MSK or local (PLAINTEXT) brokers."
    )
    env_bootstrap = os.getenv("KAFKA_BOOTSTRAP_BROKERS") or os.getenv(
        "MSK_BOOTSTRAP_BROKERS"
    )
    env_cluster_arn = os.getenv("MSK_CLUSTER_ARN")
    env_region = os.getenv("AWS_REGION")
    env_auth_mode = os.getenv("KAFKA_AUTH_MODE", "iam").lower()
    if env_auth_mode not in {"iam", "plain"}:
        env_auth_mode = "iam"
    parser.add_argument(
        "--bootstrap",
        default=env_bootstrap,
        required=env_bootstrap is None,
        help=(
            "Comma-separated bootstrap brokers (defaults to KAFKA_BOOTSTRAP_BROKERS "
            "or MSK_BOOTSTRAP_BROKERS)."
        ),
    )
    parser.add_argument(
        "--cluster-arn",
        default=env_cluster_arn,
        help="ARN of the MSK cluster (required for IAM auth, defaults to MSK_CLUSTER_ARN).",
    )
    parser.add_argument(
        "--region",
        default=env_region,
        help="AWS region of the MSK cluster (required for IAM auth, defaults to AWS_REGION).",
    )
    parser.add_argument(
        "--topics-file",
        default="../topics.yaml",
        help="Path to YAML file describing topics (default: topics.yaml).",
    )
    parser.add_argument(
        "--client-id",
        default="fastf1-topic-admin",
        help="Kafka client ID to use when connecting.",
    )
    parser.add_argument(
        "--auth-mode",
        choices=["iam", "plain"],
        default=env_auth_mode,
        type=str.lower,
        help=(
            "Authentication mode for Kafka connection. "
            "Use 'iam' for MSK with IAM auth or 'plain' for local/docker clusters "
            "(defaults to KAFKA_AUTH_MODE env var or 'iam')."
        ),
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

    broker_count = 0
    try:
        cluster_info = admin.describe_cluster()
        broker_count = len(cluster_info.get("brokers", []) or [])
    except kafka_errors.KafkaError as exc:  # pragma: no cover - defensive guard
        print(f"Warning: Failed to describe cluster, skipping broker-based adjustments: {exc}")

    for topic in topics:
        if topic.name in existing:
            skipped.append(topic.name)
            continue

        effective_rf = topic.replication_factor
        if broker_count and effective_rf > broker_count:
            print(
                f"  - Adjusting replication factor for {topic.name} from "
                f"{effective_rf} to {broker_count} (available brokers: {broker_count})."
            )
            effective_rf = broker_count
        if effective_rf < 1:
            effective_rf = 1

        topic_configs: Dict[str, Any] = dict(topic.config)
        min_insync = topic_configs.get("min.insync.replicas")
        if isinstance(min_insync, str) and min_insync.isdigit():
            min_insync_val: int | None = int(min_insync)
        elif isinstance(min_insync, (int, float)):
            min_insync_val = int(min_insync)
        else:
            min_insync_val = None

        if min_insync_val is not None and min_insync_val > effective_rf:
            print(
                f"  - Adjusting min.insync.replicas for {topic.name} from "
                f"{min_insync_val} to {effective_rf}."
            )
            topic_configs["min.insync.replicas"] = str(effective_rf)

        new_topics.append(
            NewTopic(
                name=topic.name,
                num_partitions=topic.partitions,
                replication_factor=effective_rf,
                topic_configs={k: str(v) for k, v in topic_configs.items()},
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
                print(f"  [OK] Created {topic.name}")

    if skipped:
        print("Skipped existing topics:")
        for name in skipped:
            print(f"  - {name}")


def main() -> None:
    args = parse_args()
    if not args.bootstrap:
        raise SystemExit(
            "Bootstrap brokers are required. Provide --bootstrap or set "
            "KAFKA_BOOTSTRAP_BROKERS/MSK_BOOTSTRAP_BROKERS."
        )

    if args.auth_mode == "iam":
        if not args.cluster_arn:
            raise SystemExit(
                "cluster-arn is required when auth-mode is iam. "
                "Set MSK_CLUSTER_ARN or pass --cluster-arn."
            )
        if not args.region:
            raise SystemExit(
                "region is required when auth-mode is iam. "
                "Set AWS_REGION or pass --region."
            )

    topics = load_topics(args.topics_file)
    print(
        f"Loaded {len(topics)} topics from {args.topics_file}: "
        f"{', '.join(t.name for t in topics)}"
    )

    admin_kwargs: Dict[str, Any] = {
        "bootstrap_servers": args.bootstrap,
        "client_id": args.client_id,
        "api_version_auto_timeout_ms": 30000,
    }

    if args.auth_mode == "iam":
        token_provider = IAMSaslTokenProvider(
            cluster_arn=args.cluster_arn, region=args.region
        )
        admin_kwargs.update(
            {
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "OAUTHBEARER",
                "sasl_oauth_token_provider": token_provider,
            }
        )
    else:
        admin_kwargs.update({"security_protocol": "PLAINTEXT"})

    admin = KafkaAdminClient(**admin_kwargs)

    try:
        ensure_topics(admin, topics)
    finally:
        admin.close()


if __name__ == "__main__":
    main()
