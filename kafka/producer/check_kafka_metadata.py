#!/usr/bin/env python3
"""
Quick Kafka metadata test to diagnose connection issues.
Tests if producer can connect to Kafka and fetch metadata.
"""

import sys
import logging
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Enable debug logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_kafka_connection(bootstrap_servers, topics=None):
    """Test Kafka connection and metadata retrieval."""
    
    logger.info("="*80)
    logger.info("Kafka Connection Test")
    logger.info("="*80)
    logger.info(f"Bootstrap servers: {bootstrap_servers}")
    logger.info(f"Test topics: {topics or ['telemetry.raw', 'race.events']}")
    logger.info("")
    
    topics = topics or ['telemetry.raw', 'race.events']
    
    try:
        logger.info("Creating Kafka producer with PLAINTEXT protocol...")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(','),
            security_protocol='PLAINTEXT',
            request_timeout_ms=10000,  # 10 second timeout
            api_version_auto_timeout_ms=5000,
        )
        logger.info("[OK] Producer created successfully")
        
        logger.info("")
        logger.info("Fetching cluster metadata...")
        
        # Force metadata refresh to get all topics
        future = producer._client.cluster.request_update()
        producer._client.poll(future=future)
        
        # Access cluster metadata
        cluster = producer._client.cluster
        
        logger.info(f"Cluster brokers: {cluster.brokers()}")
        logger.info(f"Available topics: {list(cluster.topics())}")
        
        # Test metadata for each topic
        logger.info("")
        for topic in topics:
            logger.info(f"Testing topic: {topic}")
            try:
                # This will force metadata update for the topic
                partitions = producer.partitions_for(topic)
                if partitions:
                    logger.info(f"  [OK] Topic '{topic}' exists with {len(partitions)} partition(s): {partitions}")
                else:
                    logger.warning(f"  [FAIL] Topic '{topic}' not found in cluster metadata")
                    logger.warning(f"    Available topics: {cluster.topics()}")
            except Exception as e:
                logger.error(f"  [FAIL] Error getting metadata for topic '{topic}': {e}")
        
        logger.info("")
        logger.info("="*80)
        logger.info("[OK] Kafka connection test PASSED")
        logger.info("="*80)
        
        producer.close()
        return 0
        
    except KafkaError as e:
        logger.error("")
        logger.error("="*80)
        logger.error("[FAIL] Kafka connection test FAILED")
        logger.error("="*80)
        logger.error(f"Error: {e}")
        logger.error("")
        logger.error("Common causes:")
        logger.error("1. Security group not allowing traffic from EMR to MSK on port 9092")
        logger.error("2. Topics not created yet (run kafka/scripts/create_topics.py)")
        logger.error("3. Wrong bootstrap servers (check Terraform output)")
        logger.error("4. MSK cluster not in ACTIVE state")
        logger.error("5. Network routing issue between EMR and MSK")
        logger.error("")
        import traceback
        traceback.print_exc()
        return 1
        
    except Exception as e:
        logger.error("")
        logger.error("="*80)
        logger.error("[FAIL] Unexpected error")
        logger.error("="*80)
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 2


def main():
    parser = argparse.ArgumentParser(description="Test Kafka connectivity and metadata")
    parser.add_argument(
        "--bootstrap",
        required=True,
        help="Comma-separated Kafka bootstrap servers"
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        help="Topics to test (default: telemetry.raw, race.events)"
    )
    
    args = parser.parse_args()
    
    sys.exit(test_kafka_connection(args.bootstrap, args.topics))


if __name__ == "__main__":
    main()

