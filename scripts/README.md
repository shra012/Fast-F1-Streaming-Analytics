# Pipeline Status Checker

Monitoring script for F1 Streaming Pipeline on EMR.

## Usage

```bash
ssh -i ~/.ssh/id_rsa hadoop@${EMR_MASTER_DNS} './check_pipeline_status.sh'
```

## Options

- `--detailed` - Include logs and partition-level metrics
- `--reset` - Interactive cleanup utility

## Prerequisites

Source environment variables:
```bash
source spark/emr_job.env
```

Deploy script to EMR:
```bash
scp -i ~/.ssh/id_rsa scripts/check_pipeline_status.sh hadoop@${EMR_MASTER_DNS}:~/
ssh -i ~/.ssh/id_rsa hadoop@${EMR_MASTER_DNS} 'chmod +x check_pipeline_status.sh'
```
