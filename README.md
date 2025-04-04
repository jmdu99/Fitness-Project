# Fitness Project

This project integrates:
- **Kafka** for real-time streaming
- **Producer** that sends enriched fitness exercise data to Kafka
- **Consumer** that stores data in **MongoDB**
- **Prefect** to batch-process and combine data from MongoDB and a CSV from **S3**
- **Amazon Redshift** for analytics
- **Amazon QuickSight** for dashboarding
- **Poetry** installed inside each container, generating its own lockfile

## How it Works

1. **Producer** downloads a CSV and a mapping file from S3, enriches random exercises with API calls, and sends them to Kafka.
2. **Consumer** listens to Kafka and inserts unique messages into MongoDB.
3. **Prefect** extracts data from MongoDB and S3, merges them, computes metrics, and loads the result into Redshift.
4. **QuickSight** reads from Redshift to display a dashboard with exercise metrics.
