# CMS Claims Analytics Pipeline

End-to-end AWS data pipeline for healthcare provider cost & utilization analysis using Medicare public data.

## Architecture
Raw CMS CSVs → S3 → Glue ETL → Parquet → Redshift → Athena → QuickSight

## Tech Stack
- AWS Glue, S3, Redshift, Athena, Lambda, Step Functions
- Python, PySpark, SQL
- CloudFormation (IaC)

## Project Status
🔄 In Progress — AWS Certified Data Engineer (DEA-C01)