# VCT Data Pipeline — Tech Stack

## Infrastructure
| Tool | Version | Purpose |
|------|---------|---------|
| Docker | latest | Containerisation |
| Docker Compose | latest | Multi-service orchestration |
| MinIO | latest | S3-compatible data lake storage |
| Project Nessie | latest | Apache Iceberg catalog |

## Python Environment
| Library | Version | Layer | Purpose |
|---------|---------|-------|---------|
| python | ^3.11 | all | Runtime |
| python-dotenv | 1.0.0 | all | Load environment variables from .env files |
| vlrdevapi | latest | bronze | VCT/VLR esports data API client |
| pydantic | 2.7.0 | bronze | Data validation and serialisation |
| boto3 | 1.34.0 | bronze | AWS/MinIO S3 SDK for object storage |
| pyspark | 3.5.1 | silver, gold | Distributed data processing |
| polars | 0.20.0 | silver, gold | Fast in-process DataFrame library |
| pyiceberg[s3filesystem,nessie] | 0.7.0 | silver, gold | Apache Iceberg table format client |
| pyarrow | 15.0.0 | silver, gold | Columnar in-memory format, Parquet I/O |
| apache-airflow | 2.9.1 | orchestration | Workflow scheduling and orchestration |
| streamlit | 1.35.0 | dashboard | Interactive data dashboard framework |
| pytest | 8.2.0 | dev | Unit and integration testing |
| ruff | 0.4.4 | dev | Fast Python linter and formatter |
| ipykernel | 6.29.0 | dev | Jupyter notebook kernel for exploration |
