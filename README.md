# Breweries Data Pipeline

Data pipeline that follows the **medallion architecture** (bronze → silver → gold), consuming data from the [Open Brewery DB API](https://api.openbrewerydb.org/v1/breweries) and storing it in MinIO (S3-compatible).

Stack: **Airflow (Astro)** · **PySpark** · **MinIO** · **Docker**

---

## Architecture

```
API (Open Brewery DB)
        │
        ▼
  ┌───────────┐     ┌───────────┐     ┌───────────┐
  │  BRONZE   │────▶│  SILVER   │────▶│   GOLD    │
  │  (JSON)   │     │ (Parquet) │     │ (Parquet) │
  └───────────┘     └───────────┘     └───────────┘
        │                │                 │
        └────────────────┴─────────────────┘
                    MinIO (S3)
```

### Layers

| Layer | Format | Description |
|-------|--------|-------------|
| **Bronze** | JSON | Raw data from API, no transformation. File named with execution date so we keep history. |
| **Silver** | Parquet (partitioned by `state_province`) | Cleaned data: strings normalized, coordinates cast to double, records without state removed. |
| **Gold** | Parquet | Aggregated view: brewery count per `brewery_type` and `state_province`. |

### Silver Transformations

- `brewery_type`: trim + lowercase
- `city`, `country`, `name`, `state_province`: trim
- `latitude` / `longitude`: cast to `DoubleType`
- Drop records with null `state_province`;
- Add `ingestion_date` column as metadata

### Data Validation

Before writing to silver layer, we validate:
- All required columns exist in the source (`id`, `name`, `brewery_type`, `state_province`)
- The bronze file is not empty.

If any check fails, the task raises a `ValueError` and Airflow retries it.

---

## How to Run

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) need to be running
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) installed

### Starting the Environment

```bash
astro dev start
```

This spins up:
- **Airflow** (scheduler, webserver, triggerer, dag processor, postgres)
- **MinIO** (S3-compatible storage)
- The bucket `breweries` is created automatically

### Access (default credentials)

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

### Running the Pipeline

1. Open the Airflow UI
2. Enable the `brewery_pipeline` DAG
3. Trigger manually or wait for the daily schedule

---

## Project Structure

```
├── dags/
│   └── brewery_pipeline.py        # Main DAG
├── include/
│   ├── utils/
│   │   └── spark_utils.py         # SparkSession configured for MinIO
│   └── scripts/
│       ├── bronze.py              # Raw ingestion (API fetch + MinIO upload)
│       ├── silver.py              # Cleaning + partitioning
│       └── gold.py                # Aggregation
├── tests/
│   └── test_brewery_pipeline.py   # Pipeline unit tests
├── .env                           # Environment variables (MinIO, API, Spark)
├── Dockerfile                     # Runtime + Java + Hadoop AWS jars
├── docker-compose.override.yml    # MinIO service
├── requirements.txt               # PySpark, boto3, requests, tenacity
└── README.md
```

---

## Tests

```bash
astro dev bash
pytest tests/ -v
```

Test coverage:
- API pagination (mocked)
- Error handling when API returns empty data
- JSON upload to MinIO (mocked)
- DAG structure and task dependencies
- Retry and tag configuration

---

## Error Handling

The pipeline has two layers of retry:

- **API level (tenacity)**: Each HTTP request to the Brewery API retries up to 3 times with exponential backoff. This handles temporary network issues or API rate limits.
- **Task level (Airflow)**: If the whole task fails, Airflow retries it up to 2 times with a 5-minute delay between attempts.
On top of that, bronze raises an error if the API returns no data, and silver validates the schema before processing.

---

## Design Decisions

**Why MinIO?** It simulates S3 locally, so no need for an AWS account. In production you just change the endpoint and credentials to point to real S3.

**Why PySpark instead of Pandas?** Spark is better for larger volumes and more common in production data lake setups (even it's a small dataset).

**Why boto3 for Bronze?** To upload raw JSON I opted for not using SparkSession. boto3 is simpler and lighter for this portion.

**Why partition by `state_province`?** It's the most useful location field without creating too many small partitions. Partitioning by city for example would generate way too many files.

**Silver layer + MinIO configs** — Spark needs some specific settings to work with MinIO:
- `fs.s3a.path.style.access=true` (MinIO doesn't support virtual-hosted style)
- `fs.s3a.connection.ssl.enabled=false` (local MinIO has no SSL)
- `mapreduce.fileoutputcommitter.algorithm.version=2` (avoids problems with `_temporary` dirs)
- Explicit credentials provider (`SimpleAWSCredentialsProvider`)

---

## Monitoring and Alerting

For a production environment, would be a good approach:

1. **Airflow alerts on failure**: send Slack or email notifications when a task fails. We already have 2 retries configured in the DAG.

2. **Freshness monitoring**: Set up SLA miss alerts in Airflow if the DAG takes too long to finish.

3. **MinIO health**: Basic storage monitoring — disk space, health checks.

---

## Trade-offs

- **Overwrite on Silver/Gold**: Each run replaces the old data. If we needed to keep history, we could use append + merge or delta tables.
- **Spark local mode**: Works fine for this data size. For production, we would need a proper cluster setup.
- **Credentials in .env**: Good enough for local dev. In production, better to use a secrets manager or vault.
- **Single DAG**: One DAG with 3 tasks. Simple and easy to follow.