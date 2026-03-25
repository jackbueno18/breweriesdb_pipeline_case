import json
import logging
import os
from typing import Any

import boto3
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=10))
def _fetch_page(url: str, page: int, per_page: int) -> list:
    """Fetches a single page from the API with retry on failure."""
    resp = requests.get(url, params={"page": page, "per_page": per_page}, timeout=30)
    resp.raise_for_status()
    return resp.json()

def run_bronze(**context: Any) -> str:
    """Fetches all breweries from the Open Brewery DB API and uploads
    the raw JSON file to the bronze layer in MinIO.

    Paginates through the API until no more results are returned.

    Args:
        **context: Airflow context dict. Uses 'ds' for execution date.

    Returns:
        S3 key where the JSON file was saved.

    Raises:
        ValueError: If the API returns no data at all.
        requests.HTTPError: If any API request fails.
    """
    execution_date: str = context["ds"]

    api_url = os.environ["BREWERY_API_URL"]
    per_page = int(os.environ.get("API_PER_PAGE", "200"))

    breweries = []
    page = 1

    while True:
        data = _fetch_page(api_url, page, per_page)
        if not data:
            break

        breweries.extend(data)
        page += 1

    if not breweries:
        raise ValueError("No data returned from API")

    logger.info(f"Fetched {len(breweries)} breweries")

    raw_json = json.dumps(breweries, indent=2)
    key = f"bronze/breweries_{execution_date}.json"

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )

    s3.put_object(
        Bucket=os.environ["MINIO_BUCKET"],
        Key=key,
        Body=raw_json.encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(f"Saved to {key}")
    return key