import json
import os
import pytest
from unittest.mock import patch, MagicMock
from tenacity import stop_after_attempt

MOCK_ENV = {
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "MINIO_BUCKET": "breweries",
    "BREWERY_API_URL": "https://api.openbrewerydb.org/v1/breweries",
    "API_PER_PAGE": "200",
    "SPARK_JARS_DIR": "/opt/spark/jars",
}


# ------------------------------------------------------------------ #
#  Bronze
# ------------------------------------------------------------------ #

@patch.dict(os.environ, MOCK_ENV)
@patch("include.scripts.bronze.boto3.client")
@patch("include.scripts.bronze.requests.get")
def test_bronze_saves_json(mock_get, mock_boto):
    """Checks that bronze fetches the API and uploads valid JSON."""
    from include.scripts.bronze import run_bronze, _fetch_page
    _fetch_page.retry.stop = stop_after_attempt(1)  # disable retry in tests

    # fake one page of data + empty page to stop
    page1 = MagicMock()
    page1.json.return_value = [{"id": "1"}, {"id": "2"}]
    page1.raise_for_status.return_value = None

    empty = MagicMock()
    empty.json.return_value = []
    empty.raise_for_status.return_value = None

    mock_get.side_effect = [page1, empty]
    mock_s3 = MagicMock()
    mock_boto.return_value = mock_s3

    key = run_bronze(ds="2025-06-15")

    assert key == "bronze/breweries_2025-06-15.json"
    mock_s3.put_object.assert_called_once()

    # make sure the uploaded string is valid JSON
    uploaded = mock_s3.put_object.call_args[1]["Body"].decode("utf-8")
    assert len(json.loads(uploaded)) == 2


@patch.dict(os.environ, MOCK_ENV)
@patch("include.scripts.bronze.requests.get")
def test_bronze_fails_on_empty_data(mock_get):
    """Checks that bronze raises an error if the API returns nothing."""
    from include.scripts.bronze import run_bronze, _fetch_page
    _fetch_page.retry.stop = stop_after_attempt(1)

    empty = MagicMock()
    empty.json.return_value = []
    empty.raise_for_status.return_value = None
    mock_get.return_value = empty

    with pytest.raises(ValueError):
        run_bronze(ds="2025-06-15")


# ------------------------------------------------------------------ #
#  DAG structure
# ------------------------------------------------------------------ #

def test_dag_has_three_tasks():
    """Checks that the DAG has the expected tasks."""
    from dags.brewery_pipeline import dag

    task_ids = [t.task_id for t in dag.tasks]
    assert "bronze_ingestion" in task_ids
    assert "silver_transformation" in task_ids
    assert "gold_aggregation" in task_ids


def test_dag_task_order():
    """Checks that tasks run in the right order: bronze -> silver -> gold."""
    from dags.brewery_pipeline import dag

    bronze = dag.get_task("bronze_ingestion")
    silver = dag.get_task("silver_transformation")
    gold = dag.get_task("gold_aggregation")

    # silver comes after bronze
    assert silver.task_id in [t.task_id for t in bronze.downstream_list]
    # gold comes after silver
    assert gold.task_id in [t.task_id for t in silver.downstream_list]


def test_dag_has_retries():
    """Checks that retries are configured."""
    from dags.brewery_pipeline import dag
    assert dag.default_args.get("retries", 0) >= 2
