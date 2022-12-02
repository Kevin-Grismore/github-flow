"""
A Prefect flow to get data about all pull reuquests, issues, and stargazers for each repository into BigQuery tables
"""

import json
from pathlib import Path

import requests
from google.cloud.bigquery import ExternalConfig, HivePartitioningOptions
from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_create_table
from prefect_gcp.cloud_storage import cloud_storage_upload_blob_from_file


@flow(name="All Collections GitHub Info to BigQuery")
def all_collections_flow(
    repos: list[str],
    token: str = None,
    credentials_block_name: str = None,
    bucket: str = None,
    dataset: str = None,
):
    """
    Collects pull request, issues, and stargazers data about the provided list of
    repositories from the GitHub API and creates BigQuery external tables containing
    that data.

    params:
        repos                   (list[str]) : The names of the repositories, formatted "<owner>/<repository>"
        token                   (str)       : A GitHub personal access token for raising the GitHub API rate limit ceiling
        credentials_block_name  (str)       : Name of the Prefect GcpCredentials block to use for accessing GCS and BigQuery
        bucket                  (str)       : The name of the GCS bucket to upload files to
        dataset                 (str)       : The name of the BigQuery dataset to create tables in
    """
    logger = get_run_logger()

    for repo in repos:
        logger.info(f"Starting subflow for repository: {repo}")

        # Run the child flow to request and save data about a single repository
        github_flow(
            repo=repo,
            bucket=bucket,
            credentials_block_name=credentials_block_name,
            token=token,
        )

    # The presence of a credentials block name and a dataset assumes the intent to create BigQuery tables
    if credentials_block_name and dataset:
        gcp_credentials = GcpCredentials.load(credentials_block_name)

        logger.info("GCP credentials provided, attempting to create Bigquery tables")

        for table in ["pulls", "issues", "stargazers"]:
            # Generate config for hive-partitioned external tables
            external_config = ExternalConfig(source_format="NEWLINE_DELIMITED_JSON")
            external_config.autodetect = True
            external_config.source_uris = f"gs://{bucket}/{table}/*"

            hive_partitioning_options = HivePartitioningOptions()
            hive_partitioning_options.mode = "AUTO"
            hive_partitioning_options.source_uri_prefix = f"gs://{bucket}/{table}"

            external_config.hive_partitioning = hive_partitioning_options

            # Creating a BigQuery table is a request-based operation, so run concurrently
            bigquery_create_table.submit(
                dataset=dataset,
                table=table,
                gcp_credentials=gcp_credentials,
                external_config=external_config,
            )


@flow(name="Get GitHub Info")
def github_flow(
    repo: str,
    token: str = None,
    credentials_block_name: str = None,
    bucket: str = None,
):
    """
    Collects pull request, issues, and stargazers data about a provided repository
    from the GitHub API and writes it to JSON files. If a valid credentials_block_name
    is provided, those files are then uploaded to a GCS bucket.

    params:
        repos                   (str)       : The name of the repository, formatted "<owner>/<repository>"
        token                   (str)       : A GitHub personal access token for raising the GitHub API rate limit ceiling
        credentials_block_name  (str)       : Name of the Prefect GcpCredentials block to use for accessing GCS and BigQuery
        bucket                  (str)       : The name of the GCS bucket to upload files to
    """
    endpoints_media = {
        "pulls": {"Accept": "application/vnd.github+json"},
        "stargazers": {"Accept": "application/vnd.github.star+json"},
        "issues": {"Accept": "application/vnd.github+json"},
    }

    for endpoint, headers in endpoints_media.items():
        # Get the PRs, stargazers and issues from the repo
        data, request_fut = paginate_requests(
            repo=repo,
            endpoint=endpoint,
            headers=headers,
            token=token,
        )

        # Get just the repo name for simpler directories
        repo_name = repo.split("/")[1]
        Path(repo_name).mkdir(exist_ok=True)

        # Write the raw data from each response to a json file
        json_fut = write_json.submit(
            data, f"{repo_name}/{endpoint}", wait_for=[request_fut]
        )

        # The presence of a credentials block name and a bucket assumes the intent to upload to GCS
        if credentials_block_name and bucket:
            gcp_credentials = GcpCredentials.load(credentials_block_name)

            # The file upload tasks need to wait for their corresponding files to be written
            cloud_storage_upload_blob_from_file.submit(
                file=f"{repo_name}/{endpoint}.json",
                bucket=bucket,
                blob=f"{endpoint}/repository={repo_name}/{endpoint}.json",
                gcp_credentials=gcp_credentials,
                wait_for=[json_fut],
            )


def paginate_requests(
    repo: str, endpoint: str, headers: dict, token: str = None
) -> tuple[list[dict], PrefectFuture]:
    """
    Calls the get_request() task for each page in the request response, ensuring
    every GitHub API call is a task.
    """
    if token:
        headers["Authorization"] = f"Bearer {token}"

    url = f"https://api.github.com/repos/{repo}/{endpoint}"

    response_fut = get_request.submit(url, headers)
    response = response_fut.result()
    data = response.json()

    while response.links.get("next"):
        response_fut = get_request.submit(
            response.links.get("next")["url"], headers, wait_for=[response_fut]
        )
        response = response_fut.result()
        data += response.json()

    return data, response_fut


@task
def get_request(url, headers):
    """
    Task wrapper for requests.get()
    """
    return requests.get(url=url, headers=headers)


@task
def write_json(data: list[dict], filename: str):
    """
    Write JSON to a file, one object per line as required by BigQuery
    """
    with open(f"{filename}.json", "w", encoding="utf-8") as file:
        for item in data:
            # + and - aren't valid symbols for field names in Bigquery
            if filename == "issues":
                item["reactions"]["minus_1"] = item["reactions"].pop("-1")
                item["reactions"]["plus_1"] = item["reactions"].pop("+1")

            json.dump(item, file, ensure_ascii=False)
            file.write("\n")


if __name__ == "__main__":
    all_collections_flow(
        [
            "PrefectHQ/prefect-gcp",
            "PrefectHQ/prefect-aws",
            "khuyentran1401/prefect-alert",
            # "<owner>/<repository>",
            # etc
        ],
        # token="<your-github-pat>",
        # credentials_block_name="<your-gcp-credentials-block-name>",
        # bucket="<your-gcs-bucket>",
        # dataset="<your-bigquery-dataset>"
    )
