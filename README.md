# Design

`eval.py` contains a parent flow, `all_collections_flow()`, a child flow `github_flow()`, a helper function for paginating through request responses, and a few tasks called by the child flow for making requests and writing files.

The parent flow receives a list of the repositories you want to request information on, and it starts a child flow for each of those repositories to collect their data. Once all the child flows are finished, one BigQuery table per API endpoint is created containing data from all the listed repositories.

# Usage

Provided you install the required packages, this flow will run as-is and simply write JSON files in repository-named folders to the current working directory.

If you'd like the flow to interact with GCP, there are a few additional requirements:

1. A Prefect workspace with a `GcpCredentials` block from `prefect-gcp` with the needed permissions to interact generally with GCS and BigQuery.
2. A (preferably empty) GCS bucket whose name you will provide.
3. The name of a BigQuery dataset you want the tables created in. Running the flow will create the dataset if it doesn't already exist.
4. Optionally, a GitHub personal access token to get around API rate limits for unauthenticated users.

Then, at the bottom of the script, uncomment the following lines and provide the required values:
```
        # token="<your-github-pat>",
        # credentials_block_name="<your-gcp-credentials-block-name>",
        # bucket="<your-gcs-bucket>",
        # dataset="<your-bigquery-dataset>"
```
