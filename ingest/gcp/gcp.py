from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from pathlib import Path


class GcpConnector:
    """
    class for interacting with BigQuery through python
    @param bq_config configuration for connecting to bigquery
    """

    def __init__(self, bq_config = None) -> None:
        if bq_config is not None:
            downloads_path = str(Path.home() / "Downloads")
            key_path = f'{downloads_path}/{bq_config["key_file"]}'
            credentials = self.__auth_with_service_key(key_path)
            self.billing_project = credentials.project_id

            # Construct a BigQuery client object.
            self.client = bigquery.Client(
                credentials=credentials, project=credentials.project_id
            )
        else:
            # Construct a BigQuery client object.
            self.client = bigquery.Client()      


    def __auth_with_service_key(self, key_path: str) -> service_account.Credentials:
        # Retrieve service account
        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return credentials

    def upload_dataframe_to_table(
        self,
        dataframe: pd.DataFrame,
        table_ref: str,
        job_config: bigquery.LoadJobConfig,
    ) -> None:
        """
        Upload pandas dataframe to BigQuery table
        @param dataframe pandas dataframe
        @param table_ref full reference of bigquery table project.dataset.table
        @param job_config configuration for upload
        """
        bq_client = self.client
        job = bq_client.load_table_from_dataframe(
            dataframe, table_ref, job_config=job_config
        )
        job.result()
        table = bq_client.get_table(table_ref)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_ref
            )
        )
