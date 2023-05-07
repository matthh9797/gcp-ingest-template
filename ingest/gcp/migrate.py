from google.cloud import bigquery, storage
from google.cloud.storage import Blob
import pandas as pd


def upload_dataframe_to_table(
    bq_client,
    dataframe: pd.DataFrame,
    table_ref: str,
    job_config: bigquery.LoadJobConfig,
) -> None:
    """
    Upload pandas dataframe to BigQuery table
    @param bq_client bigquery client object
    @param dataframe pandas dataframe
    @param table_ref full reference of bigquery table project.dataset.table
    @param job_config configuration for upload
    """
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


def upload_dataframe_to_bucket(
        storage_client,
        dataframe,
        bucketname,
        blobname
    ) -> None:
    """
    Upload dataframe to storage bucket
    """
    bucket = storage_client.get_bucket(bucketname)
    blob = Blob(blobname, bucket)
    blob.upload_from_string(dataframe.to_csv(index=False, date_format='%Y-%m-%d %H:%M:%S'), 'text/csv')
    return 'gs://{}/{}'.format(bucketname, blobname)


def upload_bucket_to_table(
    bq_client: bigquery.Client,
    gcsfile: str,
    table_ref: str,
    job_config: bigquery.LoadJobConfig,
):
    load_job = bq_client.load_table_from_uri(gcsfile, table_ref, job_config=job_config)
    load_job.result()  # waits for table load to complete

    if load_job.state != 'DONE':
        raise load_job.exception()

    return table_ref, load_job.output_rows