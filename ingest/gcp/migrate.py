import logging
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
    logging.info(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_ref
        )
    )


def upload_dataframe_to_bucket(
        storage_client,
        dataframe,
        bucketname,
        blobname,
        file_type: str = 'csv'
    ) -> None:
    """
    Upload dataframe to storage bucket
    """
    bucket = storage_client.get_bucket(bucketname)
    blob = Blob(blobname, bucket)
    if file_type == 'csv':
        blob.upload_from_string(dataframe.to_csv(index=False, date_format='%Y-%m-%d %H:%M:%S'), 'text/csv')
    elif file_type == 'json':
        # Must be new line deliminated json for bigquery: https://stackoverflow.com/questions/28976546/write-pandas-dataframe-to-newline-delimited-json
        blob.upload_from_string(dataframe.to_json(orient='records', lines=True, date_format='iso'), 'text/json')  
    else:
        logging.info(f'No upload method for file type {file_type}')

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