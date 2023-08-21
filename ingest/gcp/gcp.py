import logging
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from pathlib import Path
from datetime import datetime, timedelta

from .migrate import upload_dataframe_to_table, upload_bucket_to_table, upload_dataframe_to_bucket
from .bigquery import get_partition_type_from_str, get_partition_range, get_partition_format_from_str, get_source_format, table_schema_to_json


class GcpConnector:
    """
    class for interacting with BigQuery through python
    @param auth_config configuration for connecting to bigquery and cloud storage
    """

    def __init__(self, auth_config = None) -> None:
        if auth_config is not None:
            downloads_path = str(Path.home() / "Downloads")
            key_path = f'{downloads_path}/{auth_config["key_file"]}'
            credentials = self.__auth_with_service_key(key_path)
            self.billing_project = credentials.project_id

            # Construct a BigQuery client object.
            self.bq_client = bigquery.Client(
                credentials=credentials, project=credentials.project_id
            )
            self.storage_client = storage.Client(
                credentials=credentials, project=credentials.project_id
            )
        else:
            # Construct a BigQuery client object.
            self.bq_client = bigquery.Client()      
            self.storage_client = storage.Client()      


    def __auth_with_service_key(self, key_path: str) -> service_account.Credentials:
        # Retrieve service account
        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return credentials
    

    def upload_dataframe_to_table_via_bucket(self, dataframe, uploaded_at, file_type,
                                             storage_client, bucketname, blobdir,
                                             bq_client, dataset_id, table_id, table_ref, job_config):
        """Upload dataframe to BigQuery table via storage bucket"""
        blobname = f'{uploaded_at.strftime("%Y%m%d")}:{table_id}'
        if blobdir is not None:
            blobname = f"{blobdir}/{blobname}"
        logging.info(f'Uploading dataframe to bucket: gs://{bucketname}/{blobname}')
        gcslocation = upload_dataframe_to_bucket(storage_client, dataframe, bucketname, blobname, file_type)
        logging.info(f'Uploading gcsfile from {gcslocation} to bigquery table: {dataset_id}:{table_id}')
        try:
            job_config.source_format = get_source_format(file_type) 
        except KeyError:
            return logging.info(f'No upload method for file type {file_type}')     
        upload_bucket_to_table(bq_client, gcslocation, table_ref, job_config)

    
    def upload_dataframe_to_table_with_partition_via_bucket(self, dataframe, uploaded_at, file_type,
                                             storage_client, bucketname, blobdir,
                                             bq_client, dataset_id, table_id, dataset_ref, table_ref, job_config, 
                                             partition_col, partition_type, lag, window):
        """Upload dataframe to table with partition via storage bucket"""
        try:
            bq_client.get_table(dataset_ref.table(table_id))  # Make an API request.
            dataframe[partition_col] = dataframe[partition_col].dt.tz_localize(None)
            dt = uploaded_at - timedelta(days=lag) # Start lag days back if data lags the download date
            for _ in range(0, window):
                start_date, end_date = get_partition_range(dt, partition_type)
                dt = start_date - timedelta(days=1) # Next date in loop
                partition_id = start_date.strftime(get_partition_format_from_str(partition_type))
                logging.info(f'range: ({start_date}, {end_date})')
                blobname = f'{uploaded_at.strftime("%Y%m%d")}:{table_id}${partition_id}'
                if blobdir is not None:
                    blobname = f"{blobdir}/{blobname}"
                logging.info(f'Uploading dataframe to gcslocation: gs://{bucketname}/{blobname}')
                gcslocation = upload_dataframe_to_bucket(storage_client, 
                                                        dataframe.loc[dataframe[partition_col].between(start_date, end_date)], 
                                                        bucketname, 
                                                        blobname,
                                                        file_type)
                table_ref = dataset_ref.table(f'{table_id}${partition_id}')
                logging.info(f'Uploading gcsfile from {gcslocation} to bigquery table: {dataset_id}:{table_id}${partition_id}')
                job_config.skip_leading_rows=1
                try:
                    job_config.source_format = get_source_format(file_type) 
                except KeyError:
                    return logging.warning(f'No upload method for file type {file_type}') 
                upload_bucket_to_table(bq_client, gcslocation, table_ref, job_config)
        except NotFound:
            return logging.info(f'Table {dataset_id}.{table_id} not found. Create table first to use window uploads.')    
    

    def upload_dataframe_to_table_with_merge_via_bucket(self, dataframe, uploaded_at, file_type, merge_id_column,
                                                storage_client, bucketname, blobdir,
                                                bq_client, dataset_id, table_id, table_ref, job_config):
        """Upload dataframe to BigQuery table via bucket by merging dataframe to existing table using an id_column"""

        staging_dataset_id = f'{dataset_id}_staging'
        staging_table_id = f'stg__{uploaded_at.strftime("%Y%m%d")}_{table_id}'

        staging_dataset_ref = bq_client.dataset(staging_dataset_id)
        staging_table_ref = staging_dataset_ref.table(staging_table_id)

        self.upload_dataframe_to_table_via_bucket(dataframe, uploaded_at, file_type,
                                             storage_client, bucketname, blobdir,
                                             bq_client, staging_dataset_id, staging_table_id, staging_table_ref, job_config)
        
        cols = [x.name for x in job_config.schema]
        cols.remove(merge_id_column)
        set_conditions = [f'        t.{x} = m.{x}' for x in cols]
        joined = ',\n'.join(set_conditions)
        logging.info(f'Merging {staging_dataset_id}.{staging_table_id} onto {dataset_id}.{table_id} with {merge_id_column}')
        dml_statement = f"""
        merge into {dataset_id}.{table_id} t
        using {staging_dataset_id}.{staging_table_id} m
        on t.{merge_id_column} = m.{merge_id_column} 
        when matched then 
        update set 
        {joined}
        when not matched then
        insert row
        ;
        """
        query_job = bq_client.query(dml_statement)  # API request
        query_job.result() 



    def upload(self, 
               dataframe: dict, 
               table_id: str,
               dataset_id: str,
               schema_path: str = None,
               bucketname: str = None,
               blobdir: str = None,
               partition_col: str = None,
               partition_type: str = None,
               window: int = None, # number of days to go back and overwrite
               lag: int = 0, # Number of days,
               add_updated_at: bool = False,
               autodetect_mode = False,
               keep_autodetect_table = False,
               file_type: str = 'csv',
               merge = False,
               merge_id_column: str = None
               ) -> None:
        """
        Upload dataframe to bigquery table. Run options include use of bucket and partitions.
        @param dataframe dataframe to upload
        @param table_id name of destination bigquery table
        @param dataset_id name of destination bigquery dataset
        @param schema_path path to bigquery schema
        @param bucketname if using bucket, name of bucket
        @param blobdir sub directory for gcp storage bucket
        @param partition_col if using partition, name of partition column
        @param partition_type type of partition (DAY, MONTH, YEAR)
        @param window if using window, number of partition units to write into bq
        @param lag how many days to lag run date
        @param autodetect_mode If true, run the upload function with 100 rows of data to autodetect table schema
        @param keep_autodetect_table Do not automatically drop the table used for autodetecting table schema
        @param file_type specify file type for storage bucket (e.g. csv or json)
        @param merge if merge is true merge data into existing table, another incremental strategy
        """

        bq_client = self.bq_client
        use_bucket = (bucketname is not None)
        dataset_ref = bq_client.dataset(dataset_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition='WRITE_TRUNCATE'
        )

        if use_bucket == True:
            storage_client = self.storage_client
        
        if autodetect_mode:
            table_id = f'TEMP_AD_{table_id}'
            nrow = len(dataframe.index)
            if nrow < 100:
                logging.info(f'Warning: Dataframe has only {nrow} rows. 100 rows used for autodetect.')
            else:
                logging.info('Filtering first 100 rows of dataframe to autodetect schema')
                dataframe = dataframe.head(100)

        table_ref = dataset_ref.table(table_id)

        if partition_col is not None and autodetect_mode is False:
            job_config.time_partitioning=bigquery.TimePartitioning(
                type_=get_partition_type_from_str(partition_type),
                field=partition_col,  # Name of the column to use for partitioning.
            )

        if schema_path is None or autodetect_mode:
            job_config.autodetect=True
        else:
            schema = bq_client.schema_from_json(schema_path)    
            job_config.schema = schema
            if file_type == 'csv':
                job_config.skip_leading_rows=1

        # No accepted way to do this without adding to payload at time of writing: https://issuetracker.google.com/issues/72080883?pli=1
        uploaded_at = datetime.utcnow()

        if add_updated_at:
            dataframe.insert(0, '_etl_loaded_at', uploaded_at)

        logging.info(job_config)

        # don't allow config with a window on autodetect mode
        if ((use_bucket == True) & (window is not None) & (autodetect_mode is False)):
            self.upload_dataframe_to_table_with_partition_via_bucket(dataframe, uploaded_at, file_type,
                                             storage_client, bucketname, blobdir,
                                             bq_client, dataset_id, table_id, dataset_ref, table_ref, job_config, 
                                             partition_col, partition_type, lag, window)
        
        elif ((use_bucket == True) & (merge == True) & (autodetect_mode is False)):
            self.upload_dataframe_to_table_with_merge_via_bucket(
                                                dataframe=dataframe, uploaded_at=uploaded_at, file_type=file_type, merge_id_column=merge_id_column,
                                                storage_client=storage_client, bucketname=bucketname, blobdir=blobdir,
                                                bq_client=bq_client, dataset_id=dataset_id, table_id=table_id, table_ref=table_ref, job_config=job_config)

        # upload directly to bq and overwrite table
        elif (use_bucket == False):
            self.upload_dataframe_to_table(bq_client, dataframe, table_ref, job_config)
        
        # upload to bq via bucket and overwrite whole table
        elif ((use_bucket == True) & (window is None or autodetect_mode)):
            self.upload_dataframe_to_table_via_bucket(dataframe, uploaded_at, file_type,
                                             storage_client, bucketname, blobdir,
                                             bq_client, dataset_id, table_id, table_ref, job_config)
        
        else:
            return logging.exception('No option for configuration setup')


        # Clean up if autodetect mode
        if autodetect_mode:
            table_schema_to_json(bq_client, table_ref, f'schemas/{table_id}.json')
            if not keep_autodetect_table:
                logging.info("Dropping table used for autodetect {}".format(table_id))
                bq_client.delete_table(dataset_ref.table(table_id))



