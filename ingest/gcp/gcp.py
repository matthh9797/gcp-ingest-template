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
    

    def upload(self, 
               dataframe: dict, 
               table_id: str,
               dataset_id: str,
               schema_path: str = None,
               bucketname: str = None,
               partition_col: str = None,
               partition_type: str = None,
               window: int = None, # number of days to go back and overwrite
               lag: int = 0, # Number of days,
               add_updated_at: bool = False,
               autodetect_mode = False,
               keep_autodetect_table = False,
               file_type: str = 'csv'
               ) -> None:
        """
        Upload dataframe to bigquery table. Run options include use of bucket and partitions.
        @param dataframe dataframe to upload
        @param table_id name of destination bigquery table
        @param dataset_id name of destination bigquery dataset
        @param schema_path path to bigquery schema
        @param bucketname if using bucket, name of bucket
        @param partition_col if using partition, name of partition column
        @param partition_type type of partition (DAY, MONTH, YEAR)
        @param window if using window, number of partition units to write into bq
        @param lag how many days to lag run date
        @param autodetect_mode If true, run the upload function with 100 rows of data to autodetect table schema
        @param keep_autodetect_table Do not automatically drop the table used for autodetecting table schema
        @param file_type specify file type for storage bucket (e.g. csv or json)
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
                print(f'Warning: Dataframe has only {nrow} rows. 100 rows used for autodetect.')
            else:
                print('Filtering first 100 rows of dataframe to autodetect schema')
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
        uploaded_at = datetime.now()

        if add_updated_at:
            dataframe.insert(0, 'updated_at', uploaded_at)

        # upload directly to bq and overwrite table
        if (use_bucket == False):
            upload_dataframe_to_table(bq_client, dataframe, table_ref, job_config)
        
        # upload to bq via bucket and overwrite whole table
        elif ((use_bucket == True) & (window is None or autodetect_mode)):
            if autodetect_mode:
                blobname = f'{uploaded_at.strftime("%Y%m%d")}:{table_id}'
            else:
                blobname = f'{uploaded_at.strftime("%Y%m%d")}:{table_id}'
            print(f'Uploading dataframe to bucket: gs://{bucketname}')
            gcslocation = upload_dataframe_to_bucket(storage_client, dataframe, bucketname, blobname, file_type)
            print(f'Uploading gcsfile from {gcslocation} to bigquery table: {dataset_id}:{table_id}')
            try:
                job_config.source_format = get_source_format(file_type) 
            except KeyError:
                return print(f'No upload method for file type {file_type}')     
            print(f'Uploading data from {gcslocation} to table {table_id}')
            upload_bucket_to_table(bq_client, gcslocation, table_ref, job_config)
        
        # don't allow config with a window on autodetect mode
        elif ((use_bucket == True) & (window is not None) & (autodetect_mode is False)):
            try:
                bq_client.get_table(dataset_ref.table(table_id))  # Make an API request.
                dataframe[partition_col] = dataframe[partition_col].dt.tz_localize(None)
                dt = uploaded_at - timedelta(days=lag) # Start lag days back if data lags the download date
                for _ in range(0, window):
                    start_date, end_date = get_partition_range(dt, partition_type)
                    dt = start_date - timedelta(days=1) # Next date in loop
                    partition_id = start_date.strftime(get_partition_format_from_str(partition_type))
                    print(f'range: ({start_date}, {end_date})')
                    blobname = f'{uploaded_at.strftime("%Y%m%d")}:{table_id}${partition_id}'
                    print(f'Uploading dataframe to gcslocation: gs://{bucketname}/{blobname}')
                    gcslocation = upload_dataframe_to_bucket(storage_client, 
                                                            dataframe.loc[dataframe[partition_col].between(start_date, end_date)], 
                                                            bucketname, 
                                                            blobname,
                                                            file_type)
                    table_ref = dataset_ref.table(f'{table_id}${partition_id}')
                    print(f'Uploading gcsfile from {gcslocation} to bigquery table: {dataset_id}:{table_id}${partition_id}')
                    job_config.skip_leading_rows=1
                    try:
                        job_config.source_format = get_source_format(file_type) 
                    except KeyError:
                        return print(f'No upload method for file type {file_type}') 
                    print(f'Uploading data from {gcslocation} to table {table_id}')
                    upload_bucket_to_table(bq_client, gcslocation, table_ref, job_config)
            except NotFound:
                return print(f'Table {dataset_id}.{table_id} not found. Create table first to use window uploads.')

        else:
            return print('No option for configuration setup')


        # Clean up if autodetect mode
        if autodetect_mode:
            table_schema_to_json(bq_client, table_ref, f'schemas/{table_id}.json')
            if not keep_autodetect_table:
                print("Dropping table used for autodetect {}".format(table_id))
                bq_client.delete_table(dataset_ref.table(table_id))



