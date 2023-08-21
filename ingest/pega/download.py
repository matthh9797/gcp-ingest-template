import xmltodict
from google.cloud import storage
import pandas as pd

from gcp.bigquery import get_pd_columns_from_bq_schema


def download_from_gcs(bucketname: str, blobname: str, file_type: str) -> dict:
    """
    Download file from storage bucket and parse to python dict based on file type
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucketname)
    blob = bucket.blob(blobname)
    if file_type == 'xml':
        with blob.open('r') as f:
            return xmltodict.parse(f.read(), attr_prefix='', cdata_key='')


def download_table_a(table_obj: dict) -> pd.DataFrame:
    """
    Download table_a table from storage bucket and parse into dataframe
    @table_obj object containing table specific configuration 
    """
    bucketname = table_obj['bucketname']
    blobname = table_obj['blobname']
    file_type = table_obj['file_type'] 
    schema_path = table_obj['schema_path']
    class_id = table_obj['class_id']

    data = download_from_gcs(bucketname, blobname, file_type)

    colnames, _ = get_pd_columns_from_bq_schema(schema_path)
    if file_type == 'xml':
        data_extracted = data[class_id]['item']
        return pd.DataFrame(data_extracted,columns=colnames,dtype='string')


