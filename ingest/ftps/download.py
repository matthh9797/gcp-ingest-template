import pandas as pd


def download_from_gcs(bucketname: str, blobname: str) -> dict:
    """
    Download file from storage bucket and parse to python dict based on file type
    @param table_obj configuration for specific tabl
    """

    return pd.read_csv(f"gs://{bucketname}/{blobname}", quotechar='"', dtype='string')