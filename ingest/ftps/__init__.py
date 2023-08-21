import os
import zipfile
import logging
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

from ingest.base import Ingest
from utils.ftps import ftps_download_file 
from gcp.storage import upload_file_to_bucket
from elevate.download import download_from_gcs


class IngestFtps(Ingest):
    def __init__(self, config: dict, extract_date: str, overrides: dict = None, tables: list = 'all') -> None:
        super().__init__(config, overrides)

        self.extract_date = datetime.strptime(extract_date, '%Y%m%d')
        self.download_strategy = config['download_strategy']
        self.env = config['env']
        self.run_type = config['run_type']
        if self.run_type == 'dev':
            load_dotenv()

        all_tables = self.config['source']['tables']
        self.tables = tables

        if self.tables == 'all':
            tables = all_tables
        else:
            tables = [t for t in all_tables if t['target_name'] in self.tables]

        self.config['source']['tables'] = tables


    def extract(self) -> dict:
        """
        Extract data from source system and structure as dictionary of dataframes with table name as key
        @return dictionary of dataframes
        """
        extract_date = self.extract_date
        env = self.env
        config_source = self.config['source']

        localdir = config_source['localdir']
        bucketname = config_source['bucketname'][env]

        if self.download_strategy == 'ftps':

            remotedir = config_source['remotedir']
            zipfilename = config_source['zipfilename']
            host = os.environ.get(config_source['host'])
            username = os.environ.get(config_source['username'])
            password = os.environ.get(config_source['password'])

            logging.info(f"Retrieving zipped file: {remotedir}/{zipfilename} from host: {host}")
            try: 
                ftps_download_file(host, username, password, remotedir, zipfilename, localdir)
            except FileNotFoundError:
                os.makedirs(localdir)
                ftps_download_file(host, username, password, remotedir, zipfilename, localdir)
            
        tables = self.config['source']['tables']

        tables_names = [t['target_name'] for t in tables]
        logging.info(f"Running extract on tables: {', '.join(tables_names)}")

        df_dict = {}

        for table in tables:

            file_type = table['file_type']
            extractfile = f"{table['target_name']}_{extract_date.strftime('%Y_%m_%d')}.{file_type}"
            
            blobdir = table['blobdir']
            blobname = f'{blobdir}/{extract_date.strftime("%Y%m%d")}:{extractfile}'

            # assign bucketname and blobname to table object 
            table['bucketname'] = bucketname
            table['blobname'] = blobname

            # Extract the raw files from the zipped file and upload to gcs bucket
            if self.download_strategy == 'ftps':

                zipfilepath = f'{localdir}/{zipfilename}'
                with zipfile.ZipFile(zipfilepath, mode="r") as archive:
                    logging.info(f'Extracting file: {localdir}/{extractfile} from zipped file: {zipfilepath}')
                    archive.extract(extractfile, path=localdir)

                logging.info(f'Uploading file to bucket: {bucketname}:{blobname}')
                upload_file_to_bucket(extractfile, localdir, bucketname, blobname)

                tempfile = f'{localdir}/{extractfile}'
                logging.info(f"Removing file from local directory: {tempfile}")
                os.remove(tempfile)

            logging.info(f'Downloading file from bucket: {bucketname}:{blobname}')
            df_dict[table['target_name']] = download_from_gcs(bucketname, blobname, table)

        return df_dict