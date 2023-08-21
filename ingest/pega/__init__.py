import os
import zipfile
import logging
from paramiko import SSHException
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

from .download import download_table_a
from .transform import transform_table_a

from ingest.base import Ingest
from utils.winscp import verify_known_host, download_remote_file
from gcp.storage import upload_file_to_bucket



class IngestPega(Ingest):
    def __init__(self, config: dict, extract_date: str, overrides: dict = None, tables: list = 'all') -> None:
        super().__init__(config, overrides)

        self.extract_date = datetime.strptime(extract_date, '%Y%m%d')
        self.download_strategy = config['download_strategy']
        self.env = config['env']
        self.run_type = config['run_type']
        if self.run_type == 'dev':
            load_dotenv()
        
        increment_type = self.config['increment_type']
        self.config['gcp']['upload']['merge'] = self.config['gcp']['upload']['merge'][increment_type]

        all_tables = self.config['source']['tables']
        self.tables = tables

        if self.tables == 'all':
            tables = all_tables
        else:
            tables = [t for t in all_tables if t['target_name'] in self.tables]

        self.config['source']['tables'] = tables


    def download_file_from_server(self, filename: str, remotefile: str, localdir: str, host: str, username: str, private_key: str):
        """
        Download data from remote repository using WinSCP
        @param filename name of the file to be extracted from zipped folder after BIX extract
        @param remotefile name and path to remote zipped file
        @param localdir local directory to store data
        @param host name of pega host
        @param username username of pega user
        @param private_key private key of pega user
        """
        localfile = f"{localdir}/temp.zip"

        try:
            cnopts = verify_known_host(host, username, private_key)
        except SSHException:
            logging.info("Deleting .ssh/knownhosts to regenerate public key")  
            file = Path.joinpath(Path.home(), '.ssh/known_hosts')
            file.unlink()
            cnopts = verify_known_host(host, username, private_key)

        try:
            download_remote_file(remotefile, localfile, host, username, private_key, cnopts)
        except FileNotFoundError:
            os.makedirs(localdir)
            download_remote_file(remotefile, localfile, host, username, private_key, cnopts)
        with zipfile.ZipFile(localfile, mode="r") as archive:
            archive.extract(filename, path=localdir)


    def extract(self) -> dict:
        """
        Extract data from source system and structure as dictionary of dataframes with table name as key
        @return dictionary of dataframes
        """
        extract_date = self.extract_date
        env = self.env
        increment_type = self.config['increment_type']
        config_source = self.config['source']
        bucketname = config_source['bucketname'][env]

        # lookup for download function for individual tables
        extract_dict = {
            'table_a': download_table_a
        }

        df_dict = {}

        tables = self.config['source']['tables']

        tables_names = [t['target_name'] for t in tables]
        logging.info(f"Running extract on tables: {', '.join(tables_names)}")

        for table in tables:
            blobdir = table['blobdir']
            class_id = table['class_id'] 
            file_type = table['file_type']
            extract_rule = table['extract_rule'][increment_type]

            zippedfile = f"BIX_{class_id.replace('-', '_')}_{extract_rule}_{extract_date.strftime('%y%m%d')}_1.zip"
            extractfile = f"{class_id.replace('-', '_')}_{extract_rule}_1.{file_type}"
            logging.info(f'File to be extracted from zip folder: {extractfile}')

            blobname = f'{blobdir}/{extract_date.strftime("%Y%m%d")}:{extractfile}'

            table['bucketname'] = bucketname
            table['blobname'] = blobname

            if self.download_strategy == 'winscp':
                logging.info(f'Downloading zipped file: {zippedfile} from WinSCP containg file to extract: {extractfile}')
                remotefile = f"{config_source['remotedir']}/{zippedfile}"
                localdir = config_source['localdir']
                host = os.environ.get(config_source['host'][env])
                username = os.environ.get(config_source['username'][env])
                private_key = config_source['private_key'][env]
                logging.info(f'Downloading file from server: %s', remotefile)
                self.download_file_from_server(extractfile, remotefile, localdir, host, username, private_key)
                logging.info(f'Uploading file to bucket: %s', bucketname)
                upload_file_to_bucket(extractfile, localdir, bucketname, blobname)
            
            logging.info(f'Downloading file from bucket: %s', bucketname)
            df_dict[table['target_name']] = extract_dict[table['target_name']](table)

        return df_dict
    

    def transform(self, df_dict_raw: dict) -> dict:
        """
        Apply transformations to dataframes, if no transformations, return param
        @param df_dict raw dictionary of dataframes
        @return transformed dictionary of dataframes
        """
        df_dict_transformed = df_dict_raw

        config_tables = self.config['source']['tables']

        transformed_dict = {
            'table_a': transform_table_a
        }

        # add args to function
        for table in config_tables:
            table_name = table['target_name']
            if (table_name in transformed_dict) & ('transform_args' in table):
                transformed_dict[table_name]['args'] = table['transform_args']
        
        logging.info(transformed_dict)

        for k, f in transformed_dict.items():
            try:
                if hasattr(f, '__call__'):
                    df_dict_transformed[k] = f(df_dict_raw[k])
                else:
                    df_dict_transformed[k] = f['func'](df_dict_raw[k], **f['args'])
            except KeyError:
                continue

        return df_dict_transformed