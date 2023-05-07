import pandas as pd
from google.cloud import bigquery
import requests

from gcp import GcpConnector


class Ingest:
    """
    Base class for ingesting api data into GCP bigquery. This process assumes you are working with an 
    api endpoint that returns a json-like object that can be parsed into a dictionary of tables.

    There are two ways to use this class, if:
    1. The class methods work with you api, create a child class that inherits all methods
    2. The class methods do not work with your api, create a child class that overrides relevant methods
    """
    def __init__(self, config: dict) -> None:
        self.config = config


    # Helper Methods
    def get_endpoint_url(self, config_api: dict) -> str:
        """
        Get endpoint name from api configuration
        @param config_api api configuration
        """
        if 'suffix' in config_api:
            return f"{config_api['baseurl']}/{config_api['suffix']}/"
        elif 'baseurl' in config_api:
            return config_api['baseurl'] 
        else:
            return config_api['name']


    def download(self, endpoint: str) -> dict:
        """
        Retrieve data from api endpoint
        @param endpoint API endpoint
        @return json containing api output
        """
        r = requests.get(endpoint)
        if r.status_code == 404:
            print(f"Invalid api url provided: {endpoint}")
            return 404
        else:
            return r.json()
        

    def to_df_dict(self, json: dict, keys: list) -> dict:
        """ 
        Re-structure data from json format to dictionary of dataframes with table name as key
        @param json json output of fantasy.premierleague api call
        @param keys list of keys to convert to dataframes
        @return dictionary of dataframes 
        """
        df_dict = {}
        for key in keys:
            df_dict[key] = pd.DataFrame(json[key])
        return df_dict
    

    # Main Methods
    def extract(self) -> dict:
        """
        Extract data from source system and structure as dictionary of dataframes with table name as key
        @return dictionary of dataframes
        """
        config_api = self.config['api']

        dict = self.download(self.get_endpoint_url(config_api))

        if dict == 404:
            return dict
        else:
            return self.to_df_dict(dict, config_api['tables'])


    def transform(self, df_dict_raw: dict) -> dict:
        """
        Apply transformations to dataframes, if no transformations, return param
        @param df_dict raw dictionary of dataframes
        @return transformed dictionary of dataframes
        """
        df_dict_transformed = df_dict_raw
        return df_dict_transformed


    def load(self, df_dict_transformed: dict, gcp_connector: GcpConnector) -> None:
        """
        Load dictionary of dataframes to bigquery using upload configuration
        @param df_dict_transformed dictionary of dataframes from transform step
        @param gcp_connector instance of GcpConnector
        """
        config_gcp = self.config['gcp']

        upload_kwargs = config_gcp['upload']

        # Allow indidual tables to overwrite global upload config
        # if 'tables' in upload_kwargs:
        #     upload_kwargs.pop('tables')
        #     for dataframe_name, dataframe in df_dict_transformed.items():
        #             if dataframe_name in config_gcp['tables']:
        #                 for kwarg in config_gcp['tables'][dataframe_name]:
        #                     upload_kwargs[kwarg] = config_gcp['tables'][dataframe_name][kwarg]
        #         gcp_connector.upload(dataframe = dataframe, table_id = dataframe_name, **upload_kwargs)
                                    
        # else:
        for dataframe_name, dataframe in df_dict_transformed.items():
            gcp_connector.upload(dataframe = dataframe, table_id = dataframe_name, **upload_kwargs)

 

    def run(self, env: str, overrides: dict = None) -> None:
        """
        Ingestion process runner method to download, parse and upload api data into bigquery
        @param env environment determines which authentication method is used for GCP
        @param overrides overrides for default config, dict with same structure as config
        """
        if overrides is not None:
            from utils.helpers import update

            print("Overriding default config")
            update(self.config, overrides)

        config_api = self.config['api']
        config_gcp = self.config['gcp']

        # ETL Process

        ## Step 1: Download data as dictionary and parse to dictionary of dataframes
        print(f"Extracting data from endpoint: {self.get_endpoint_url(config_api)}")
        df_dict_raw = self.extract()

        ## Step 2: Transform dataframes if exists
        df_dict_transformed = self.transform(df_dict_raw)

        ## Step 3: Upload dictionary of dataframes to bq tables

        ### bq config only required for local development
        if env == 'dev':
            gcp_connector = GcpConnector(config_gcp)
        elif env == 'prod':
            gcp_connector = GcpConnector()
        else:
            return "Env must be dev or prod"

        return self.load(df_dict_transformed, gcp_connector)
