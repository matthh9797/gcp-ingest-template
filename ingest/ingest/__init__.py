from apis.yfinance import download_stock_history, transform_stock_history
from .base import Ingest

class IngestYahoo(Ingest):

    # Override default methods
    def extract(self) -> dict:
        """
        Extract data from source system and structure as dictionary of dataframes with table name as key
        @return dictionary of dataframes
        """
        config_api = self.config['api']
        config_stock_history = config_api['tables']['stock_history']

        extract_dict = {'stock_history': 
                            {'func': download_stock_history, 
                             'args': {'tickers': config_stock_history['tickers'], 'period': config_stock_history['period']}}
                        }

        df_dict_raw = {}
        for k, v in extract_dict.items():
            df_dict_raw[k] = v['func'](**v['args'])
        
        return df_dict_raw
    

    def transform(self, df_dict_raw: dict) -> dict:
        """
        Apply transformations to dataframes, if no transformations, return param
        @param df_dict raw dictionary of dataframes
        @return transformed dictionary of dataframes
        """
        df_dict_transformed = df_dict_raw
        transform_dict = {'stock_history': transform_stock_history}

        for k, f in transform_dict.items():
            df_dict_transformed[k] = f(df_dict_raw[k])

        return df_dict_transformed
    
