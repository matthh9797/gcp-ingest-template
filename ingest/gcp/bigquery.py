import logging
from google.cloud import bigquery
from datetime import datetime
import numpy as np

from utils.io import json_to_dict

# Lookup Functions
def get_partition_type_from_str(partition_type: str) -> bigquery.TimePartitioningType:
    """
    get bigquery partition type object from partition keyword
    @param partition_type keyword (HOUR, DAY, MONTH, YEAR)
    @return bigquery.TimePartitioningType
    """
    lookup = {
        'DAY': bigquery.TimePartitioningType.DAY,
        'MONTH': bigquery.TimePartitioningType.MONTH,
        'YEAR': bigquery.TimePartitioningType.YEAR,
    }
    return lookup[partition_type]


def get_partition_format_from_str(partition_type: str) -> bigquery.TimePartitioningType:
    """
    get bigquery partition type object from partition keyword
    @param partition_type keyword (HOUR, DAY, MONTH, YEAR)
    @return str partition format
    """
    lookup = {
        'DAY': '%Y%m%d',
        'MONTH': '%Y%m',
        'YEAR': '%Y',
    }
    return lookup[partition_type]


def get_partition_range(dt: datetime, paritition_type):
    """
    Get the range of dates that a a parition type contains for a given datetime
    @param datetime
    @param partition_type must be DAY, MONTH or YEAR
    @return tuple containing start and end of date range
    """
    year = dt.year 
    month = dt.month
    day = dt.day
    if paritition_type == 'DAY':
        day = datetime(year, month, day)
        return day, day 
    elif paritition_type == 'MONTH':
        import calendar
        _, end = calendar.monthrange(year, month)
        som = datetime(year, month, 1)
        eom = datetime(year, month, end)
        return som, eom
    elif paritition_type == 'YEAR':
        soy = datetime(year, 1, 1)
        eoy = datetime(year, 12, 31)
        return soy, eoy
    else:
        logging.info('paritition_type must be YEAR, MONTH or DAY')


def get_source_format(file_type):
    """
    get bigquery source format from file type
    @param file_type (e.g. csv or json)
    """
    lookup = {
        'csv': 'CSV',
        'json': bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    }
    return lookup[file_type]


def bigquery_dtypes_to_python(bq_dtype: str) -> str:
    lookup = {
        'STRING': str,
        'TIMESTAMP': np.datetime64,
        'INTEGER': int,
        'FLOAT': float,
        'BOOLEAN': bool,
        'DATE': np.datetime64,
    }
    return lookup[bq_dtype]


def get_source_format(file_type):
    """
    get bigquery source format from file type
    @param file_type (e.g. csv or json)
    """
    lookup = {
        'csv': 'CSV',
        'json': bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    }
    return lookup[file_type]


# Helpers
def table_schema_to_json(   bq_client: bigquery.Client,
                            table_ref: bigquery.table.TableReference,
                            file: str
                            ) -> None:
    """Get schema of bigquery table using table ref"""
    table = bq_client.get_table(table_ref)

    table = bq_client.get_table(table_ref)
    schema = table.schema

    return bq_client.schema_to_json(schema, file)


def get_pd_columns_from_bq_schema(bq_schema_file):
    """convert bigquery schema file to pandas colnames and dtypes"""
    bq_schema = json_to_dict(bq_schema_file)
    pd_colnames = []
    pd_dtypes = []
    for field in bq_schema:
        pd_colnames.append(field['name'])
        pd_dtypes.append(bigquery_dtypes_to_python(field['type']))
    
    return pd_colnames, pd_dtypes 