from google.cloud import bigquery
from datetime import datetime


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
        print('paritition_type must be YEAR, MONTH or DAY')


def table_schema_to_json(   bq_client: bigquery.Client,
                            table_ref: bigquery.table.TableReference,
                            file: str
                            # add_updated_at: bool = False
                            ) -> None:
    """Get schema of bigquery table using table ref"""
    table = bq_client.get_table(table_ref)

    table = bq_client.get_table(table_ref)
    schema = table.schema

    # if add_updated_at:
    #     updated_at = bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED", default_value_expression='CURRENT_TIMESTAMP()', description="Last time row was updated")
    #     schema.append(updated_at)

    return bq_client.schema_to_json(schema, file)
