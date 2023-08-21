import logging
import pandas as pd


# intended use case:
# nested = {
#     'id': '1', 'orders': [{'subid': '1', 'package': 'wlr'}, {'subid': '2', 'package': 'bb'}]
# }
# nested_key = 'orders'
# unnest_dict(nested, nested_key)
def unnest_dict(nested: dict, nested_key: str) -> list:
    """
    Unnest a dictionary key with array value 
    @param nested dictionary with nested value
    @param nested_key name of key with nested array value
    @return list of unnested dictionaries 
    """
    unnested = []
    for x in nested[nested_key]:
        row = nested.copy()
        row[nested_key] = x
        unnested.append(row)
    return unnested


# rows = [
#     nested,
#     {'id': '2', 'orders': {'subid': '1', 'package': 'bb'}}
# ]
# nested_key = 'orders'

# unnest(rows, nested_key)
def unnest(rows: list, nested_key: str) -> list:
    """
    Unnest a list of dictionaries with a nested key value
    @param a list of dictionaries with a potentially nested value
    @param nested_key name of key with nested array value
    @return an unnested list of dictionaries
    """
    unnested = []
    n_nested=0
    for row in rows:
        if isinstance(row[nested_key], list):
            newrows = unnest_dict(row, nested_key)
            unnested.extend(newrows)
            n_nested = n_nested+1
        else:
            unnested.append(row)

    logging.info(f"Unnested {n_nested} rows")
    return unnested


def flatten(rows, max_level=1, sep='_'):
    """"""
    return pd.json_normalize(rows, max_level=max_level, sep=sep).to_dict(orient='records')