from flask import Flask
import os
import logging

from utils import dict_from_yaml
from ingest import IngestYahoo


app = Flask(__name__)

# Parameters
@app.route("/", methods=['POST'])
def ingest(env: str = 'prod', overrides: dict = None):

    try:
        config = dict_from_yaml('config.yaml')
        ingest_yahoo = IngestYahoo(config)
        ingest_yahoo.run(env, overrides)
        ok = 'Ingested successfully'
        logging.info(ok)
        return ok
    except Exception as e:
        logging.exception("Failed to ingest ... try again later?")


if __name__ == "__main__":
    import argparse    

    parser = argparse.ArgumentParser(description='ingest stock data from yfinance API to Google Cloud Bigquery')
    parser.add_argument('--env', default='prod', help='Environment')
    parser.add_argument('--overrides', default='prod', help='Environment')

    args = parser.parse_args()

    ingest(args.env)

    if args.env == 'prod':
        app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))