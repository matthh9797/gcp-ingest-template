import os
import logging
import json
from datetime import date
from flask import Flask
from flask import request, escape
import google.cloud.logging

from pega import IngestPega
from elevate import IngestElevate
from utils import dict_from_yaml, verify_public_ip


client = google.cloud.logging.Client()
client.setup_logging()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

# Parameters
@app.route("/pega", methods=['POST'])
def ingest_pega():

    try:

        ip = verify_public_ip()
        logging.info(ip)

        # request arguments
        json = request.get_json(force=True) # https://stackoverflow.com/questions/53216177/http-triggering-cloud-function-with-cloud-scheduler/60615210#60615210
        download_strategy = escape(json['download_strategy']) if 'download_strategy' in json else 'winscp'
        increment_type = escape(json['increment_type']) if 'increment_type' in json else 'window'
        tables = escape(json['tables']) if 'tables' in json else 'all'
        env = escape(json['env']) if 'env' in json else 'prod'
        extract_date = escape(json['extract_date']) if 'extract_date' in json else date.today().strftime("%Y%m%d")

        overrides = {
            'download_strategy': download_strategy,
            'increment_type': increment_type,
            'tables': tables,
            'env': env
        }

        config = dict_from_yaml('pega/config.yaml')
        ingest_pega = IngestPega(config, extract_date, overrides)
        ingest_pega.run()
        ok = 'Ingested successfully'
        logging.info(ok)
        return ok
    except Exception as e:
        logging.exception("Failed to ingest ... try again later?")


@app.route("/elevate", methods=['POST'])
def ingest_elevate():

    try:

        # request arguments
        json = request.get_json(force=True) # https://stackoverflow.com/questions/53216177/http-triggering-cloud-function-with-cloud-scheduler/60615210#60615210
        download_strategy = escape(json['download_strategy']) if 'download_strategy' in json else 'ftps'
        tables = escape(json['tables']) if 'tables' in json else 'all'
        env = escape(json['env']) if 'env' in json else 'prod'
        extract_date = escape(json['extract_date']) if 'extract_date' in json else date.today().strftime("%Y%m%d")

        overrides = {
            'download_strategy': download_strategy,
            'tables': tables,
            'env': env
        }

        config = dict_from_yaml('elevate/config.yaml')
        ingest_elevate = IngestElevate(config, extract_date, overrides)
        ingest_elevate.run()
        ok = 'Ingested successfully'
        logging.info(ok)
        return ok
    except Exception as e:
        logging.exception("Failed to ingest ... try again later?")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))