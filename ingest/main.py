import os
import logging
from datetime import date
from flask import Flask
from flask import request, escape
import google.cloud.logging

from source_a import IngestA
from utils import dict_from_yaml


client = google.cloud.logging.Client()
client.setup_logging()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__)

# Parameters
@app.route("/source_a", methods=['POST'])
def ingest_a():

    try:

        # request arguments
        json = request.get_json(force=True) # https://stackoverflow.com/questions/53216177/http-triggering-cloud-function-with-cloud-scheduler/60615210#60615210
        increment_type = escape(json['increment_type']) if 'increment_type' in json else 'window'
        tables = escape(json['tables']) if 'tables' in json else 'all'
        env = escape(json['env']) if 'env' in json else 'prod'
        extract_date = escape(json['extract_date']) if 'extract_date' in json else date.today().strftime("%Y%m%d")

        overrides = {
            'increment_type': increment_type,
            'tables': tables,
            'env': env
        }

        config = dict_from_yaml('source_a/config.yaml')
        ingest_a = IngestA(config, extract_date, overrides)
        ingest_a.run()
        ok = 'Ingested successfully'
        logging.info(ok)
        return ok
    except Exception as e:
        logging.exception("Failed to ingest ... try again later?")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))