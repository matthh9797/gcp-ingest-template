#!/bin/bash

# same as deploy_cr.sh
NAME={{REPLACE}} # e.g. ingest-yfinance-daily

PROJECT_ID=$(gcloud config get-value project)

URL=$(gcloud run services describe {{REPLACE}} --format 'value(status.url)')
echo $URL

# Feb 2015
echo {\"env\":\"prod\"\} > /tmp/message

curl -k -X POST $URL \
   -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
   -H "Content-Type:application/json" --data-binary @/tmp/message
