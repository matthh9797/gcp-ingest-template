#!/bin/bash

# same as deploy_cr.sh
NAME={{REPLACE}} # e.g. ingest-yfinance-daily

PROJECT_ID=$(gcloud config get-value project)

URL=$(gcloud run services describe {{REPLACE}} --format 'value(status.url)')
FUNCTION=${URL}/{{REPLACE}} # with name of endpoint
echo $FUNCTION

# For testing downloading directly from storage bucket. Either replace with 'winscp' or send no message to download from WinSCP
echo {\"env\":\"prod\"\} > /tmp/message


curl -k -X POST $FUNCTION \
   -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
   -H "Content-Type:application/json" --data-binary @/tmp/message
