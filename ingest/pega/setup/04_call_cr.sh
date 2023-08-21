#!/bin/bash

# same as deploy_cr.sh
NAME=ingest-pega-daily

PROJECT_ID=$(gcloud config get-value project)

URL=$(gcloud run services describe ingest-pega-daily --format 'value(status.url)')
FUNCTION=${URL}/pega
echo $FUNCTION

# For testing downloading directly from storage bucket. Either replace with 'winscp' or send no message to download from WinSCP
# echo {\"download_strategy\":\"winscp\"\} > /tmp/message

curl -k -X POST $FUNCTION \
   -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
   -H "Content-Type:application/json" --data-binary @/tmp/message
