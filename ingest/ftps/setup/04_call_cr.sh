#!/bin/bash

# same as deploy_cr.sh
NAME=ingest-ftps-daily

PROJECT_ID=$(gcloud config get-value project)

URL=$(gcloud run services describe ingest-ftps-daily --format 'value(status.url)')
FUNCTION=${URL}/ftps
echo $FUNCTION

echo {\"env\":\"prod\"\} > /tmp/message

curl -k -X POST $FUNCTION \
   -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
   -H "Content-Type:application/json" --data-binary @/tmp/message
