#!/bin/bash

NAME=ingest-ftps-daily
PROJECT_ID=$(gcloud config get-value project)
SVC_ACCT=svc-ftps-ingest
SVC_EMAIL=${SVC_ACCT}@${PROJECT_ID}.iam.gserviceaccount.com
REGION=europe-west2 # e.g. europe-west2

SVC_URL=$(gcloud run services describe ingest-ftps-daily --format 'value(status.url)')
SVC_FUNCTION=${SVC_URL}/ftps
echo $SVC_FUNCTION
echo $SVC_EMAIL

# note that there is no year or month. The service looks for next month in that case.
echo {\"env\":\"prod\"\} > /tmp/message
cat /tmp/message

gcloud scheduler jobs create http ftpsdaily \
       --description "Ingest ftps data using cloud run" \
       --location=${REGION} \
       --attempt-deadline="15m" \
       --schedule="34 11 * * 1-5" --time-zone "Europe/London" \
       --uri=$SVC_FUNCTION --http-method POST \
       --oidc-service-account-email $SVC_EMAIL --oidc-token-audience=$SVC_URL \
       --max-backoff=7d \
       --max-retry-attempts=5 \
       --max-retry-duration=2d \
       --min-backoff=12h \
       --headers="Content-Type=application/json" \
       --message-body-from-file=/tmp/message


# To try this out, go to Console and do two things:
#    in Service Accounts, give yourself the ability to impersonate this service account (ServiceAccountUser)
#    in Cloud Scheduler, click "Run Now"