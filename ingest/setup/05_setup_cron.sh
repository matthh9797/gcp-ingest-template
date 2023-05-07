#!/bin/bash

# same as in setup_svc_acct.sh and call_cr.sh
NAME=ingest-yfinance-daily
PROJECT_ID=$(gcloud config get-value project)
SVC_ACCT=svc-yfinance-ingest
SVC_EMAIL=${SVC_ACCT}@${PROJECT_ID}.iam.gserviceaccount.com

SVC_URL=$(gcloud run services describe ingest-yfinance-daily --format 'value(status.url)')
echo $SVC_URL
echo $SVC_EMAIL

# note that there is no year or month. The service looks for next month in that case.
echo {\"env\":\"prod\"\} > /tmp/message
cat /tmp/message

gcloud scheduler jobs create http yfinancedaily \
       --description "Ingest yfinance using cloud run" \
       --schedule="52 02 * * *" --time-zone "Europe/London" \
       --uri=$SVC_URL --http-method POST \
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