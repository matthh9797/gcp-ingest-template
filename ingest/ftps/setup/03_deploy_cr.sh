#!/bin/bash

NAME=ingest-ftps-daily 
SVC_ACCT=svc-ftps-ingest 
PROJECT_ID=$(gcloud config get-value project)
REGION=europe-west2 
SVC_EMAIL=${SVC_ACCT}@${PROJECT_ID}.iam.gserviceaccount.com
MEMORY=5Gi

#gcloud functions deploy $URL \
#    --entry-point ingest_flights --runtime python37 --trigger-http \
#    --timeout 540s --service-account ${SVC_EMAIL} --no-allow-unauthenticated

gcloud run deploy $NAME --region $REGION --source=$(pwd) \
    --platform=managed --service-account ${SVC_EMAIL} --no-allow-unauthenticated \
    --timeout 12m \

gcloud run services update $NAME --memory $MEMORY
