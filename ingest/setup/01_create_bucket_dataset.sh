#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-yfinance-staging
LOCATION=EU

# Create bucket
gcloud storage buckets create gs://$BUCKET --location=$LOCATION # Remove if already created bucket

# set GCS bucket object TTL to delete blobs after 30 days (https://stackoverflow.com/questions/68071455/how-to-set-google-cloud-storage-bucket-gcs-file-object-expiration-ttl-using)
echo '    
{
    "rule":
    [
        {
            "action": {"type": "Delete"},
            "condition": {"age": 30}
        }
    ]
}
' > gcs_lifecycle.tmp
gsutil lifecycle set gcs_lifecycle.tmp gs://$BUCKET
rm gcs_lifecycle.tmp

# Create bq dataset
# bq --location=$LOCATION mk \
#     --dataset \
#     --description="Raw data ingested daily using yfinance api" \
#     --label=source:yfinance \
#     $PROJECT_ID:yfinance_raw