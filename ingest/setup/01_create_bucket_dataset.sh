#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
BUCKET=fpl-staging # with source api name
LOCATION=EU # e.g. EU

# Create bucket
gcloud storage buckets create gs://$BUCKET --location=$LOCATION --uniform-bucket-level-access

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
bq --location=$LOCATION mk \
    --dataset \
    --description="Raw data ingested daily from the fpl api" \
    --label=source:fantasy_premierleague \
    $PROJECT_ID:fpl_raw # with dataset id