#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-{{REPLACE}}-staging # with source api name
LOCATION={{REPLACE}} # e.g. EU

# Create bucket
gcloud storage buckets create gs://$BUCKET --location=$LOCATION --uniform-bucket-level-access

# set GCS bucket object TTL to delete blobs after 90 days (https://stackoverflow.com/questions/68071455/how-to-set-google-cloud-storage-bucket-gcs-file-object-expiration-ttl-using)
echo '    
{
    "rule":
    [
        {
            "action": {"type": "Delete"},
            "condition": {"age": 90}
        }
    ]
}
' > gcs_lifecycle.tmp
gsutil lifecycle set gcs_lifecycle.tmp gs://$BUCKET
rm gcs_lifecycle.tmp

# Create bq dataset
bq --location=$LOCATION mk \
    --dataset \
    --description="{{REPLACE}}" \
    --label=source:{{REPLACE}} \
    $PROJECT_ID:{{REPLACE}} # with dataset id