#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-{{REPLACE}}-staging # with source api name
LOCATION={{REPLACE}} # e.g. EU

# Create bucket
gcloud storage buckets create gs://$BUCKET # Remove if already created bucket

# Create bq dataset
bq --location=$LOCATION mk \
    --dataset \
    --description="{{REPLACE}}" \
    --label=source:yfinance \
    $PROJECT_ID:{{REPLACE}} # with dataset id