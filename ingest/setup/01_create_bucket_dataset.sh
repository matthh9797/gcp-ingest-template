#!/bin/bash

PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-yfinance-staging
LOCATION=EU

# Create bucket
gcloud storage buckets create gs://$BUCKET # Remove if already created bucket

# Create bq dataset
bq --location=$LOCATION mk \
    --dataset \
    --description="Raw data ingested daily using yfinance api" \
    --label=source:yfinance \
    $PROJECT_ID:yfinance_raw