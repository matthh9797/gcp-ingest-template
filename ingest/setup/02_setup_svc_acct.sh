#!/bin/bash

SVC_ACCT=svc-{{REPLACE}}-ingest # with source api name
PROJECT_ID=$(gcloud config get-value project)
BUCKET=${PROJECT_ID}-{{REPLACE}}-staging # with source api name
REGION={{REPLACE}} # e.g. europe-west2
SVC_PRINCIPAL=serviceAccount:${SVC_ACCT}@${PROJECT_ID}.iam.gserviceaccount.com

gsutil ls gs://$BUCKET || gsutil mb -l $REGION gs://$BUCKET
gsutil uniformbucketlevelaccess set on gs://$BUCKET

gcloud iam service-accounts create $SVC_ACCT --display-name "{{REPLACE}}"

# {{REPLACE}} REMOVE FROM SCRIPT IF NOT USING BUCKET FOR load.py METHOD
# make the service account the admin of the bucket
# it can read/write/list/delete etc. on only this bucket
gsutil iam ch ${SVC_PRINCIPAL}:roles/storage.admin gs://$BUCKET

# ability to create/delete partitions etc in BigQuery table
bq --project_id=${PROJECT_ID} query --nouse_legacy_sql \
  "GRANT \`roles/bigquery.dataOwner\` ON SCHEMA {{REPLACE}} TO '$SVC_PRINCIPAL' "

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member ${SVC_PRINCIPAL} \
  --role roles/bigquery.jobUser

# At this point, test running as service account
# download a json key from the console (temporarily)
# either add this to .gcloudignore and .gitignore or put it in a different directory!
# gcloud auth activate-service-account --key-file tempkey.json
# after this, go back to being yourself with gcloud auth login

# Make sure the sevice account can invoke cloud functions
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member ${SVC_PRINCIPAL} \
  --role roles/run.invoker
