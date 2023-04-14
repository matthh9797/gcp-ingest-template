# Ingest Data into GCP

This repository provides a template process for batch data ingestion into GCP. The code expands on the ideas and code from the book, Data Science on GCP. The repository for the book is here: [https://github.com/GoogleCloudPlatform/data-science-on-gcp/tree/edition2/02_ingest/monthlyupdate](https://github.com/GoogleCloudPlatform/data-science-on-gcp/tree/edition2/02_ingest/monthlyupdate).

## Pre-Requisites

- The [gcloud CLI](https://cloud.google.com/sdk/docs/install) installed and configured.
- [Anaconda](https://www.anaconda.com/products/distribution) installed and configured.
- You have a billable GCP project setup.

## Getting Started

To get started, create a new repository using [this one](https://github.com/matthh9797/gcp-ingest-template) as a template. Then, create a conda environment for your project.

```bash
conda create --name <PROJECT_NAME> pip
pip install ipykernel
```

Install base packages using one of the following options.

### Option 1 - Use requirements.txt

```bash
pip install -r requirements.txt
```

### Option 2 - Manual install

```bash
pip install --upgrade google-cloud-bigquery
pip install pyarrow
pip install Flask
pip install pandas
pip install pathlib
pip install PyYAML
```

### Install additional packages, if required

Use `pip install XYZ` to install any additional packages required for your ingestion process. Then run `pip list --format=freeze > requirements.txt` to re-generate requirements.txt with packages versions.

**Note:**

- Ensure any additional packages are install with pip, not conda, to avoid a conflicting environment.
- Use `pip list --format=freeze > requirements.txt` instead of `pip install -r requirements.txt` to avoid [namespace error](https://stackoverflow.com/questions/62885911/pip-freeze-creates-some-weird-path-instead-of-the-package-version).

## Add Child of Ingest Base Class

Create an instance of the `Ingest` base class for your process overriding methods if necessary.

```python
# inside ingest/__init__.py
class ChildA(Ingest):
    def download():
        # If required, override methods
```

If you create multiple instances you may want to split them into there own modules `ingest/child_a.py`, `ingest/child_b.py` and import them into `ingest/__init__.py`.

## Test Using Local Development Notebook

Test your code in a local development environment, optionally, using development notebook.

To run the `load` method in a development environment you will require an access key with the correct credentials to be placed in your local Downloads folder. If so, skip to next step to set up service key.

## GCP Setup

First, update the `Dockerfile` to reflect your python version. Replace {{REPLACE}} with python version (e.g. `FROM python:3.11-slim`).

Run the setup bash scripts in the `setup/` directory one by one to setup a service key, deploy to cloud run, call the cloud run service to test and setup a cron job. Find and replace {{REPLACE}} with relevant options for GCP. For more info check out: [https://github.com/GoogleCloudPlatform/data-science-on-gcp/tree/edition2/02_ingest](https://github.com/GoogleCloudPlatform/data-science-on-gcp/tree/edition2/02_ingest)