# Ingest Data into GCP

This repository provides a template process for batch data ingestion into GCP. The code expands on the ideas and code from the book, Data Science on GCP. The repository for the book is here: [https://github.com/GoogleCloudPlatform/data-science-on-gcp/tree/edition2/02_ingest/monthlyupdate](https://github.com/GoogleCloudPlatform/data-science-on-gcp/tree/edition2/02_ingest/monthlyupdate).

## Pre-Requisites

- The [gcloud CLI](https://cloud.google.com/sdk/docs/install) installed and configured.
- [Anaconda](https://www.anaconda.com/products/distribution) installed and configured.
- You have a billable GCP project setup.

## Getting Started

To get started, create a new repository using [this one](https://github.com/matthh9797/gcp-ingest-template) as a template. Then, create a conda environment for your project.

```cmd
conda create --name <PROJECT_NAME> pip
conda activate <PROJECT_NAME>
```

Change directory into ingest using `cd ingest` and install base packages.

```cmd
pip install -r requirements.txt
```

### Install additional packages, if required

Use `pip install XYZ` to install any additional packages required for your ingestion process. Then run `pip list --format=freeze > requirements.txt` to re-generate requirements.txt with packages versions.

**Note:**

- Ensure any additional packages are install with pip, not conda, to avoid a conflicting environment.
- Use `pip list --format=freeze > requirements.txt` instead of `pip install -r requirements.txt` to avoid [namespace error](https://stackoverflow.com/questions/62885911/pip-freeze-creates-some-weird-path-instead-of-the-package-version).

## Add Child of Ingest Base Class

Create an instance of the `Ingest` base class for your process overriding methods if necessary. Optionally, use development notebook.

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

Run the setup bash scripts in the `setup/` directory one by one to setup a service key, deploy to cloud run, call the cloud run service to test and setup a cron job. [**Note:** It is important that you run the deploy script from the ingest dir] Find and replace {{REPLACE}} with relevant options for GCP. For more info check out: [https://github.com/GoogleCloudPlatform/data-science-on-gcp/tree/edition2/02_ingest](https://github.com/GoogleCloudPlatform/data-science-on-gcp/tree/edition2/02_ingest)

Once, deployed you can easily setup continuous deployment with the UI: [cloud.google.com/run/docs/continuous-deployment-with-cloud-build](https://cloud.google.com/run/docs/continuous-deployment-with-cloud-build).