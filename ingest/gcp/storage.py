from google.cloud import storage


def upload_file_to_bucket(src_filename, src_dir, bucket, blobname):
    client = storage.Client()

    bucket = client.get_bucket(bucket)
    blob = bucket.blob(blobname)
    blob.upload_from_filename(f'{src_dir}/{src_filename}')


def download_file_from_bucket(bucketname, blobname, destination_file_name):

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucketname)
    blob = bucket.blob(blobname)
    blob.download_to_filename(destination_file_name)