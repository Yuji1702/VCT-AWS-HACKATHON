import requests
import json
import gzip
import time
from io import BytesIO
import boto3
import os

# Set your AWS credentials as environment variables
os.environ['AWS_ACCESS_KEY_ID'] = 'ACESS_KEY'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'SECRET_ACCESS_KEY'
os.environ['AWS_DEFAULT_REGION'] = 'REGION'

S3_BUCKET_URL = "URL"
S3_BUCKET_NAME = "NAME"  # Replace with your S3 bucket name
AWS_REGION = "REGION"  # Replace with your S3 bucket region

# Initialize S3 client
s3_client = boto3.client('s3', region_name=AWS_REGION)

# (game-changers, vct-international, vct-challengers)
LEAGUE = "vct-international"

# (2022, 2023, 2024)
YEAR = 2024

def download_gzip_and_upload_to_s3(file_name):
    """
    Download the gzip file, extract it, and upload the JSON directly to S3.
    """
    remote_file = f"{S3_BUCKET_URL}/{file_name}.json.gz"
    response = requests.get(remote_file, stream=True)

    if response.status_code == 200:
        gzip_bytes = BytesIO(response.content)
        with gzip.GzipFile(fileobj=gzip_bytes, mode="rb") as gzipped_file:
            # Read the decompressed data directly as bytes
            json_bytes = gzipped_file.read()

            # Upload to S3
            upload_bytes_to_s3(json_bytes, f"{file_name}.json")
            print(f"{file_name}.json uploaded to S3")
        return True
    elif response.status_code == 404:
        # Ignore if the file doesn't exist
        return False
    else:
        print(f"Failed to download {file_name}: {response}")
        return False

def upload_bytes_to_s3(data, s3_file_name):
    """
    Upload byte data directly to S3.
    """
    try:
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_file_name, Body=data)
        print(f"Uploaded {s3_file_name} to S3 bucket {S3_BUCKET_NAME}")
    except Exception as e:
        print(f"Failed to upload {s3_file_name} to S3: {str(e)}")

def download_esports_files():
    """
    Download and upload esports-related files directly to S3.
    """
    esports_data_files = ["leagues", "tournaments", "players", "teams", "mapping_data"]

    for file_name in esports_data_files:
        download_gzip_and_upload_to_s3(f"{LEAGUE}/esports-data/{file_name}")

def download_games():
    """
    Download and upload game data directly to S3.
    """
    start_time = time.time()

    # Download the mapping_data.json first to get the game mappings
    local_mapping_file = f"{LEAGUE}/esports-data/mapping_data.json"
    response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=local_mapping_file)
    mappings_data = json.loads(response['Body'].read())

    game_counter = 0

    for esports_game in mappings_data:
        s3_game_file = f"{LEAGUE}/games/{YEAR}/{esports_game['platformGameId']}"
        response = download_gzip_and_upload_to_s3(s3_game_file)

        if response:
            game_counter += 1
            if game_counter % 10 == 0:
                print(f"----- Processed {game_counter} games, current run time: {round((time.time() - start_time)/60, 2)} minutes")

if __name__ == "__main__":
    download_esports_files()
    download_games()