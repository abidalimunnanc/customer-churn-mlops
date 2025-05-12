import os
import requests
import boto3

# Define constants
DATA_URL = "https://raw.githubusercontent.com/blastchar/telco-customer-churn/master/WA_Fn-UseC_-Telco-Customer-Churn.csv"
LOCAL_PATH = "data/raw/telco_churn.csv"
S3_BUCKET = "your-bucket-name"
S3_KEY = "churn/raw/telco_churn.csv"

# Create folder if not exists
os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)

# Download dataset
def download_dataset():
    print("Downloading dataset...")
    response = requests.get(DATA_URL)
    with open(LOCAL_PATH, "wb") as f:
        f.write(response.content)
    print(f"Saved to {LOCAL_PATH}")

# Upload to S3
def upload_to_s3():
    print("Uploading to S3...")
    s3 = boto3.client('s3')
    s3.upload_file(LOCAL_PATH, S3_BUCKET, S3_KEY)
    print(f"Uploaded to s3://{S3_BUCKET}/{S3_KEY}")

if __name__ == "__main__":
    download_dataset()
    upload_to_s3()
