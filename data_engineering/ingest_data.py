import os
import requests
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Constants from environment
DATA_URL = "WA_Fn-UseC_-Telco-Customer-Churn.csv"
LOCAL_PATH = "WA_Fn-UseC_-Telco-Customer-Churn.csv"
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY = "raw/telco_churn.csv"

# Optional: Load AWS credentials manually (only if not using ~/.aws/credentials)
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# # Create local folder if it doesn't exist
# os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)

def download_dataset():
    print("📥 Downloading dataset...")
    response = requests.get(DATA_URL)
    with open(LOCAL_PATH, "wb") as f:
        f.write(response.content)
    print(f"✅ Saved to {LOCAL_PATH}")

def upload_to_s3():
    print("☁️ Uploading to S3...")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    s3.upload_file(LOCAL_PATH, S3_BUCKET, S3_KEY)
    print(f"✅ Uploaded to s3://{S3_BUCKET}/{S3_KEY}")

if __name__ == "__main__":
    # download_dataset()
    upload_to_s3()
