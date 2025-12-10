import os
from dotenv import load_dotenv
import boto3
from supabase import create_client, Client

load_dotenv()

AWS_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))

URTICKET_API = os.getenv("URTICKET_API")
URTICKET_TOKEN = os.getenv("URTICKET_TOKEN")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

# Initialize clients
s3 = boto3.client("s3", region_name=AWS_REGION)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_latest_csv():
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=AWS_BUCKET)

    csv_files = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                csv_files.append(obj)

    if not csv_files:
        return None

    latest = max(csv_files, key=lambda x: x["LastModified"])
    key = latest["Key"]

    obj = s3.get_object(Bucket=AWS_BUCKET, Key=key)
    content = obj["Body"].read().decode("utf-8")
    return content
