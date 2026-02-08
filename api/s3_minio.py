import os
import json
import boto3
from botocore.client import Config


def get_s3_client():
    """
    MinIO/S3 client using path-style addressing.
    Uses env vars:
      MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_REGION
    """
    endpoint = os.getenv("MINIO_ENDPOINT")
    if not endpoint:
        raise ValueError("MINIO_ENDPOINT is not set (e.g., http://localhost:9100)")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_bucket(bucket: str):
    """
    Create bucket if it doesn't exist.
    """
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)


def put_json(bucket: str, key: str, payload: dict):
    """
    Write JSON payload to s3://bucket/key
    """
    s3 = get_s3_client()
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )