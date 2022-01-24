import os
from dotenv import load_dotenv

root = os.path.abspath(os.curdir)
load_dotenv(os.path.join(root, ".env"))

print(root)
BACKEND = os.getenv("BACKEND")
BUCKET_LANDING = os.getenv("BUCKET_LANDING")
BUCKET_STAGING = os.getenv("BUCKET_STAGING")
BUCKET_PROCESSED = os.getenv("BUCKET_PROCESSED")
BUCKET_LANDING_URL = os.path.join("s3://", BUCKET_LANDING)
BUCKET_STAGING_URL = os.path.join("s3://", BUCKET_STAGING)
BUCKET_PROCESSED_URL = os.path.join("s3://", BUCKET_PROCESSED)
KEY = os.getenv("KEY")
LOCAL_LANDING_DIR = os.getenv("LOCAL_LANDING_DIR")
LOCAL_STAGING_DIR = os.getenv("LOCAL_STAGING_DIR")
LOCAL_PROCESSED_DIR= os.getenv("LOCAL_PROCESSED_DIR")
CSV_PATH = (
    os.path.join(LOCAL_LANDING_DIR, KEY)
    if BACKEND == "local"
    else f"s3://{BUCKET_LANDING}/{KEY}"
)
STAGING_PATH = LOCAL_STAGING_DIR if BACKEND == "local" else BUCKET_STAGING_URL
PROCESSED_PATH = LOCAL_PROCESSED_DIR if BACKEND == "local" else BUCKET_PROCESSED_URL
DM_DATA_DIR = os.getenv("DM_DATA_DIR")