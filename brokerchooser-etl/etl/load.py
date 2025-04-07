import pandas as pd
import logging
from pathlib import Path
import sqlite3
import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

# ----------------------
# Load environment variables
# ----------------------
dotenv_path = Path(__file__).parent / "aws.env"
load_dotenv(dotenv_path=dotenv_path)

# ----------------------
# Configure Logging
# ----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ----------------------
# Load to Storage
# ----------------------
def load_to_csv(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(exist_ok=True, parents=True)
    df.to_csv(output_path, index=False)
    logging.info(f"✅ Data saved to CSV: {output_path}")

def load_to_sqlite(df: pd.DataFrame, db_path: Path, table_name: str = "matched_data") -> None:
    db_path.parent.mkdir(exist_ok=True, parents=True)
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    logging.info(f"✅ Data written to SQLite table '{table_name}' at {db_path}")

def upload_to_s3(local_path: Path, bucket: str, s3_key: str):
    try:
        s3 = boto3.client("s3")
        s3.upload_file(str(local_path), bucket, s3_key)
        logging.info(f"☁️ Uploaded {local_path} to s3://{bucket}/{s3_key}")
    except Exception as e:
        logging.error(f"❌ Failed to upload {local_path} to S3: {e}")

# ----------------------
# Main Loader
# ----------------------
def load():
    base_path = Path(__file__).parent.parent.resolve()
    input_path = base_path / "output" / "matched_data.csv"
    db_path = base_path / "output" / "matched_data.sqlite"
    s3_bucket = os.getenv("S3_BUCKET")
    s3_key = os.getenv("S3_KEY") or f"etl/{datetime.now():%Y-%m-%d}/matched_data.csv"

    logging.info(f"📂 Checking for input file at: {input_path}")
    if not input_path.exists():
        logging.error(f"❌ Input file not found: {input_path}")
        return

    df = pd.read_csv(input_path, low_memory=False)
    logging.info(f"📥 Loaded {len(df)} rows from {input_path}")

    load_to_csv(df, base_path / "output" / "final_output.csv")
    load_to_sqlite(df, db_path)

    if s3_bucket:
        logging.info(f"📤 Preparing to upload with S3 key: {s3_key}")
        upload_to_s3(input_path, s3_bucket, s3_key)
    else:
        logging.info("🌐 Skipping S3 upload: no S3_BUCKET provided")

# For Airflow DAG
def load_data():
    load()

# For CLI
if __name__ == "__main__":
    load()
