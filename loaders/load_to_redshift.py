"""
Continuously checks Postgres for the last loaded fetched_at pointer,
finds any S3 files newer than that pointer, loads them into Redshift
via COPY, and advances the pointer. Repeats every POLL_INTERVAL_SECONDS.
"""

import time
import boto3
import redshift_connector
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

POLL_INTERVAL_SECONDS = 10

S3_BUCKET = "berlinbikewatch-raw"
S3_PREFIX = "bike_stations/"
IAM_ROLE_ARN = os.environ["IAM_ROLE_ARN"]

POSTGRES_CONFIG = {
    "host": os.environ["POSTGRES_HOST"],
    "port": int(os.environ["POSTGRES_PORT"]),
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "dbname": os.environ["POSTGRES_DB"],
}

REDSHIFT_CONFIG = {
    "host": os.environ["REDSHIFT_HOST"],
    "port": int(os.environ["REDSHIFT_PORT"]),
    "database": os.environ["REDSHIFT_DATABASE"],
    "user": os.environ["REDSHIFT_USER"],
    "password": os.environ["REDSHIFT_PASSWORD"],
}

session = boto3.Session(profile_name="berlinbikewatch")
s3 = session.client("s3")

redshift_conn = redshift_connector.connect(**REDSHIFT_CONFIG)
redshift_cursor = redshift_conn.cursor()

pg_conn = psycopg2.connect(**POSTGRES_CONFIG)


def get_pointer():
  cur = pg_conn.cursor()
  cur.execute("SELECT last_loaded_fetched_at FROM load_pointer;")
  pointer = cur.fetchone()[0]
  cur.close()
  return pointer


def update_pointer(new_value):
  cur = pg_conn.cursor()
  cur.execute("UPDATE load_pointer SET last_loaded_fetched_at = %s;", (new_value,))
  pg_conn.commit()
  cur.close()


def list_new_files(pointer):
  new_files = []

  paginator = s3.get_paginator("list_objects_v2")
  for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
    for obj in page.get("Contents", []):
      key = obj["Key"]

      if not key.endswith(".jsonl"):
        continue

      filename = key.split("/")[-1]
      timestamp_str = filename.replace(".jsonl", "")

      try:
        file_fetched_at = datetime.strptime(timestamp_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
      except ValueError:
        print(f"Skipping unparsable filename: {filename}")
        continue

      if file_fetched_at > pointer:
        new_files.append((key, file_fetched_at))

  new_files.sort(key=lambda x: x[1])
  return new_files


def load_file_to_redshift(key):
  s3_path = f"s3://{S3_BUCKET}/{key}"
  copy_sql = f"""
    COPY stations
    FROM '{s3_path}'
    IAM_ROLE '{IAM_ROLE_ARN}'
    FORMAT AS JSON 'auto'
    TIMEFORMAT AS 'auto';
  """
  redshift_cursor.execute(copy_sql)
  redshift_conn.commit()


def main():
  while True:
    pointer = get_pointer()
    new_files = list_new_files(pointer)

    if new_files:
      print(f"Found {len(new_files)} new file(s). Loading...")
      latest_fetched_at = pointer

      for key, file_fetched_at in new_files:
        print(f"  Loading {key} ...")
        load_file_to_redshift(key)
        latest_fetched_at = max(latest_fetched_at, file_fetched_at)
        print(f"  Loaded {key} successfully.")

      update_pointer(latest_fetched_at)
      print(f"Pointer updated to: {latest_fetched_at}")
    else:
      print("No new files. Waiting...")

    time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
  main()