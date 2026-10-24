"""
Once a day, checks Postgres for the last loaded fetched_at pointer, finds any
S3 files newer than that pointer, and loads all of them into Redshift in a
single manifest-based COPY. Advances the pointer only if that COPY succeeds.
Repeats every POLL_INTERVAL_SECONDS.

Daily instead of every 5 min so Redshift Serverless can actually auto-pause
between loads, instead of being kept continuously active by frequent small
loads. The producer/consumer are unaffected and keep polling every 5 min, so
~288 files accumulate in S3 per run — loaded as one manifest COPY rather than
288 individual COPYs.
"""

import json
import time
import uuid
import boto3
import redshift_connector
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

POLL_INTERVAL_SECONDS = 86400  # once a day; see module docstring

S3_BUCKET = "berlinbikewatch-raw"
S3_PREFIX = "bike_stations/"
MANIFEST_PREFIX = "manifests/"
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

session = boto3.Session()
s3 = session.client("s3")


def get_redshift_connection():
  conn = redshift_connector.connect(**REDSHIFT_CONFIG)
  return conn, conn.cursor()


redshift_conn, redshift_cursor = get_redshift_connection()

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


def build_and_upload_manifest(keys):
  """Write a Redshift manifest listing every key, upload it, return its s3:// path.

  A unique, timestamped key (plus a short uuid4 suffix) means two runs could
  never collide on the same manifest key even if they somehow overlapped.
  """
  manifest = {
    "entries": [
      {"url": f"s3://{S3_BUCKET}/{key}", "mandatory": True}
      for key in keys
    ]
  }

  timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
  manifest_key = f"{MANIFEST_PREFIX}load_manifest_{timestamp}_{uuid.uuid4().hex[:8]}.json"

  s3.put_object(
    Bucket=S3_BUCKET,
    Key=manifest_key,
    Body=json.dumps(manifest).encode("utf-8"),
  )

  return f"s3://{S3_BUCKET}/{manifest_key}"


def load_files_to_redshift(keys):
  """One manifest, one COPY for all `keys`. Retries once on a dropped connection."""
  global redshift_conn, redshift_cursor

  manifest_path = build_and_upload_manifest(keys)
  print(f"  Manifest uploaded: {manifest_path} ({len(keys)} file(s))")

  copy_sql = f"""
    COPY stations
    FROM '{manifest_path}'
    IAM_ROLE '{IAM_ROLE_ARN}'
    MANIFEST
    FORMAT AS JSON 'auto'
    TIMEFORMAT AS 'auto';
  """

  try:
    redshift_cursor.execute(copy_sql)
    redshift_conn.commit()
  except Exception as e:
    print(f"  Redshift connection error ({e}), reconnecting and retrying...")
    redshift_conn, redshift_cursor = get_redshift_connection()
    redshift_cursor.execute(copy_sql)
    redshift_conn.commit()


def main():
  while True:
    try:
      pointer = get_pointer()
      new_files = list_new_files(pointer)

      if new_files:
        print(f"Found {len(new_files)} new file(s). Loading via a single manifest COPY...")
        keys = [key for key, _ in new_files]
        latest_fetched_at = max(fetched_at for _, fetched_at in new_files)

        try:
          load_files_to_redshift(keys)
          update_pointer(latest_fetched_at)
          print(f"Loaded {len(keys)} file(s) successfully. Pointer updated to: {latest_fetched_at}")
        except Exception as e:
          print(f"Manifest COPY failed, pointer NOT advanced: {e}")
      else:
        print("No new files. Waiting...")

    except Exception as e:
      print(f"Loop iteration failed: {e}")

    time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
  main()