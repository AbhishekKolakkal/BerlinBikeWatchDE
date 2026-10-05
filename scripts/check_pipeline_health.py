"""
BerlinBikeWatch — pipeline health check + Telegram alerting.

Built in direct response to three real incidents that went unnoticed for hours
to days: a producer crash (Sept 8, 1h46m unnoticed), a loader crash (Sept 18,
5 days unnoticed), and a Kafka OOM kill (Sept 26, 4 days unnoticed, root-caused
to unbounded Kafka UI memory growth).

Design: a single-shot script invoked repeatedly by cron — not a new
long-running service, not a new monitoring platform. If this script itself
fails, it just doesn't run until the next cron tick; it is deliberately not
a thing that itself needs monitoring.

Runs on the Contabo host directly (not containerized) — Check 1 needs `docker
ps`, and running on the host avoids mounting the Docker socket into a
container just for that.

Three checks: container health (docker ps), data freshness (Redshift), and
memory pressure (/proc/meminfo). State (healthy/unhealthy per check, last
alert time) lives in a small Postgres table in the same `bbw_metadata`
database already used for the loader's pointer, so a crashed/unhealthy check
alerts once on failure, once on recovery, and otherwise reminds at most every
REMINDER_INTERVAL_MINUTES while it stays down — not on every 5-minute tick.
"""

import os
import subprocess
from datetime import datetime, timezone

import psycopg2
import redshift_connector
import requests
from dotenv import load_dotenv

load_dotenv()

# --- Config -----------------------------------------------------------------

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

EXPECTED_CONTAINERS = [
  "kafka1", "bbw-postgres", "bbw-producer", "bbw-consumer", "bbw-loader", "bbw-dashboard",
]

STALENESS_THRESHOLD_MINUTES = 15   # pipeline polls every 5 min -> 3 missed polls
MEMORY_THRESHOLD_GIB = 1.0         # the Kafka-UI OOM incident dropped available memory to ~0
REMINDER_INTERVAL_MINUTES = 60     # re-alert at most this often while a check stays unhealthy

REDSHIFT_CONFIG = {
  "host": os.environ["REDSHIFT_HOST"],
  "port": int(os.environ["REDSHIFT_PORT"]),
  "database": os.environ["REDSHIFT_DATABASE"],
  "user": os.environ["REDSHIFT_USER"],
  "password": os.environ["REDSHIFT_PASSWORD"],
  "timeout": 15,
}

POSTGRES_CONFIG = {
  "host": os.environ["POSTGRES_HOST"],
  "port": int(os.environ["POSTGRES_PORT"]),
  "user": os.environ["POSTGRES_USER"],
  "password": os.environ["POSTGRES_PASSWORD"],
  "dbname": os.environ["POSTGRES_DB"],
}

CHECKS = [
  # (check_name, human label, function) -- function returns (healthy: bool, detail: str)
]


# --- The three checks ---------------------------------------------------------

def check_containers() -> tuple[bool, str]:
  """All EXPECTED_CONTAINERS must appear in `docker ps` with a status starting 'Up'."""
  try:
    result = subprocess.run(
      ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
      capture_output=True, text=True, timeout=10, check=True,
    )
  except Exception as exc:  # noqa: BLE001 - any failure here means "unhealthy", not a crash
    return False, f"Could not run 'docker ps': {exc}"

  statuses = {}
  for line in result.stdout.strip().splitlines():
    if not line:
      continue
    name, _, status = line.partition("\t")
    statuses[name] = status

  problems = []
  for name in EXPECTED_CONTAINERS:
    status = statuses.get(name)
    if status is None:
      problems.append(f"{name} is not running (not found)")
    elif not status.startswith("Up"):
      problems.append(f"{name} is not running (status: {status})")

  if problems:
    return False, "; ".join(problems)
  return True, f"All {len(EXPECTED_CONTAINERS)} containers running."


def check_data_freshness() -> tuple[bool, str]:
  """Latest `stations.fetched_at` in Redshift must be within STALENESS_THRESHOLD_MINUTES."""
  try:
    conn = redshift_connector.connect(**REDSHIFT_CONFIG)
  except Exception as exc:  # noqa: BLE001
    return False, f"Could not connect to Redshift: {exc}"

  try:
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(fetched_at) FROM stations;")
    row = cursor.fetchone()
  except Exception as exc:  # noqa: BLE001
    return False, f"Redshift query failed: {exc}"
  finally:
    conn.close()

  if not row or row[0] is None:
    return False, "No station data found in Redshift."

  latest = row[0]
  if latest.tzinfo is None:
    latest = latest.replace(tzinfo=timezone.utc)
  age_minutes = (datetime.now(timezone.utc) - latest).total_seconds() / 60

  if age_minutes > STALENESS_THRESHOLD_MINUTES:
    return False, f"Latest data is {age_minutes:.0f} min old (threshold: {STALENESS_THRESHOLD_MINUTES} min)."
  return True, f"Latest data is {age_minutes:.0f} min old."


def check_memory() -> tuple[bool, str]:
  """Available memory (from /proc/meminfo) must stay above MEMORY_THRESHOLD_GIB."""
  try:
    meminfo = {}
    with open("/proc/meminfo") as f:
      for line in f:
        key, _, rest = line.partition(":")
        meminfo[key.strip()] = rest.strip()
    available_kb = int(meminfo["MemAvailable"].split()[0])
  except Exception as exc:  # noqa: BLE001
    return False, f"Could not read /proc/meminfo: {exc}"

  available_gib = available_kb / (1024 ** 2)
  if available_gib < MEMORY_THRESHOLD_GIB:
    return False, f"Available memory is {available_gib:.2f} GiB (threshold: {MEMORY_THRESHOLD_GIB:.1f} GiB)."
  return True, f"Available memory is {available_gib:.2f} GiB."


CHECKS = [
  ("container_health", "Container health", check_containers),
  ("data_freshness", "Data freshness", check_data_freshness),
  ("memory_pressure", "Memory pressure", check_memory),
]


# --- Alert state (Postgres, bbw_metadata.alert_state) ------------------------

def ensure_state_table(pg_conn) -> None:
  cur = pg_conn.cursor()
  cur.execute("""
    CREATE TABLE IF NOT EXISTS alert_state (
      check_name TEXT PRIMARY KEY,
      healthy BOOLEAN NOT NULL,
      detail TEXT,
      unhealthy_since TIMESTAMPTZ,
      last_alert_at TIMESTAMPTZ
    );
  """)
  pg_conn.commit()
  cur.close()


def get_state(pg_conn, check_name: str) -> dict | None:
  cur = pg_conn.cursor()
  cur.execute(
    "SELECT healthy, detail, unhealthy_since, last_alert_at FROM alert_state WHERE check_name = %s;",
    (check_name,),
  )
  row = cur.fetchone()
  cur.close()
  if row is None:
    return None
  return {"healthy": row[0], "detail": row[1], "unhealthy_since": row[2], "last_alert_at": row[3]}


def set_state(pg_conn, check_name: str, *, healthy: bool, detail: str, unhealthy_since, last_alert_at) -> None:
  cur = pg_conn.cursor()
  cur.execute(
    """
    INSERT INTO alert_state (check_name, healthy, detail, unhealthy_since, last_alert_at)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (check_name) DO UPDATE SET
      healthy = EXCLUDED.healthy,
      detail = EXCLUDED.detail,
      unhealthy_since = EXCLUDED.unhealthy_since,
      last_alert_at = EXCLUDED.last_alert_at;
    """,
    (check_name, healthy, detail, unhealthy_since, last_alert_at),
  )
  pg_conn.commit()
  cur.close()


# --- Telegram -----------------------------------------------------------------

def send_telegram_alert(message: str) -> None:
  url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
  try:
    resp = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
    resp.raise_for_status()
  except Exception as exc:  # noqa: BLE001 - a failed alert must not crash the health check itself
    print(f"Failed to send Telegram alert: {exc}")


def format_failure(label: str, detail: str, now: datetime) -> str:
  return (
    f"🚨 BerlinBikeWatch Alert\n"
    f"Check: {label}\n"
    f"Detail: {detail}\n"
    f"Time: {now:%Y-%m-%d %H:%M} UTC"
  )


def format_still_down(label: str, detail: str, unhealthy_since: datetime, now: datetime) -> str:
  return (
    f"🚨 BerlinBikeWatch Still Down\n"
    f"Check: {label}\n"
    f"Detail: {detail}\n"
    f"Down since: {unhealthy_since:%Y-%m-%d %H:%M} UTC\n"
    f"Time: {now:%Y-%m-%d %H:%M} UTC"
  )


def format_recovery(label: str, detail: str, previous_detail: str | None, now: datetime) -> str:
  was = f" (was: {previous_detail})" if previous_detail else ""
  return (
    f"✅ BerlinBikeWatch Recovered\n"
    f"Check: {label}\n"
    f"Detail: {detail}{was}\n"
    f"Time: {now:%Y-%m-%d %H:%M} UTC"
  )


# --- Per-check decision logic --------------------------------------------------

def evaluate_and_alert(pg_conn, check_name: str, label: str, healthy: bool, detail: str) -> None:
  now = datetime.now(timezone.utc)
  state = get_state(pg_conn, check_name)
  was_healthy = True if state is None else state["healthy"]

  if healthy:
    if state is not None and not was_healthy:
      send_telegram_alert(format_recovery(label, detail, state["detail"], now))
    set_state(pg_conn, check_name, healthy=True, detail=detail, unhealthy_since=None, last_alert_at=None)
    return

  # unhealthy from here on
  if state is None or was_healthy:
    send_telegram_alert(format_failure(label, detail, now))
    set_state(pg_conn, check_name, healthy=False, detail=detail, unhealthy_since=now, last_alert_at=now)
    return

  last_alert_at = state["last_alert_at"]
  due_for_reminder = (
    last_alert_at is None
    or (now - last_alert_at).total_seconds() >= REMINDER_INTERVAL_MINUTES * 60
  )
  if due_for_reminder:
    send_telegram_alert(format_still_down(label, detail, state["unhealthy_since"], now))
    set_state(pg_conn, check_name, healthy=False, detail=detail,
              unhealthy_since=state["unhealthy_since"], last_alert_at=now)
  else:
    set_state(pg_conn, check_name, healthy=False, detail=detail,
              unhealthy_since=state["unhealthy_since"], last_alert_at=last_alert_at)


# --- Entry point ----------------------------------------------------------------

def main() -> None:
  try:
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    ensure_state_table(pg_conn)
  except Exception as exc:  # noqa: BLE001 - the state store itself is down; say so, then bail
    send_telegram_alert(
      f"⚠️ BerlinBikeWatch health-check script could not reach Postgres for state tracking: {exc}"
    )
    raise SystemExit(1)

  try:
    for check_name, label, check_fn in CHECKS:
      healthy, detail = check_fn()
      print(f"[{check_name}] healthy={healthy} detail={detail}")
      evaluate_and_alert(pg_conn, check_name, label, healthy, detail)
  finally:
    pg_conn.close()


if __name__ == "__main__":
  main()
