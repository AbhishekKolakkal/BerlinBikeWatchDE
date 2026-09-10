"""
BerlinBikeWatch — Streamlit dashboard.

Tile 1 (live map): direct NextBike API call, no Redshift.
Tiles 2-4 (history): Redshift queries, cached to limit RPU-hour billing.

Run:  uv run streamlit run dashboard/app.py
"""

import os
from datetime import datetime, timezone

import pandas as pd
import redshift_connector
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

NEXTBIKE_URL = "https://api.nextbike.net/maps/nextbike-live.json"
BERLIN_CITY_ID = 362

REDSHIFT_CONFIG = {
  "host": os.environ["REDSHIFT_HOST"],
  "port": int(os.environ["REDSHIFT_PORT"]),
  "database": os.environ["REDSHIFT_DATABASE"],
  "user": os.environ["REDSHIFT_USER"],
  "password": os.environ["REDSHIFT_PASSWORD"],
  "timeout": 20,  # socket connect timeout, so an unreachable Redshift fails the tile instead of hanging the page
}

EMPTY_COLOR = "#ff3b30"
OK_COLOR = "#34c759"


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def run_redshift_query(sql: str) -> pd.DataFrame:
  """Open a short-lived Redshift connection, run one query, return a DataFrame.

  Cached for 5 min (matching the pipeline poll interval) so page reloads and
  widget interactions don't re-bill RPU-hours on every rerender.
  """
  conn = redshift_connector.connect(**REDSHIFT_CONFIG)
  try:
    cursor = conn.cursor()
    cursor.execute(sql)
    df = cursor.fetch_dataframe()
    return df if df is not None else pd.DataFrame()
  finally:
    conn.close()


@st.cache_data(ttl=60)
def fetch_live_stations() -> tuple[pd.DataFrame, datetime]:
  """Fetch the live NextBike feed; return (stations_df, requested_at).

  Cached for 60s so reruns don't hammer the public API.
  """
  requested_at = datetime.now(timezone.utc)
  resp = requests.get(NEXTBIKE_URL, params={"city": BERLIN_CITY_ID}, timeout=10)
  resp.raise_for_status()
  places = resp.json()["countries"][0]["cities"][0]["places"]

  rows = [
    {
      "uid": p["uid"],
      "lat": p["lat"],
      "lng": p["lng"],
      "name": p["name"],
      "bikes_available_to_rent": p["bikes_available_to_rent"],
      "free_racks": p["free_racks"],
    }
    for p in places
    if p.get("spot") is True
  ]
  return pd.DataFrame(rows), requested_at


# ---------------------------------------------------------------------------
# Queries (validated upstream — kept close to the originals)
# ---------------------------------------------------------------------------

ANOMALIES_SQL = """
WITH anomaly_detection AS (
    SELECT uid,
        bikes_available_to_rent,
        LAG(bikes_available_to_rent) OVER (PARTITION BY uid ORDER BY fetched_at) AS previous_bikes_avail_to_rent,
        fetched_at,
        LAG(fetched_at) OVER (PARTITION BY uid ORDER BY fetched_at) AS previous_fetched_at,
        EXTRACT(EPOCH FROM (fetched_at - LAG(fetched_at) OVER (PARTITION BY uid ORDER BY fetched_at))) / 60 AS gap_minutes
    FROM stations
)
SELECT a.uid,
    s.name,
    a.previous_bikes_avail_to_rent,
    a.bikes_available_to_rent,
    (a.previous_bikes_avail_to_rent - a.bikes_available_to_rent) AS drop_size,
    a.fetched_at,
    a.gap_minutes
FROM anomaly_detection a
JOIN (SELECT DISTINCT uid, name FROM stations) s ON s.uid = a.uid
WHERE a.previous_bikes_avail_to_rent - a.bikes_available_to_rent >= 3
AND a.gap_minutes <= 15
ORDER BY a.fetched_at DESC
LIMIT 50;
"""

BUSIEST_SQL = """
WITH bike_history AS (
    SELECT uid, bikes,
        LAG(bikes) OVER (PARTITION BY uid ORDER BY fetched_at) AS previous_bikes,
        fetched_at
    FROM stations
),
changed_check AS (
    SELECT uid, bikes, previous_bikes, fetched_at,
        CASE WHEN bikes != previous_bikes THEN 'changed' ELSE 'same' END AS status
    FROM bike_history
),
activity_summary AS (
    SELECT uid, status, COUNT(*) AS change_count
    FROM changed_check
    GROUP BY uid, status
)
SELECT act_s.uid, s.name, act_s.change_count
FROM activity_summary act_s
JOIN (SELECT DISTINCT uid, name FROM stations) s ON s.uid = act_s.uid
WHERE act_s.status = 'changed'
ORDER BY act_s.change_count DESC
LIMIT 20;
"""

DEAD_SQL = """
WITH bike_history AS (
    SELECT uid, bikes,
        LAG(bikes) OVER (PARTITION BY uid ORDER BY fetched_at) AS previous_bikes,
        fetched_at
    FROM stations
    WHERE fetched_at >= GETDATE() - INTERVAL '1 day'
),
changed_check AS (
    SELECT uid, bikes, previous_bikes, fetched_at,
        CASE WHEN bikes != previous_bikes THEN 'changed' ELSE 'same' END AS status
    FROM bike_history
),
status_counts AS (
    SELECT uid, status, COUNT(*) AS cnt
    FROM changed_check
    GROUP BY uid, status
)
SELECT sc.uid, s.name
FROM status_counts sc
JOIN (SELECT DISTINCT uid, name FROM stations) s ON s.uid = sc.uid
GROUP BY sc.uid, s.name
HAVING COUNT(DISTINCT sc.status) = 1;
"""


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="BerlinBikeWatch", layout="wide")
st.title("BerlinBikeWatch")

# --- Tile 1: live station map -------------------------------------------------
st.subheader("Live station map")

try:
  stations_df, requested_at = fetch_live_stations()

  empty_mask = stations_df["bikes_available_to_rent"] == 0
  stations_df = stations_df.assign(
    color=empty_mask.map({True: EMPTY_COLOR, False: OK_COLOR})
  )

  m1, m2, m3 = st.columns(3)
  m1.metric("Stations", f"{len(stations_df):,}")
  m2.metric("Empty stations", f"{int(empty_mask.sum()):,}")
  m3.metric("Bikes available", f"{int(stations_df['bikes_available_to_rent'].sum()):,}")

  st.caption(
    f"Live NextBike data · requested {requested_at:%Y-%m-%d %H:%M:%S} UTC · "
    f"red = no bikes, green = bikes available"
  )
  st.map(stations_df, latitude="lat", longitude="lng", color="color")
except Exception as exc:  # noqa: BLE001 - surface any failure in the tile, keep the page alive
  st.error(f"Could not load live NextBike data: {exc}")

st.divider()

# --- Tiles 2-4: history from Redshift ---------------------------------------
tab_anom, tab_busy, tab_dead = st.tabs(
  ["Recent anomalies", "Busiest stations", "Dead stations (24h)"]
)

with tab_anom:
  st.caption("Stations that lost 3+ rentable bikes between two polls at most 15 min apart.")
  try:
    anomalies = run_redshift_query(ANOMALIES_SQL)
    if anomalies.empty:
      st.info("No anomalies found in the loaded history.")
    else:
      st.dataframe(
        anomalies[
          ["name", "uid", "fetched_at", "previous_bikes_avail_to_rent",
           "bikes_available_to_rent", "drop_size", "gap_minutes"]
        ],
        hide_index=True,
        width='stretch',
      )
  except Exception as exc:  # noqa: BLE001
    st.error(f"Redshift query failed: {exc}")

with tab_busy:
  st.caption("Stations ranked by how many polls recorded a change in bike count.")
  try:
    busiest = run_redshift_query(BUSIEST_SQL)
    if busiest.empty:
      st.info("No activity data in the loaded history.")
    else:
      chart_series = busiest.set_index("name")["change_count"].sort_values()
      st.bar_chart(chart_series, horizontal=True, width='stretch')
      st.dataframe(busiest, hide_index=True, width='stretch')
  except Exception as exc:  # noqa: BLE001
    st.error(f"Redshift query failed: {exc}")

with tab_dead:
  st.caption("Stations whose bike count never changed over the last 24 hours.")
  try:
    dead = run_redshift_query(DEAD_SQL)
    if dead.empty:
      st.info("No dead stations in the last 24 hours.")
    else:
      st.write(f"{len(dead):,} station(s) with no activity:")
      st.dataframe(dead, hide_index=True, width='stretch')
  except Exception as exc:  # noqa: BLE001
    st.error(f"Redshift query failed: {exc}")
