"""
BerlinBikeWatch — Streamlit dashboard.

Tile 1 (live map): direct NextBike API call, no Redshift.
Tiles 2-4 (history): Redshift queries, cached to limit RPU-hour billing.

Selecting a row in any history table focuses that station on the map
(Streamlit has no hover event, so this is a click, not a hover).

Run:  uv run streamlit run dashboard/app.py
"""

import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pydeck as pdk
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

BERLIN_TZ = ZoneInfo("Europe/Berlin")

EMPTY_RGB = [255, 59, 48]     # red   - no bikes
OK_RGB = [52, 199, 89]        # green - bikes available
HIGHLIGHT_RGB = [255, 214, 10]  # amber - focused station

BERLIN_VIEW = pdk.ViewState(latitude=52.52, longitude=13.405, zoom=10.5)


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


def try_query(sql: str) -> tuple[pd.DataFrame | None, str | None]:
  """Run a cached Redshift query, returning (df, error_message)."""
  try:
    return run_redshift_query(sql), None
  except Exception as exc:  # noqa: BLE001 - surfaced in the tile, page stays alive
    return None, str(exc)


def utc_to_berlin(series: pd.Series) -> pd.Series:
  """Timestamp column (UTC, tz-aware or naive) -> 'YYYY-MM-DD HH:MM:SS' in Berlin local time."""
  ts = pd.to_datetime(series, utc=True)
  return ts.dt.tz_convert(BERLIN_TZ).dt.strftime("%Y-%m-%d %H:%M:%S")


def humanize_age(dt_utc: datetime) -> str:
  """Rough 'how long ago' label for a UTC datetime."""
  minutes = int((datetime.now(timezone.utc) - dt_utc).total_seconds() // 60)
  if minutes < 1:
    return "just now"
  if minutes < 60:
    return f"{minutes} min ago"
  hours, minutes = divmod(minutes, 60)
  if hours < 24:
    return f"{hours}h {minutes}m ago"
  return f"{hours // 24}d ago"


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

LATEST_FETCH_SQL = "SELECT MAX(fetched_at) AS latest FROM stations;"

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
# Row selection -> focused station
# ---------------------------------------------------------------------------

def focus_from_table(table_key: str, df: pd.DataFrame) -> None:
  """on_select callback: store the clicked row's station as the map focus."""
  state = st.session_state.get(table_key) or {}
  rows = state.get("selection", {}).get("rows", [])
  if rows:
    row = df.iloc[rows[0]]
    st.session_state["focused"] = {"uid": int(row["uid"]), "name": str(row["name"])}
  else:
    st.session_state["focused"] = None


def history_table(df: pd.DataFrame, columns: list[str], table_key: str) -> None:
  """Render a selectable history table wired to the map focus."""
  view = df[columns]
  st.dataframe(
    view,
    hide_index=True,
    width="stretch",
    key=table_key,
    on_select=lambda: focus_from_table(table_key, view),
    selection_mode="single-row",
  )


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="BerlinBikeWatch", layout="wide")
st.title("BerlinBikeWatch")

focused = st.session_state.get("focused")

latest_df, latest_err = try_query(LATEST_FETCH_SQL)
anomalies, anomalies_err = try_query(ANOMALIES_SQL)
busiest, busiest_err = try_query(BUSIEST_SQL)
dead, dead_err = try_query(DEAD_SQL)

if latest_err:
  st.caption(f"⚠️ Warehouse freshness unknown: {latest_err}")
elif latest_df is not None and not latest_df.empty and pd.notna(latest_df.iloc[0, 0]):
  latest_utc = pd.to_datetime(latest_df.iloc[0, 0], utc=True).to_pydatetime()
  latest_berlin = latest_utc.astimezone(BERLIN_TZ)
  st.caption(
    f"Warehouse data current to {latest_berlin:%Y-%m-%d %H:%M:%S} (Berlin) · "
    f"{humanize_age(latest_utc)}"
  )
else:
  st.caption("Warehouse has no station data yet.")

if anomalies is not None and not anomalies.empty:
  anomalies = anomalies.assign(
    fetched_at_berlin=utc_to_berlin(anomalies["fetched_at"])
  )

# --- Tile 1: live station map ----------------------------------------------
st.subheader("Live station map")

try:
  stations_df, requested_at = fetch_live_stations()

  empty_mask = stations_df["bikes_available_to_rent"] == 0
  stations_df = stations_df.assign(
    color=[EMPTY_RGB if e else OK_RGB for e in empty_mask]
  )

  m1, m2, m3 = st.columns(3)
  m1.metric("Stations", f"{len(stations_df):,}")
  m2.metric("Empty stations", f"{int(empty_mask.sum()):,}")
  m3.metric("Bikes available", f"{int(stations_df['bikes_available_to_rent'].sum()):,}")

  layers = [
    pdk.Layer(
      "ScatterplotLayer",
      data=stations_df,
      get_position=["lng", "lat"],
      get_fill_color="color",
      get_radius=60,
      radius_min_pixels=3,
      radius_max_pixels=12,
      pickable=True,
    )
  ]
  view_state = BERLIN_VIEW

  focus_note = "red = no bikes, green = bikes available"
  if focused:
    match = stations_df[stations_df["uid"] == focused["uid"]]
    if not match.empty:
      spot = match.iloc[0]
      layers.append(
        pdk.Layer(
          "ScatterplotLayer",
          data=match,
          get_position=["lng", "lat"],
          get_fill_color=HIGHLIGHT_RGB,
          get_radius=110,
          radius_min_pixels=9,
          stroked=True,
          get_line_color=[255, 255, 255],
          line_width_min_pixels=2,
          pickable=True,
        )
      )
      view_state = pdk.ViewState(
        latitude=float(spot["lat"]), longitude=float(spot["lng"]), zoom=14
      )
      focus_note = (
        f"Focused: **{focused['name']}** "
        f"({int(spot['bikes_available_to_rent'])} bikes, {int(spot['free_racks'])} free racks)"
      )
    else:
      focus_note = (
        f"Focused station **{focused['name']}** (uid {focused['uid']}) is not in "
        f"the current live feed — can't place it on the map."
      )

  requested_berlin = requested_at.astimezone(BERLIN_TZ)
  st.caption(
    f"Live NextBike data · requested {requested_berlin:%Y-%m-%d %H:%M:%S} (Berlin) · {focus_note}"
  )
  st.pydeck_chart(
    pdk.Deck(
      layers=layers,
      initial_view_state=view_state,
      map_style="light",
      tooltip={"html": "<b>{name}</b><br/>Available: {bikes_available_to_rent}"},
    )
  )
  if focused:
    st.button("Clear focus", on_click=lambda: st.session_state.update(focused=None))
except Exception as exc:  # noqa: BLE001 - surface any failure in the tile, keep the page alive
  st.error(f"Could not load live NextBike data: {exc}")

st.divider()
st.caption("Click a row in any table below to focus that station on the map.")

# --- Tiles 2-4: history from Redshift -------------------------------------
tab_anom, tab_busy, tab_dead = st.tabs(
  ["Recent anomalies", "Busiest stations", "Dead stations (24h)"]
)

with tab_anom:
  st.caption("Stations that lost 3+ rentable bikes between two polls at most 15 min apart. Times in Berlin local time.")
  if anomalies_err:
    st.error(f"Redshift query failed: {anomalies_err}")
  elif anomalies.empty:
    st.info("No anomalies found in the loaded history.")
  else:
    history_table(
      anomalies,
      ["name", "uid", "fetched_at_berlin", "previous_bikes_avail_to_rent",
       "bikes_available_to_rent", "drop_size", "gap_minutes"],
      "anom_table",
    )

with tab_busy:
  st.caption("Stations ranked by how many polls recorded a change in bike count.")
  if busiest_err:
    st.error(f"Redshift query failed: {busiest_err}")
  elif busiest.empty:
    st.info("No activity data in the loaded history.")
  else:
    st.bar_chart(
      busiest.set_index("name")["change_count"].sort_values(),
      horizontal=True,
      width="stretch",
    )
    history_table(busiest, ["name", "uid", "change_count"], "busy_table")

with tab_dead:
  st.caption("Stations whose bike count never changed over the last 24 hours.")
  if dead_err:
    st.error(f"Redshift query failed: {dead_err}")
  elif dead.empty:
    st.info("No dead stations in the last 24 hours.")
  else:
    st.write(f"{len(dead):,} station(s) with no activity:")
    history_table(dead, ["name", "uid"], "dead_table")
