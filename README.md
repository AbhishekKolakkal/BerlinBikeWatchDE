# BerlinBikeWatch

A real-time data engineering pipeline that polls Berlin's NextBike bike-share system, streams it through Kafka, lands it in an S3 data lake, loads it into Redshift, and surfaces anomalies — stations that empty or fill unusually fast — through a Streamlit dashboard.

Built as a Data Engineering Zoomcamp portfolio project: ingestion → streaming → storage → warehouse → analysis → dashboard → production deployment, end to end, with the bugs and trade-offs documented rather than hidden.

**🔴 Live dashboard: [bbw.empathyminds.club](https://bbw.empathyminds.club/)** — running continuously against the real, current pipeline, not a static demo.

**→ The full build story — bugs, wrong turns, real debugging — is in [PROJECT_JOURNEY.md](PROJECT_JOURNEY.md).**
**→ How to deploy/redeploy it is in [DEPLOYMENT.md](DEPLOYMENT.md).**

---

## The hypothesis

A station near a festival, in good weather, empties out faster than its own baseline — an anomaly explainable by real-world context, not just "count dropped." The pipeline currently detects the anomaly side of that hypothesis (drops, surges, dead stations) from bike data alone; the event/weather correlation is scoped as future work (see [What's next](#whats-next)) after an honest investigation of Berlin's open events API surfaced real limitations — see [PROJECT_JOURNEY.md §6](PROJECT_JOURNEY.md#6-the-kulturdaten-investigation--real-limitations-honestly-found).

## Architecture

```
NextBike API (polled every 5 min)
        │
        ▼
   Kafka Producer ──────► Kafka Topic: bike_stations
   (Docker, KRaft)             │
                                ▼
                        Kafka Consumer
                 (batches by poll, filters
                  out free-floating bikes)
                                │
                                ▼
                       AWS S3 (raw data lake)
             bike_stations/dt=YYYY-MM-DD/hour=HH/*.jsonl
                                │
                                ▼
                  Loader (Postgres-backed pointer,
                   duplicate-safe COPY into Redshift)
                                │
                                ▼
                   AWS Redshift Serverless
                 (stations table, SQL analysis)
                                │
                                ▼
                    Streamlit Dashboard
             (live map, anomalies, busiest stations,
                    pipeline health)
```

`producer`, `consumer`, `loader`, and the dashboard (`streamlit-frontend`) are all the **same Docker image**, differing only in which script `command:` runs — see `Dockerfile` / `docker-compose.yaml`. The whole stack runs continuously on a VPS, independent of any local machine.

## Dashboard

Four tabs, all backed by cached Redshift queries (`@st.cache_data`, 5 min TTL) so page interactions don't rack up Redshift Serverless RPU-hours:

- **Live station map** — direct NextBike API call (not Redshift), color-coded by availability, click any history-table row to focus/recenter the map on that station
- **Recent anomalies** — stations that lost 3+ rentable bikes in ≤15 minutes
- **Busiest stations** — ranked by how often their bike count changed
- **Dead stations** — zero bike-count movement in the last 24h
- **Pipeline health** — live data-integrity self-check (rows-per-poll drift detection), a poll-gap/uptime report, and a manually-maintained incident log

## Tech stack

| Layer | Choice |
|---|---|
| Ingestion | Python, `requests` |
| Streaming | Kafka (KRaft mode, no Zookeeper) |
| Raw storage | AWS S3, JSON Lines, partitioned by `dt=`/`hour=` |
| Duplicate-load tracking | Postgres (a single pointer row, deliberately — see the journey doc for why) |
| Warehouse | AWS Redshift Serverless |
| Dashboard | Streamlit + pydeck |
| Packaging | `uv` |
| Deployment | Docker Compose, one shared image, pushed to AWS ECR, pulled onto a Contabo VPS |

## Repo layout

```
producers/       fetch_nextbike.py, produce_nextbike.py — poll NextBike, publish to Kafka
consumers/       consume_nextbike.py — batch by poll, write JSONL to S3
loaders/         load_to_redshift.py — Postgres pointer + COPY into Redshift
dashboard/       app.py — the Streamlit app
sql/             create_stations_table.sql, JSONPaths for Redshift COPY
analysis/        check_uid_stability.ipynb — exploratory notebook
docker-compose.yaml, Dockerfile
DEPLOYMENT.md    deploy/redeploy runbook + troubleshooting
PROJECT_JOURNEY.md   the full, honest build narrative
FeatureFixes.md  running backlog of feature/fix ideas
```

## Running it locally

```bash
uv sync
```

Create a `.env` in the repo root:

```bash
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=

REDSHIFT_HOST=
REDSHIFT_PORT=
REDSHIFT_DATABASE=
REDSHIFT_USER=
REDSHIFT_PASSWORD=

POSTGRES_HOST=
POSTGRES_PORT=
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=

IAM_ROLE_ARN=
KAFKA_BOOTSTRAP_SERVERS=
```

Then either run the full stack containerized:

```bash
docker compose up -d
```

or run pieces directly for development:

```bash
uv run producers/produce_nextbike.py     # needs Kafka running
uv run consumers/consume_nextbike.py
uv run loaders/load_to_redshift.py
uv run streamlit run dashboard/app.py
```

Note: the dashboard's Redshift tiles require your IP to be allowlisted in the Redshift Serverless security group — see [DEPLOYMENT.md](DEPLOYMENT.md#troubleshooting) if they hang or error.

## Notable findings from the data

- The busiest station in the dataset (`U Tierpark (Mobilitätsstation)`) saw 206 bike-count changes in ~4 days.
- Most detected "surges" (rapid bike-count *increases*) turn out to be NextBike's own rebalancing trucks restocking near-empty stations, not organic demand — identifiable by surges consistently starting from a previous count of 1–2 bikes.
- A manual check of a live surge against Berlin's Kulturdaten events API produced a plausible-looking match by keyword/borough that turned out to be a false positive on inspection — the matched venue was geographically distant from the actual station. Concrete evidence that real geocoding + distance calculation is required for event correlation, not keyword/borough matching.

More detail on all of these, including the SQL, is in [PROJECT_JOURNEY.md §4](PROJECT_JOURNEY.md#4-sql-analysis--what-i-built-and-found).

## What's next

1. Ingest Kulturdaten events/locations into Redshift properly (same pattern as NextBike)
2. Geocode event locations (Nominatim) for real distance-based matching
3. Embed event descriptions (Chroma) for semantic anomaly-to-event matching
4. A Flink job doing live, continuous correlation between the bike-station and events streams

Also open: `restart: unless-stopped` on the Docker Compose services, so a future unhandled crash recovers automatically instead of needing to be noticed manually.

## Honest limitations

- A ~1h46m data gap from Sept 8th is permanent — NextBike's API has no history endpoint to backfill from.
- Surge detection can't yet reliably separate operational rebalancing from genuine demand.
- Event correlation is investigated but not yet built into the pipeline.

See [PROJECT_JOURNEY.md §9](PROJECT_JOURNEY.md#9-honest-limitations) for the full list.
