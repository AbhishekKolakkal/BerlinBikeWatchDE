# BerlinBikeWatch — Project Journey

**A real-time data engineering pipeline correlating Berlin bike-share activity with anomaly detection, built end-to-end: ingestion → streaming → storage → warehouse → analysis → dashboard → production deployment.**

This document isn't a polished feature list. It's a chronological account of how this was actually built — including the bugs, the wrong turns, the trade-offs, and what I learned from each one. If you're evaluating this project, the mistakes are as much the point as the working parts.

---

## 1. What This Project Is

BerlinBikeWatch polls Berlin's NextBike bike-share system every few minutes, streams the data through Kafka, lands it in S3 as a raw data lake, loads it into Redshift, and runs continuous analysis to detect anomalies — stations that suddenly empty or suddenly fill. The original hypothesis: these anomalies often correlate with real-world events (festivals, weather) happening nearby.

The project is explicitly built to demonstrate real data engineering practice — not a toy pipeline, but one with genuine streaming infrastructure, cloud storage, a proper warehouse, containerized deployment, and honest engineering trade-offs at every layer.

---

## 2. Architecture

```
NextBike API (polled every 5 min)
        │
        ▼
   Kafka Producer  ──────► Kafka Topic: bike_stations
   (Docker, KRaft mode)         │
                                 ▼
                         Kafka Consumer
                    (batches by poll, filters
                     free-floating bikes)
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

Everything above — Kafka, Postgres, Producer, Consumer, Loader —
runs containerized (Docker Compose) on a Contabo VPS, independent
of any local machine. Images are built locally, pushed to AWS ECR,
and pulled on the server using short-lived tokens (no long-lived
AWS credentials stored on the remote server).
```

---

## 3. The Build Journey

### 3.1 Understanding the data source, first

Before writing any code, I spent real time just mapping NextBike's live JSON endpoint (`api.nextbike.net/maps/nextbike-live.json?city=362`) field by field — country level, city level, station level. Fields like `booked_bikes`, `set_point_bikes`, and `refresh_rate` aren't officially documented anywhere, so I inferred their meaning from context and cross-checked against a second city's response to confirm the pattern held.

**Early mistake:** I initially assumed `places[]` only contained real stations. It actually contains a mix of real docked stations *and* individual free-floating bikes (`spot: false`, name like `"BIKE 17421"`, zero rack capacity). This wasn't caught until a consumer script started printing `BIKE ####` names instead of real station names — a good reminder to actually look at real data rather than assume a schema based on one example.

### 3.2 Local pipeline: fetch → poll → produce

I built this in deliberately small, testable steps:
- `fetch_nextbike.py` — a one-shot fetch-and-parse script
- `poll_nextbike.py` — added a polling loop and local file writes, partitioned `dt=/hour=` to mirror the eventual S3 layout
- `produce_nextbike.py` — replaced local writes with Kafka production, one message per station (not one bundled message per poll)

**Key design decision:** one Kafka message per station, not one big message per poll. This matches the eventual database schema (one row per station per poll), enables per-station processing, and gives a meaningful partition key (`uid`). The trade-off is more `.produce()` calls per poll (~4,500), but at a 5-minute interval this is negligible — Kafka is built for far higher throughput than this.

**A genuine bug from these early scripts:** a loop where `list.append()` was nested one indentation level too deep, inside the field loop instead of the outer place loop — resulting in 19 duplicate copies of each station record instead of one. Classic "build the whole item, then append once" lesson.

### 3.3 Kafka — permissions, listeners, and a real debugging chain

Standing up Kafka locally (Docker, KRaft mode — no Zookeeper, per current best practice) surfaced two genuinely instructive bugs:

**Bug 1 — data directory permissions.** Kafka's container runs as `uid 1000`; Docker created the mounted volume folder owned by `root`. Fix: `chown -R 1000:1000 ./kafka1/data`. This exact bug recurred later on the Contabo server too, since it was a fresh volume there as well.

**Bug 2 — listener configuration.** This took real work to diagnose. Kafka needs separate listeners for different audiences: `PLAINTEXT` (for other containers on the same Docker network), `CONTROLLER` (internal KRaft coordination), and a custom `EXTERNAL` listener (for clients outside Docker — my host machine, and later, other remote clients). The actual bug: `KAFKA_LISTENERS` had `PLAINTEXT` bound to the container's hostname instead of `0.0.0.0`, meaning Kafka was only accepting connections addressed exactly to `kafka1`, not `localhost` — even from inside the same container. Understanding the distinction between *binding* (`KAFKA_LISTENERS`) and *advertising* (`KAFKA_ADVERTISED_LISTENERS`) was the key insight that resolved this.

### 3.4 S3 and the JSON Lines lesson

The consumer batches messages by poll (detecting a new poll via a `fetched_at`-comparison state machine) and writes one file per poll to S3, partitioned `dt=/hour=`.

**A real, non-obvious bug:** my first version wrote each batch as `{"places": [...]}` — one JSON object wrapping an array. This structure is completely reasonable to *write*, but Redshift's `COPY` command cannot parse nested arrays inside a wrapping object — it expects either one JSON object per line, or a top-level array. The fix was switching to genuine **JSON Lines** format: one flat station object per line, no wrapper at all. This is also just a better format in general — many tools process data line-by-line specifically because of this constraint, so it wasn't purely a Redshift workaround, it was adopting a more broadly compatible format.

### 3.5 Redshift — schema design and a real networking saga

I designed the `stations` table deliberately: `INTEGER` for `uid` (after checking real observed values exceeded `SMALLINT`'s range), `DECIMAL(9,6)` for lat/lng (matching GPS-precision convention), `TIMESTAMPTZ` for `fetched_at`, and a composite primary key `(uid, fetched_at)` since `uid` alone repeats every poll — this is a time-series table, not a current-state table.

**The `COPY` command itself needed real debugging:**
- `TIMEFORMAT AS 'auto'` was required because Redshift's default timestamp parser doesn't handle ISO 8601's `T` separator and microseconds out of the box.
- The nested-JSON issue above required switching to JSON Lines before `COPY` would even load multi-record files correctly.

**The networking saga:** getting my own laptop, and later Contabo, to actually reach Redshift required two separate fixes working together — a security group inbound rule scoped to my specific IP (which changes periodically, since I don't have a static IP — a real, recurring annoyance I eventually just learned to expect and re-check), *and* a completely separate "Publicly accessible" toggle on the Redshift Serverless workgroup itself, which was off by default and easy to miss. Diagnosing this required working from first principles: `nc -zv` to test raw TCP connectivity, isolating whether it was a network-level problem before ever looking at credentials.

### 3.6 Duplicate-prevention — a genuine architecture decision

Once `COPY` needed to run repeatedly (not once, by hand), duplicate-loading became a real risk: Redshift does **not** enforce primary key uniqueness automatically, so re-running `COPY` against an already-loaded file silently creates duplicate rows.

I considered two designs:
- **A per-file tracking table** (`loaded_files`, one row per successfully loaded S3 key) — more robust, correctly handles out-of-order arrivals and partial failures, but more bookkeeping.
- **A single pointer** (`load_pointer`, one row, "last successfully loaded timestamp") — simpler, but blind to out-of-order files or partial failures.

I chose the **pointer**, consciously accepting the simpler, less failure-resistant option — with the explicit reasoning that I'd upgrade to the tracking-table approach only if a real failure mode actually showed up in practice. This is a legitimate trade-off, not a shortcut I was unaware of.

I also deliberately chose to keep the pointer in **Postgres**, not Redshift — after weighing the trade-off explicitly: Postgres avoids any Redshift query cost for a value checked on every loader cycle, at the cost of losing atomicity between "load succeeded" and "pointer updated" (two separate systems, not one transaction). For a personal project, the cost savings won out.

### 3.7 A real production incident

After roughly three days of unattended, correct operation on Contabo, the producer crashed. Root cause: an unhandled `requests.exceptions.ConnectTimeout` — a single transient NextBike API timeout — with no `try/except` around the fetch call, so the entire container exited and sat dead for about **1 hour 46 minutes** before I noticed and diagnosed it.

**The fix:** wrap the fetch in a `try/except`, log the failure, and `continue` to the next poll rather than crashing. **The gap this incident left in the data is genuinely unrecoverable** — NextBike's API has no historical replay, so that window is permanently missing from the dataset. I chose to document this honestly rather than pretend it didn't happen; the gap is directly visible and quantifiable via a `LAG()`-based query I built specifically to detect polling gaps (see §4).

I did **not** yet add `restart: unless-stopped` to the Docker Compose services — a clear, identified, still-open improvement that would have auto-recovered from this specific failure within seconds rather than requiring me to notice it manually.

---

## 4. SQL Analysis — What I Built and Found

All of the following were built from first principles in Redshift's Query Editor, learning window functions (`LAG`, `PARTITION BY`, `OVER`), `HAVING` vs. `WHERE`, CTEs, and `JOIN`s along the way — including several real Redshift-specific quirks (e.g. `DATEDIFF` requiring bare, unquoted datepart keywords; window function results needing to be resolved in a CTE before being filtered in an outer query).

- **Gap/outage detection** — a `LAG()`-based query comparing each poll's timestamp to the previous one, per the whole dataset, surfacing the exact Sept 8th outage described above (~1h46m) as the largest gap in the data.
- **Dead-station detection** — using `LAG()` + `PARTITION BY uid`, a `CASE WHEN` to label each row `'changed'`/`'same'`, and `HAVING COUNT(DISTINCT status) = 1` to find stations with zero bike-count movement across a full day.
- **Busiest-station ranking** — same `changed`/`same` pattern, aggregated with `COUNT()` and joined back to station names, surfacing `U Tierpark (Mobilitätsstation)` as the clear busiest station in the dataset (206 changes over ~4 days).
- **Anomaly (drop) detection** — the "sudden bike drop" query, refined specifically to exclude false positives caused by the Sept 8th outage: filtering to only count a drop as anomalous if the time gap to the previous poll was ≤15 minutes, since a large drop across a multi-hour gap is not actually sudden, just an artifact of missing data.
- **Surge detection** — the mirror of the drop query (bikes *increasing* rapidly). This surfaced a genuinely important, honest finding: **the majority of detected "surges" are very likely NextBike's own rebalancing operations** (trucks restocking near-empty stations), not organic demand — identified by the consistent pattern of surges starting from `previous_bikes_available ≤ 1-2`. I separated these from a smaller set of surges starting from an already-healthy stock level, which are more plausible candidates for genuine, event-driven demand.

---

## 5. Deployment — From Laptop to Production

### 5.1 Why containerize at all

Running three long-lived Python processes plus Kafka and Postgres locally, manually, across multiple terminals, wasn't sustainable for a week-long unattended run. Rather than manually installing Python/dependencies on a remote server and hoping the environment matched, I containerized the application: one shared image (`bbw-app`), with the specific script to run decided at container-start time via the `command:` field in Docker Compose — rather than building three near-identical Dockerfiles.

### 5.2 Pushing to AWS ECR, and a real security decision

Once the image built and ran correctly locally, I pushed it to a private ECR repository. Authenticating Docker to a *remote* server (Contabo) against a private registry required a real decision: put my long-lived AWS access key on that third-party server, or not.

**I chose not to.** Instead, I generate a short-lived (~12 hour) ECR login token **on my own laptop** (using my properly-scoped local AWS profile) and pipe it directly over SSH into a `docker login` command running on the Contabo server — so the server only ever holds a temporary token, never my actual secret key. This is a small but genuine security-conscious decision that most portfolio projects skip entirely.

### 5.3 Standing up the full stack on Contabo

The exact same `docker-compose.yml` that worked locally was copied to the server, along with a `.env` file. Real issues that came up specifically on this second machine, worth noting because they reveal genuine understanding rather than "it just worked":

- The Kafka data-directory permissions bug recurred, since the volume was freshly created there too.
- `boto3.Session(profile_name=...)` failed inside the containers — `~/.aws/credentials` (a host-machine file) doesn't exist inside a container at all. Fixed by switching to plain AWS environment variables, which is also the more portable, container-friendly pattern going forward (works identically on any machine, not tied to a specific local profile file).
- Python's stdout buffering hid script output when running non-interactively inside containers — fixed with `PYTHONUNBUFFERED=1` in the compose environment, rather than needing `-u` on every command individually.
- The Redshift security-group IP allowlist needed updating for Contabo's IP too — and again later, when my *own* IP changed while testing from Claude Code locally. This repeated enough times that I now understand it's a structural consequence of not having a static residential IP, not a one-off mistake.

### 5.4 The production incident, deployed

The `ConnectTimeout` crash described in §3.7 happened on this deployed, running system — not in local testing. Diagnosing it required the same systematic approach used throughout: check `docker ps -a` for container status, check logs for the actual traceback, reason about root cause before applying a fix, then rebuild → push → pull → redeploy through the full ECR pipeline described above.

---

## 6. The Kulturdaten Investigation — Real Limitations, Honestly Found

To test the original "anomaly correlates with nearby events" hypothesis, I investigated Berlin's Kulturdaten open-data platform (`api-v2.kulturdaten.berlin`). This produced genuinely valuable findings, even though it did not fully succeed:

- The documented `/events`, `/locations`, `/attractions` endpoints are real and live, but **GET-based query parameters (search, pagination via `page`) are silently ignored from some access paths** — real search requires POST endpoints not fully explored.
- **Locations have street addresses, not coordinates** — precise distance-based matching would require a separate geocoding step, which I explicitly scoped as future work rather than faking with borough-level matching.
- **The `/events` endpoint only exposes current and future events, not historical ones** — meaning the historical surges detected in the existing dataset (Sept 5–9) cannot be retroactively verified against this API at all. This was confirmed directly by extracting every `startDate` in a full data pull and observing none were earlier than the current date.
- Testing against a **live, current surge** (Sept 10th), I found a plausible-looking event match by keyword/borough (`"Tiergarten"` appearing in both), which turned out to be a **false positive** on manual verification — the matched location (a library on Lützowstraße) was geographically distant from the actual surge station (near Brandenburg Gate), despite sharing a borough tag. This concretely demonstrated why borough/keyword matching alone is unreliable, and why real geocoding + distance calculation is a genuine requirement, not a nice-to-have.

---

## 7. Key Trade-offs and Decisions (Summary)

| Decision | Choice made | Reasoning |
|---|---|---|
| Kafka vs. direct S3 write | Kafka | Enables multiple independent consumers, replay, decoupling — acknowledged as somewhat over-engineered for current scale, deliberately included to demonstrate the pattern |
| One Kafka message per station vs. per poll | Per station | Matches DB schema, enables per-station processing, meaningful partition key |
| Duplicate-prevention: tracking table vs. pointer | Pointer | Simpler; accepted reduced resilience to out-of-order/partial failures as a conscious trade-off |
| Pointer storage: Redshift vs. Postgres | Postgres | Avoids Redshift query cost; accepted loss of cross-system atomicity |
| Dashboard data source: live API vs. Redshift | Both, split by tile | Live map needs freshness + zero cost; historical tiles need data the live API can't provide |
| Spark vs. Flink for future streaming correlation | Flink | Event-at-a-time model fits real-time anomaly correlation better than Spark's micro-batching; prior hands-on exposure |
| dbt / Snowflake / additional tools | Declined (for now) | No genuine problem they solve that existing tools don't already handle at current scale |

---

## 8. What's Next

A sequenced, four-phase plan for extending real event correlation into the pipeline:

1. **Locations + Events → Redshift** — a proper ingestion pipeline (same proven pattern as NextBike), replacing today's manual `curl`/`grep` investigation with permanent, queryable tables.
2. **Geocoding** — real `lat`/`lng` for Kulturdaten locations (via Nominatim), enabling genuine distance-based matching instead of borough/keyword guessing — directly fixing the false positive found in §6.
3. **Vector database for semantic event matching** — embedding real event/attraction descriptions (Chroma), so anomaly explanation can distinguish "a large festival" from "a small gallery opening" by meaning, not just proximity.
4. **Flink-based live correlation** — a continuous streaming job joining the bike-station stream against a real events stream, by genuine distance and time window, once Phases 1–3 provide real, geocoded, embedded data to correlate against.

Also identified, not yet done: `restart: unless-stopped` on all Docker Compose services (closing the exact gap that caused the Sept 8th outage to go unnoticed for so long).

---

## 9. Honest Limitations

- The Sept 8th data gap is permanent and unrecoverable — NextBike's API has no history endpoint.
- Surge detection currently cannot reliably distinguish operational rebalancing from genuine demand without further refinement (e.g. a station-relative rolling baseline rather than a fixed threshold).
- Event correlation is not yet real — the Kulturdaten investigation found genuine data and API limitations that need dedicated infrastructure (Phases 1–2 above) before it can move past manual, one-off verification.
- Free-floating bike data (individual `bike_list[]` tracking, ghost-bike detection) was deliberately scoped out of the current build and remains a legitimate but separate future direction, not pursued further to keep focus on the core anomaly-correlation narrative.

## 10. Incident: Loader Crash and a Configuration Regression (Sept 18–23)

This section documents a second real production incident, genuinely more informative than the first, because it surfaced not just a code bug but a **configuration regression** — settings that had been correctly added earlier, then silently lost during later edits.

### 10.1 The crash

On **2026-09-18**, the loader crashed with:
```
redshift_connector.error.InterfaceError: BrokenPipe: server socket closed. Please check
that client side networking configurations such as Proxies, firewalls, VPN, etc. are not
affecting your network connection.
```

**Root cause:** the loader opens a single Redshift connection at container startup and reuses it indefinitely across every poll cycle. A long-lived connection eventually had its underlying TCP socket closed — either by Redshift Serverless server-side, or by a silent drop somewhere in the network path. This is a well-understood, expected failure mode for long-lived database connections; it was a known, deferred risk flagged earlier in the project ("a connection left open for a very long time can occasionally drop") that had not yet been addressed.

There was no `try/except` around the `COPY` execution, so this single dropped connection crashed the entire container. Because `restart: unless-stopped` had **not** been applied to the `loader` service (only `dashboard` had it, from an earlier partial fix), the container sat dead — **for five full days** — before being noticed.

### 10.2 The fix

- Wrapped `load_file_to_redshift` in a `try/except`: on any execution failure, reconnect to Redshift (`get_redshift_connection()`) and retry the same `COPY` once before giving up.
- Kept the earlier per-file `try/except` (from the deleted-S3-file incident) so one bad file doesn't halt the batch.
- Added an outer, loop-level `try/except` in `main()` as a final safety net, so a genuinely unrecoverable error (e.g. Postgres itself unreachable) logs and waits for the next cycle rather than crashing the container.

### 10.3 The recovery — validating the pointer design under real failure

Before redeploying, the Postgres `load_pointer` value was checked directly:
```sql
SELECT * FROM load_pointer;
-- 2026-09-18 01:08:28+00
```
This confirmed the pointer was sitting exactly where the crash occurred, untouched by the five days of downtime. **No manual reset was performed.** After redeploying the fixed image, the loader correctly found and began working through **1,552 backlogged files**, starting from the exact file it had failed on originally — not reprocessing anything already loaded, not skipping anything new.

This is the first real-world validation of the pointer-based duplicate-prevention design under an actual multi-day outage, and it held up correctly: the simpler design choice (a single pointer, accepted at the time as less resilient than a per-file tracking table) proved sufficient for this failure mode, since the failure was a clean crash before any pointer update — not a partial, out-of-order, or corrupted state.

### 10.4 A second, unrelated discovery: a configuration regression

While diagnosing why the redeployed loader was producing no visible log output at all, two things were found to have been silently lost from `docker-compose.yaml` at some point during later, unrelated edits (most likely during the Traefik/domain-routing work in a previous session):

- **`PYTHONUNBUFFERED: "1"`** — missing from `producer`, `consumer`, and `loader`. Without it, `print()` output sits in Python's internal buffer and never reaches `docker logs`, making the container appear silent even while doing real, correct work. This is the same buffering issue diagnosed and fixed earlier in the project (§3, initial local debugging) — it had simply regressed.
- **`restart: unless-stopped`** — present only on `dashboard`, never actually added to `producer`, `consumer`, or `loader`, despite being identified as a clear, planned fix after the Sept 8 incident. This gap is what let the Sept 18 crash go unnoticed for five days instead of self-recovering within seconds.

Both were reapplied to all four long-running services. This is a useful, honest lesson in its own right: **a fix that isn't captured in version-controlled configuration can silently disappear during unrelated future edits.** The correct fix wasn't just "add the setting" a second time — it's a prompt to keep `docker-compose.yaml` under source control (git) so changes like this are visible in diffs and reviewable, rather than being discovered only when a service goes silently dark.

### 10.5 What this incident adds to the project's story

Combined with the Sept 8 producer incident, this is now a second, independently-diagnosed production failure — a different root cause (dropped DB connection vs. API timeout), a different discovery path (empty logs, not a crashed container), and a genuine configuration-management lesson on top of the code fix. Together they form a real pattern worth stating plainly: **this pipeline has now survived two real production incidents, each correctly diagnosed from first principles (log inspection, direct database queries, systematic elimination of hypotheses) and each resulting in a durable fix, not a one-off patch.**

---

## 11. Next: Alerting and Monitoring

Both incidents above shared the same root enabling factor: **nothing told me a service was down.** Discovery happened by chance (checking the dashboard, noticing stale data) rather than by design. This is the next concrete piece of infrastructure planned:

**Scope, decided deliberately small to start:**
- A lightweight, cron-scheduled script (not a new monitoring platform) checking two things:
  1. **Container health** — are `producer`, `consumer`, `loader`, `kafka1`, `postgres`, and `dashboard` all in a running state?
  2. **Data freshness** — is `MAX(fetched_at)` in Redshift within an expected threshold of "now" (catching a container that's technically running but silently stuck, which container-status checks alone would miss)?
- Notification via a simple webhook (Telegram or Discord) — no new infrastructure beyond the script itself and a free bot/webhook.

**Deliberately deferred:** a full Prometheus/Grafana/Alertmanager stack was considered and explicitly set aside for now, using the same reasoning applied earlier to Spark, dbt, and Snowflake — it's genuinely more powerful, but disproportionate to the current scale and would compete for focus with the still-open event-correlation phases (§8) rather than strengthening the core project narrative. It remains a reasonable future upgrade if the project's monitoring needs outgrow the simple version.