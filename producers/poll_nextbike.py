import json
from datetime import datetime, timezone, date
from fetch_nextbike import fetch_raw_nextbike_data, extract_cities_data, extract_places_data
import time
from pathlib import Path

NEXTBIKE_URL = "https://api.nextbike.net/maps/nextbike-live.json"
BERLIN_CITY_ID = 362
POLL_INTERVAL_SECONDS = 5

"""
doubts
- why is while true and time working
- the time.sleep should I call this after it has made the file or before it will make the file ? what will be the drawback

"""

def main():

  while True:
    fetched_at = datetime.now(timezone.utc).isoformat()
    print(fetched_at)

    raw_nextbike_data = fetch_raw_nextbike_data(NEXTBIKE_URL, BERLIN_CITY_ID)

    city_data = extract_cities_data(raw_nextbike_data["countries"][0]["cities"][0], fetched_at)

    places_data = extract_places_data(raw_nextbike_data["countries"][0]["cities"][0]["places"], fetched_at)

    now = datetime.now(timezone.utc)

    today = now.date().isoformat()
    hour = now.strftime("%H")
    filename = now.strftime("%Y%m%dT%H%M%SZ") + ".json"

    folder = Path(f"data/raw/bike_stations/dt={today}/hour={hour}")
    folder.mkdir(parents=True, exist_ok=True)

    filepath = folder / filename

    # here I feel this of making a folder or checking if a folder is made every 5 seconds is waste, can this be done in some other way ?
    Path(f"data/raw/bike_stations/dt={today}/hour={hour}").mkdir(parents=True, exist_ok=True)

    record = {
      "city": city_data,
      "places": places_data
    }

    with open(filepath, "w") as f:
      json.dump(record, f, indent=2)

    time.sleep(POLL_INTERVAL_SECONDS)



if __name__ == "__main__":
  main()