import json
from datetime import datetime, timezone, date
from fetch_nextbike import fetch_raw_nextbike_data, extract_cities_data, extract_places_data
import time
from pathlib import Path

import os
from dotenv import load_dotenv

from confluent_kafka import Producer

load_dotenv()

NEXTBIKE_URL = "https://api.nextbike.net/maps/nextbike-live.json"
BERLIN_CITY_ID = 362
POLL_INTERVAL_SECONDS = 300


def iterate_through_places_data_and_send_to_kafka(producer,places_data):

  for place_data in places_data:
    producer.produce(topic='bike_stations', key=str(place_data["uid"]), value=json.dumps(place_data).encode('utf-8'))

  producer.flush()



def main():

  p = Producer({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS']})


  while True:
    fetched_at = datetime.now(timezone.utc).isoformat()
    print(fetched_at)

    raw_nextbike_data = fetch_raw_nextbike_data(NEXTBIKE_URL, BERLIN_CITY_ID)

    city_data = extract_cities_data(raw_nextbike_data["countries"][0]["cities"][0], fetched_at)

    places_data = extract_places_data(raw_nextbike_data["countries"][0]["cities"][0]["places"], fetched_at)

    iterate_through_places_data_and_send_to_kafka(p, places_data)

    time.sleep(POLL_INTERVAL_SECONDS)



if __name__ == "__main__":
  main()