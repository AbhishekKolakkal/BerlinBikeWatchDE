import json
import requests
from datetime import datetime, timezone

NEXTBIKE_URL = "https://api.nextbike.net/maps/nextbike-live.json"
BERLIN_CITY_ID = 362


def fetch_raw_nextbike_data(city_id: int = BERLIN_CITY_ID) -> dict:
  response = requests.get(NEXTBIKE_URL, params={"city": city_id}, timeout=10)
  response.raise_for_status()
  return response.json()


def extract_cities_data(city_data: dict, fetched_at: str) -> dict:
  CITY_FIELDS = ["refresh_rate", "num_places", "available_bikes", "break", "booked_bikes", "set_point_bikes"]

  extracted_city = {field: city_data.get(field) for field in CITY_FIELDS}
  extracted_city["fetched_at"] = fetched_at

  return extracted_city


def extract_places_data(places_data: list, fetched_at: str) -> list:
  PLACE_FIELDS = ["uid", "lat", "lng", "name", "booked_bikes", "bikes", "bikes_available_to_rent",
                  "active_place", "bike_racks", "free_racks", "special_racks", "free_special_racks",
                  "maintenance", "terminal_type", "place_type", "rack_locks"]

  extracted_places = []
  for place in places_data:
    extracted_place = {field: place.get(field) for field in PLACE_FIELDS}
    extracted_place["fetched_at"] = fetched_at
    extracted_places.append(extracted_place)   # runs once per place, not once per field

  return extracted_places


def main():
  fetched_at = datetime.now(timezone.utc).isoformat()  # one timestamp shared by this whole poll

  raw_nextbike_data = fetch_raw_nextbike_data()

  city_data = extract_cities_data(raw_nextbike_data["countries"][0]["cities"][0], fetched_at)
  places_data = extract_places_data(raw_nextbike_data["countries"][0]["cities"][0]["places"], fetched_at)

  print(f"fetched_at: {fetched_at}")
  print(f"city_data: {city_data}")
  print(f"parsed places: {len(places_data)} (city reports num_places: {city_data['num_places']})")
  print("sample place:", places_data[0])


if __name__ == "__main__":
  main()