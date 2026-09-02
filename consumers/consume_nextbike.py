import json
from confluent_kafka import Consumer, KafkaError
import boto3

c = Consumer({
  'bootstrap.servers': 'localhost:9094',
  'group.id': 'bike_stations_consumer',
  'auto.offset.reset': 'earliest'
})

c.subscribe(["bike_stations"])

FIELD_WIDTHS = {
  "name": 45,
  "booked_bikes": 8,
  "bikes": 6,
  "bikes_available_to_rent": 10,
  "active_place": 8,
  "bike_racks": 8,
  "free_racks": 8,
  "special_racks": 8,
  "maintenance": 8,
  "rack_locks": 8,
}

list_of_places = []

fetched_at_marker = ""


header = (
  f"{'Station Name':<{FIELD_WIDTHS['name']}} | "
  f"{'Booked':<{FIELD_WIDTHS['booked_bikes']}} | "
  f"{'Bikes':<{FIELD_WIDTHS['bikes']}} | "
  f"{'Available':<{FIELD_WIDTHS['bikes_available_to_rent']}} | "
  f"{'Active':<{FIELD_WIDTHS['active_place']}} | "
  f"{'Racks':<{FIELD_WIDTHS['bike_racks']}} | "
  f"{'Free':<{FIELD_WIDTHS['free_racks']}} | "
  f"{'Special':<{FIELD_WIDTHS['special_racks']}} | "
  f"{'Maint.':<{FIELD_WIDTHS['maintenance']}} | "
  f"{'Locks':<{FIELD_WIDTHS['rack_locks']}}"
)
print(header)
print("-" * len(header))


def send_batch_to_s3(places, fetched_at):

  pass

try:
  while True:
    msg = c.poll(timeout=3)

    if msg is None:
      pass  # don't print this every 3s, it just adds noise while waiting
    elif msg.error():
      print(f"Error: {msg.error()}")
    else:
      try:
        place_data = json.loads(msg.value())

        if not place_data.get("spot"):
          continue

        row = (
          f"{str(place_data.get('name'))[:FIELD_WIDTHS['name']]:<{FIELD_WIDTHS['name']}} | "
          f"{str(place_data.get('booked_bikes')):<{FIELD_WIDTHS['booked_bikes']}} | "
          f"{str(place_data.get('bikes')):<{FIELD_WIDTHS['bikes']}} | "
          f"{str(place_data.get('bikes_available_to_rent')):<{FIELD_WIDTHS['bikes_available_to_rent']}} | "
          f"{str(place_data.get('active_place')):<{FIELD_WIDTHS['active_place']}} | "
          f"{str(place_data.get('bike_racks')):<{FIELD_WIDTHS['bike_racks']}} | "
          f"{str(place_data.get('free_racks')):<{FIELD_WIDTHS['free_racks']}} | "
          f"{str(place_data.get('special_racks')):<{FIELD_WIDTHS['special_racks']}} | "
          f"{str(place_data.get('maintenance')):<{FIELD_WIDTHS['maintenance']}} | "
          f"{str(place_data.get('rack_locks')):<{FIELD_WIDTHS['rack_locks']}}"
        )
        # print(row)

        fetched_at = place_data.get('fetched_at')
        

        if fetched_at == fetched_at_marker:
          list_of_places.append(place_data)
        else:
          if list_of_places:
            print(f"--- Batch complete: {len(list_of_places)} places, fetched_at={fetched_at_marker} -> send to S3 ---")
            send_batch_to_s3(list_of_places, fetched_at_marker)

          list_of_places = []
          fetched_at_marker = fetched_at


      except json.JSONDecodeError:
        print(f"Skipping non-JSON message: {msg.value()}")
finally:
  if list_of_places:
    print(f"--- Final flush: {len(list_of_places)} places, fetched_at={fetched_at_marker} -> send to S3 ---")
  c.close()
  print("Consumer closed cleanly.")