from confluent_kafka import Consumer, KafkaError

c = Consumer({'bootstrap.servers': 'localhost:9094', 'group.id': 'notification-group', 'auto.offset.reset': 'earliest'})

c.subscribe(["bike_stations"])

msg = c.poll(timeout=3)

if msg is None:
  print("No Message received")
elif msg.error():
  print(f"Error: {msg.error()}")
else:
  print(f"Recieved: {msg.value()}")

c.close()