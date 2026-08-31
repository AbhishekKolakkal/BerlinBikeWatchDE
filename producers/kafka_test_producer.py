from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9094'})

p.produce(topic='bike_stations', value="Test Message from Producer Code")

p.flush()




