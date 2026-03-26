import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'raw_events',
    bootstrap_servers=['localhost:29092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='test-consumer'
)

print(" En écoute sur 'raw_events'...\n")

count = 0
for message in consumer:
    event = message.value
    print(f" #{count+1}: {event['title']} - {event['city']} - {event['price']} MAD")
    count += 1
    
    if count >= 10:  # Afficher les 10 premiers
        break

consumer.close()