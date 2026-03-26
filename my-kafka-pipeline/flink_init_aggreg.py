import json
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta
from collections import defaultdict

consumer = KafkaConsumer(
    'raw_events',
    bootstrap_servers=['localhost:29092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='aggregation-consumer'
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Buffer en mémoire (1 heure)
aggregated_data = defaultdict(lambda: {
    'total_events': 0,
    'prices': [],
    'categories': defaultdict(int),
    'events': []
})

print(" Agrégation en cours...\n")

for message in consumer:
    event = message.value
    city = event['city']
    
    # Ajouter à l'agrégation
    aggregated_data[city]['total_events'] += 1
    aggregated_data[city]['prices'].append(event['price'])
    aggregated_data[city]['categories'][event['category']] += 1
    aggregated_data[city]['events'].append(event['title'])
    
    # Afficher chaque 5 events
    if aggregated_data[city]['total_events'] % 5 == 0:
        avg_price = sum(aggregated_data[city]['prices']) / len(aggregated_data[city]['prices'])
        
        aggregation = {
            'city': city,
            'timestamp': datetime.now().isoformat(),
            'total_events': aggregated_data[city]['total_events'],
            'avg_price': round(avg_price, 2),
            'max_price': max(aggregated_data[city]['prices']),
            'min_price': min(aggregated_data[city]['prices']),
            'categories': dict(aggregated_data[city]['categories']),
        }
        
        print(f"--- Agrégation {city}:")
        print(f"   Total: {aggregation['total_events']} events")
        print(f"   Prix moyen: {aggregation['avg_price']} MAD")
        print(f"   Prix max: {aggregation['max_price']} MAD")
        print(f"   Catégories: {dict(aggregation['categories'])}\n")
        
        # Envoyer à Kafka
        producer.send('events_aggregated', value=aggregation)

consumer.close()
producer.close()