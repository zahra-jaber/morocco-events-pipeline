import json
import time
import os
from kafka import KafkaProducer
from datetime import datetime

# Connexion à Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def find_json_files():
    """Trouve les fichiers JSON dans le répertoire courant"""
    current_dir = os.getcwd()
    print(f"  Répertoire courant: {current_dir}\n")
    
    json_files = [f for f in os.listdir(current_dir) if f.endswith('_events.json')]
    
    if not json_files:
        print(" Aucun fichier *_events.json trouvé!")
        print("Files dans le répertoire:")
        for f in os.listdir(current_dir):
            print(f"  - {f}")
        return []
    
    print(f" Fichiers trouvés: {json_files}\n")
    return json_files

def load_and_send_events():
    """Charge les fichiers JSON et envoie à Kafka"""
    
    files = find_json_files()
    
    if not files:
        return
    
    total_sent = 0
    
    for file_name in files:
        try:
            with open(file_name, 'r', encoding='utf-8') as f:
                events = json.load(f)
            
            print(f" Envoi de {len(events)} events depuis {file_name}...")
            
            for idx, event in enumerate(events):
                try:
                    # Normaliser le format
                    normalized_event = normalize_event(event, file_name)
                    
                    # Envoyer à Kafka
                    future = producer.send('raw_events', value=normalized_event)
                    future.get(timeout=5)  # Attendre confirmation
                    
                    print(f"   -- #{idx+1}: {normalized_event['title'][:50]}...")
                    total_sent += 1
                    time.sleep(0.3)  # Délai
                    
                except Exception as e:
                    print(f"   Erreur envoi #{idx+1}: {e}")
                    
        except FileNotFoundError:
            print(f" Fichier non trouvé: {file_name}")
        except json.JSONDecodeError as e:
            print(f" Erreur JSON dans {file_name}: {e}")

    print(f"\n --Total: {total_sent} events envoyés!")
    producer.flush()
    producer.close()

def normalize_event(event, source):
    """Normalise le format guichet/ticket"""
    
    if 'guichet' in source:
        # Format Guichet
        return {
            'event_id': str(hash(event.get('title', '')) % 100000),
            'title': event.get('title', ''),
            'price': float(event.get('price', 0)),
            'city': event.get('city', '').upper(),
            'category': event.get('category', ''),
            'event_date': event.get('startAt', '').split(' - ')[0] if ' - ' in event.get('startAt', '') else '',
            'start_time': event.get('startAt', '').split(' - ')[1] if ' - ' in event.get('startAt', '') else '00:00',
            'source': 'guichet',
            'ingestion_timestamp': datetime.now().isoformat()
        }
    else:
        # Format Ticket
        place = event.get('place', {})
        city = place.get('city', '') if isinstance(place, dict) else ''
        
        return {
            'event_id': str(hash(event.get('title', '')) % 100000),
            'title': event.get('title', ''),
            'price': float(event.get('price', 0)),
            'city': city.upper(),
            'category': event.get('category', ''),
            'event_date': event.get('event_date', ''),
            'start_time': event.get('open_at', '00:00'),
            'source': 'ticket',
            'ingestion_timestamp': datetime.now().isoformat()
        }

if __name__ == '__main__':
    print(" Démarrage du producer...\n")
    load_and_send_events()