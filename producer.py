import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:29092'  
TOPIC_NAME = 'random_numbers'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

def generate_random_number():
    numbers = list(range(1, 11))
    weights = [10, 10, 10, 10, 10, 10, 10, 10, 1, 1]  
    return random.choices(numbers, weights=weights, k=1)[0]

def produce_messages():
    try:
        while True:
            number = generate_random_number()
            timestamp = datetime.now().isoformat()
            message = {
                'number': number,
                'timestamp': timestamp
            }
            producer.send(TOPIC_NAME, value=message)
            print(f"Sent: {message}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nMessage produce stopped.")
    finally:
        producer.flush()
        print("Completed.")

if __name__ == "__main__":
    produce_messages()
