
import json
import time
from confluent_kafka import Producer
from event_generator import generate_event

# Connect Python to our local Kafka broker
producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

TOPIC = "ecommerce_events"


def delivery_report(error, message):
    if error is not None:
        print(f"Delivery failed: {error}")
    else:
        print(
            f"Delivered to {message.topic()} | "
            f"Partition: {message.partition()} | "
            f"Offset: {message.offset()}"
        )


print("Starting e-commerce event producer...")

try:
    while True:
        event = generate_event()

        producer.produce(
            topic=TOPIC,
            key=event["customer_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report
        )

        # Process delivery callbacks
        producer.poll(0)

        print(f"Sent event: {event['event_type']}")
        time.sleep(2)

except KeyboardInterrupt:
    print("\nStopping producer...")

finally:
    remaining = producer.flush(10)
    print(f"Undelivered messages: {remaining}")
    print("Producer stopped.")
