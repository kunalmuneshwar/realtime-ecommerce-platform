
import getpass
import json
import time
from pathlib import Path

import boto3
import psycopg2
from confluent_kafka import Consumer, KafkaException


# --------------------------------------------------
# 1. Kafka Configuration
# --------------------------------------------------

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "ecommerce-file-writer-v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

TOPIC = "ecommerce_events"


# --------------------------------------------------
# 2. PostgreSQL Configuration
# --------------------------------------------------

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "ecommerce_analytics",
    "user": "postgres",
}


# --------------------------------------------------
# 3. AWS S3 Configuration
# --------------------------------------------------

BUCKET_NAME = "realtime-ecommerce-kunal-2026"
AWS_REGION = "ap-south-1"


# --------------------------------------------------
# 4. Batch Configuration
# --------------------------------------------------

BATCH_SIZE = 10
BATCH_TIMEOUT = 30

BATCH_DIR = Path("data/raw/batches")
BATCH_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# 5. Insert Events into PostgreSQL
# --------------------------------------------------

def insert_into_postgres(messages, connection):
    """Insert a batch of events into PostgreSQL."""

    rows = []

    for message in messages:

        event = json.loads(message.value().decode("utf-8"))

        rows.append((
            event["event_id"],
            event["event_type"],
            event["customer_id"],
            event.get("order_id"),
            event.get("product_id"),
            event.get("product_name"),
            event.get("price"),
            event["timestamp"]
        ))

    query = """
        INSERT INTO public.events (
            event_id,
            event_type,
            customer_id,
            order_id,
            product_id,
            product_name,
            product_price,
            event_timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
    """

    try:

        with connection.cursor() as cursor:
            cursor.executemany(query, rows)

        connection.commit()

        print(
            f"PostgreSQL: processed {len(rows)} events."
        )

    except Exception:

        connection.rollback()
        raise


# --------------------------------------------------
# 6. Upload Raw Events to S3
# --------------------------------------------------

def upload_to_s3(messages, s3):
    """Save the batch locally and upload it to S3."""

    first_message = messages[0]
    last_message = messages[-1]

    partition = first_message.partition()
    first_offset = first_message.offset()
    last_offset = last_message.offset()

    filename = (
        f"batch_p{partition}_offsets_"
        f"{first_offset}_{last_offset}.jsonl"
    )

    local_path = BATCH_DIR / filename

    # Save raw JSON events locally

    with local_path.open("w", encoding="utf-8") as file:

        for message in messages:
            file.write(
                message.value().decode("utf-8") + "\n"
            )

    # Upload batch to S3

    s3_key = (
        f"raw/events/partition-{partition}/{filename}"
    )

    s3.upload_file(
        str(local_path),
        BUCKET_NAME,
        s3_key
    )

    print(
        f"S3: uploaded {len(messages)} events."
    )

    print(
        f"s3://{BUCKET_NAME}/{s3_key}"
    )


# --------------------------------------------------
# 7. Process Kafka Batch
# --------------------------------------------------

def process_batch(messages, consumer, connection, s3):
    """
    Write events to PostgreSQL and S3.
    Commit Kafka offsets only after both operations succeed.
    """

    if not messages:
        return

    print(
        f"\nProcessing batch of {len(messages)} events..."
    )

    # Step 1: PostgreSQL

    insert_into_postgres(
        messages,
        connection
    )

    # Step 2: S3

    upload_to_s3(
        messages,
        s3
    )

    # Step 3: Commit Kafka offsets

    consumer.commit(
        message=messages[-1],
        asynchronous=False
    )

    print(
        "Kafka offsets committed successfully.\n"
    )


# --------------------------------------------------
# 8. Main Consumer Function
# --------------------------------------------------

def main():

    password = getpass.getpass(
        "Enter PostgreSQL password: "
    )

    connection = psycopg2.connect(
        **DB_CONFIG,
        password=password
    )

    print("Connected to PostgreSQL.")

    consumer = Consumer(KAFKA_CONFIG)

    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION
    )

    consumer.subscribe([TOPIC])

    batch = []

    last_flush_time = time.monotonic()

    print(
        "Kafka to PostgreSQL + S3 consumer started."
    )

    print("Waiting for events...\n")

    try:

        while True:

            message = consumer.poll(1.0)

            if message is not None:

                if message.error():
                    raise KafkaException(message.error())

                batch.append(message)

                print(
                    f"Received offset {message.offset()} | "
                    f"Batch: {len(batch)}/{BATCH_SIZE}"
                )

            elapsed = (
                time.monotonic() - last_flush_time
            )

            if batch and (
                len(batch) >= BATCH_SIZE
                or elapsed >= BATCH_TIMEOUT
            ):

                process_batch(
                    batch,
                    consumer,
                    connection,
                    s3
                )

                batch.clear()

                last_flush_time = time.monotonic()

    except KeyboardInterrupt:

        print("\nStopping consumer...")

        if batch:

            print("Processing remaining events...")

            process_batch(
                batch,
                consumer,
                connection,
                s3
            )

    finally:

        consumer.close()
        connection.close()

        print(
            "Kafka consumer and PostgreSQL connection closed."
        )


# --------------------------------------------------
# 9. Run Consumer
# --------------------------------------------------

if __name__ == "__main__":
    main()
