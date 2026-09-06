# ==========================================================
# Scientific Event Worker
# ----------------------------------------------------------
# Consumes ScientificEvents from RabbitMQ, validates them,
# persists them to PostgreSQL, and acknowledges the message
# only after successful persistence.
#
# Flow:
#
#     RabbitMQ
#        ↓
#     JSON decode
#        ↓
#     ScientificEvent validation
#        ↓
#     PostgreSQL persistence
#        ↓
#     ACK
#
# Failed validation or persistence:
#
#        ↓
#     NACK
#        ↓
#     Dead Letter Queue
# ==========================================================

import json

import pika

from shared.events import ScientificEvent
from storage.postgres import PostgresEventStore


MAX_RETRIES = 3

class Worker:
    """
    Consumes Scientific Events from RabbitMQ
    and persists validated events to PostgreSQL.
    """

    def __init__(self):
        self.connection = None
        self.channel = None

        self.event_store = PostgresEventStore()

    # ==========================================================
    # Connect to RabbitMQ
    # ==========================================================

    def connect(self):
        """
        Establish a connection and open a channel.
        """

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )

        self.channel = self.connection.channel()

        print("✅ Worker connected to RabbitMQ")
        print("✅ Worker channel opened")

    # ==========================================================
    # Connect to PostgreSQL
    # ==========================================================

    def connect_storage(self):
        """
        Establish the PostgreSQL connection and ensure
        the required schema exists.
        """

        self.event_store.connect()
        self.event_store.create_schema()

    # ==========================================================
    # Declare Queue
    # ==========================================================

    def declare_queue(self):
        """
        Make sure the scientific events queue exists.
        """

        self.channel.queue_declare(
            queue="scientific_events",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "scientific_events.dlx",
                "x-dead-letter-routing-key": "scientific_events.dlq"
            }
        )

        print("✅ Queue ready")

        # ======================================================
        # Configure Prefetch
        # ======================================================

        self.channel.basic_qos(
            prefetch_count=1
        )

        print("✅ Prefetch configured: 1")

    # ==========================================================
    # Consume Messages
    # ==========================================================

    def consume(self):
        """
        Start consuming messages from the scientific events queue.
        """

        self.channel.basic_consume(
            queue="scientific_events",
            on_message_callback=self.handle_message,
            auto_ack=False
        )

        print("👂 Worker is waiting for messages...")

        self.channel.start_consuming()

    # ==========================================================
    # Retry Message
    # ==========================================================

    def retry_message(self, properties, body) -> bool:
        """
        Republish a failed message with an incremented retry count.

        Returns True if the message was republished for another
        attempt, otherwise False when the retry limit is reached.
        """

        retry_count = 0

        if properties.headers:
            retry_count = properties.headers.get(
                "x-retry-count",
                0
            )

        if retry_count >= MAX_RETRIES:
            return False

        retry_count += 1

        headers = dict(properties.headers or {})
        headers["x-retry-count"] = retry_count

        self.channel.basic_publish(
            exchange="",
            routing_key="scientific_events",
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                headers=headers
            )
        )

        print(
            f"↻ Message scheduled for retry "
            f"{retry_count}/{MAX_RETRIES}"
        )

        return True

    # ==========================================================
    # Handle Message
    # ==========================================================

    def handle_message(self, channel, method, properties, body):
        """
        Decode, validate, persist, and acknowledge one message.

        Permanent failures are sent to the DLQ.
        Storage failures are retried up to MAX_RETRIES.
        """

        print("📩 Message received")

        # ======================================================
        # Decode JSON
        # ======================================================

        try:

            data = json.loads(body)

            print("✅ JSON decoded")

        except Exception as error:

            print(
                f"❌ JSON decoding failed: {error}"
            )

            channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False
            )

            print(
                "↪️ Invalid message sent to DLQ"
            )

            return

        # ======================================================
        # Validate Scientific Event
        # ======================================================

        try:

            event = ScientificEvent.model_validate(data)

            print("✅ Event validated")
            print(event)

        except Exception as error:

            print(
                f"❌ Event validation failed: {error}"
            )

            channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False
            )

            print(
                "↪️ Invalid event sent to DLQ"
            )

            return

        # ======================================================
        # Persist Event
        # ======================================================

        try:

            print("💾 Persisting event...")

            persisted = self.event_store.save(event)

            if persisted:
                print(
                    f"✅ Event persisted: "
                    f"{event.event_id}"
                )

            else:
                print(
                    f"ℹ️ Event already persisted — "
                    f"safe to acknowledge: "
                    f"{event.event_id}"
                )

            channel.basic_ack(
                delivery_tag=method.delivery_tag
            )

            if persisted:
                print("✅ Message acknowledged")
            else:
                print("✅ Duplicate safely acknowledged")

        except Exception as error:

            print(
                f"❌ Persistence failed: {error}"
            )

            # ==================================================
            # Retry / DLQ
            # ==================================================

            if self.retry_message(properties, body):

                channel.basic_ack(
                    delivery_tag=method.delivery_tag
                )

                print(
                    "✅ Current message acknowledged "
                    "after retry was published"
                )

            else:

                channel.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=False
                )

                print(
                    "↪️ Retry limit reached; "
                    "message sent to DLQ"
                )

    # ==========================================================
    # Close
    # ==========================================================

    def close(self):
        """
        Close PostgreSQL and RabbitMQ connections.
        """

        self.event_store.close()

        if self.connection and self.connection.is_open:
            self.connection.close()

            print("✅ RabbitMQ connection closed")

# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":

    worker = Worker()

    try:
        worker.connect()
        worker.connect_storage()
        worker.declare_queue()
        worker.consume()

    except KeyboardInterrupt:
        print("🛑 Worker stopped")

    finally:
        worker.close()
