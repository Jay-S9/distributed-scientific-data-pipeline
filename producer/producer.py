# ============================
# Imports
# ============================

import pika

from shared.events import ScientificEvent


class Producer:
    """
    Responsible for preparing ScientificEvents
    for transmission.
    """

    def publish(self, event: ScientificEvent) -> None:

        # ============================
        # Connect
        # ============================

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )

        print("✅ Connected to RabbitMQ")

        # ============================
        # Open Channel
        # ============================

        channel = connection.channel()

        print("✅ Channel opened")

        # ============================
        # Declare Queue
        # ============================

        channel.queue_declare(
            queue="scientific_events",
            durable=True
        )

        print("✅ Queue ready")

        # ============================
        # Publish Event
        # ============================

        channel.basic_publish(
            exchange="",
            routing_key="scientific_events",
            body=event.model_dump_json()
        )

        print("✅ Event published")

        # ============================
        # Close Connection
        # ============================

        connection.close()

        print("✅ Connection closed")