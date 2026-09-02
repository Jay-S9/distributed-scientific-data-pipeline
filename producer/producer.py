# ============================
# Imports
# ============================

import pika

from shared.events import ScientificEvent


class Producer:
    """
    Responsible for preparing ScientificEvents
    and publishing them to RabbitMQ.
    """

    def __init__(self):
        self.connection = None
        self.channel = None

    # ============================
    # Connect to RabbitMQ
    # ============================

    def connect(self) -> None:
        """
        Establish one RabbitMQ connection
        and open one channel for the producer's lifetime.
        """

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )

        print("✅ Connected to RabbitMQ")

        self.channel = self.connection.channel()

        print("✅ Channel opened")

        # ============================
        # Declare Queue
        # ============================

        self.channel.queue_declare(
            queue="scientific_events",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "scientific_events.dlx",
                "x-dead-letter-routing-key": "scientific_events.dlq"
            }
        )

        print("✅ Queue ready")

    # ============================
    # Publish Event
    # ============================

    def publish(self, event: ScientificEvent) -> None:
        """
        Publish one ScientificEvent
        using the existing connection and channel.
        """

        self.channel.basic_publish(
            exchange="",
            routing_key="scientific_events",
            body=event.model_dump_json(),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

        print("✅ Event published")

    # ============================
    # Close Connection
    # ============================

    def close(self) -> None:
        """
        Close the RabbitMQ connection
        when the producer is finished.
        """

        if self.connection and self.connection.is_open:
            self.connection.close()

            print("✅ Connection closed")