# ============================
# Imports
# ============================

import json
import pika

from shared.events import ScientificEvent

# ============================
# Worker
# ============================

class Worker:
    """
    Consumes Scientific Events from RabbitMQ.
    """

    def __init__(self):
        self.connection = None
        self.channel = None

    # ============================
    # Connect to RabbitMQ
    # ============================

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

    # ============================
    # Declare Queue
    # ============================

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

        # ============================
        # Configure Prefetch
        # ============================

        self.channel.basic_qos(
            prefetch_count=1
        )

        print("✅ Prefetch configured: 1")

    # ============================
    # Consume Messages
    # ============================

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

    # ============================
    # Handle Message
    # ============================

    def handle_message(self, channel, method, properties, body):
        """
        Handle a message received from RabbitMQ.
        """

        print("📩 Message received")

        try:

            # ============================
            # Decode JSON
            # ============================

            data = json.loads(body)

            print("✅ JSON decoded")

            # ============================
            # Validate Event
            # ============================

            event = ScientificEvent.model_validate(data)

            print("✅ Event validated")
            print(event)

            print("⏳ Processing event...")

            # ============================
            # Acknowledge Message
            # ============================

            channel.basic_ack(
                delivery_tag=method.delivery_tag
            )

            print("✅ Message acknowledged")

        except Exception as error:

            # ============================
            # Processing Failure
            # ============================

            print(f"❌ Message processing failed: {error}")

            # ============================
            # Reject Message
            # ============================

            channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False
            )

            print("↪️ Message rejected and sent to DLQ")