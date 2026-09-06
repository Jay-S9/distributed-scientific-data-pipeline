import json
import os

from worker.worker import Worker
from shared.events import ScientificEvent


class CrashBeforeAckWorker(Worker):
    """
    Test worker that simulates a worker crash after successful
    persistence but before RabbitMQ acknowledgement.
    """

    def handle_message(self, channel, method, properties, body):
        print("📩 Message received")

        data = json.loads(body)
        print("✅ JSON decoded")

        event = ScientificEvent.model_validate(data)
        print("✅ Event validated")

        print("💾 Persisting event...")
        self.event_store.save(event)
        print("✅ Persistence completed")

        print("💥 Simulating worker crash BEFORE ACK")

        # Terminate the process immediately.
        # RabbitMQ therefore receives no ACK and will
        # redeliver the unacknowledged message.
        os._exit(1)


if __name__ == "__main__":

    worker = CrashBeforeAckWorker()

    try:
        worker.connect()
        worker.connect_storage()
        worker.declare_queue()
        worker.consume()

    finally:
        worker.close()