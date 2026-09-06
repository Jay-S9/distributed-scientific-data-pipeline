from worker.worker import Worker


class FaultInjectedWorker(Worker):
    """
    Test worker that deliberately fails during persistence.
    Used only to verify retry-before-DLQ behavior.
    """

    def __init__(self):
        super().__init__()
        self.failure_count = 0

    def connect_storage(self):
        """
        Connect normally so the worker lifecycle remains realistic.
        """
        self.event_store.connect()
        self.event_store.create_schema()

    def handle_message(self, channel, method, properties, body):
        """
        Inject a persistence failure before the real save() call.
        """

        print("💥 FAULT INJECTION: forcing persistence failure")

        original_save = self.event_store.save

        def failing_save(event):
            self.failure_count += 1

            print(
                f"💥 Injected persistence failure "
                f"#{self.failure_count}"
            )

            raise RuntimeError(
                "Injected database failure for fault-injection test"
            )

        self.event_store.save = failing_save

        try:
            super().handle_message(
                channel,
                method,
                properties,
                body
            )

        finally:
            self.event_store.save = original_save


if __name__ == "__main__":

    worker = FaultInjectedWorker()

    try:
        worker.connect()
        worker.connect_storage()
        worker.declare_queue()
        worker.consume()

    except KeyboardInterrupt:
        print("🛑 Fault-injection worker stopped")

    finally:
        worker.close()