# ==========================================================
# PostgreSQL Event Store
# ----------------------------------------------------------
# Persists validated ScientificEvents in PostgreSQL.
#
# Worker
#    ↓
# ScientificEvent
#    ↓
# PostgresEventStore
#    ↓
# PostgreSQL
# ==========================================================

import json

import psycopg

from shared.events import ScientificEvent
from storage.base import EventStore


class PostgresEventStore(EventStore):
    """
    PostgreSQL implementation of the EventStore contract.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "scientific_pipeline",
        user: str = "pipeline",
        password: str = "pipeline",
    ):
        self.connection_string = (
            f"host={host} "
            f"port={port} "
            f"dbname={database} "
            f"user={user} "
            f"password={password}"
        )

    # ==========================================================
    # Connect
    # ==========================================================

    def connect(self) -> None:
        """
        Establish a PostgreSQL connection.
        """

        self.connection = psycopg.connect(
            self.connection_string
        )

        print("✅ Connected to PostgreSQL")

    # ==========================================================
    # Create Schema
    # ==========================================================

    def create_schema(self) -> None:
        """
        Create the scientific_events table if it does not exist.
        """

        with self.connection.cursor() as cursor:

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS scientific_events (
                    event_id UUID PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL,
                    source TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    payload JSONB NOT NULL
                )
                """
            )

        self.connection.commit()

        print("✅ PostgreSQL schema ready")

    # ==========================================================
    # Save Event
    # ==========================================================

    def save(self, event: ScientificEvent) -> None:
        """
        Persist one validated ScientificEvent.
        """

        with self.connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO scientific_events (
                    event_id,
                    timestamp,
                    source,
                    schema_version,
                    payload
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    event.event_id,
                    event.timestamp,
                    event.source,
                    event.schema_version,
                    json.dumps(event.payload.model_dump(mode="json")),
                ),
            )

        self.connection.commit()

        print(
            f"✅ Event persisted: "
            f"{event.event_id}"
        )

    # ==========================================================
    # Close
    # ==========================================================

    def close(self) -> None:
        """
        Close the PostgreSQL connection.
        """

        if self.connection and not self.connection.closed:
            self.connection.close()

            print("✅ PostgreSQL connection closed")