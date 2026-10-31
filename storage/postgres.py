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
import os

import psycopg
from dotenv import load_dotenv

load_dotenv()

from shared.events import ScientificEvent
from storage.base import EventStore


class PostgresEventStore(EventStore):
    """
    PostgreSQL implementation of the EventStore contract.
    """

    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = os.getenv("POSTGRES_DB", "scientific_pipeline")
        self.user = os.getenv("POSTGRES_USER", "pipeline")
        self.password = os.getenv("POSTGRES_PASSWORD")

        if not self.password:
            raise ValueError(
                "POSTGRES_PASSWORD environment variable is not set."
            )

        self.connection_string = (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.database} "
            f"user={self.user} "
            f"password={self.password}"
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

    def save(self, event: ScientificEvent) -> bool:
        """
        Persist one validated ScientificEvent.

        Returns True when a new event is persisted.
        Returns False when the event was already persisted.
        """

        try:

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

            return True

        except psycopg.errors.UniqueViolation:

            self.connection.rollback()

            return False

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