# ==========================================================
# MuRun2010B Producer
# ----------------------------------------------------------
# Reads events from the MuRun2010B dataset and publishes
# them to RabbitMQ using the shared ScientificEvent envelope.
#
# Dataset:
#     data/raw/MuRun2010B.csv
#
# Flow:
#     CSV row
#        ↓
#     MuRun2010BPayload
#        ↓
#     ScientificEvent
#        ↓
#     RabbitMQ
# ==========================================================

import pandas as pd

from producer.producer import Producer
from shared.events import ScientificEvent
from shared.payloads import (
    MuRun2010BPayload,
    ParticleMeasurement,
)


class MuRun2010BProducer:
    """
    Reads the MuRun2010B dataset and publishes
    its events to RabbitMQ.
    """

    def __init__(self, dataset_path: str):
        self.dataset_path = dataset_path
        self.producer = Producer()

    # ==========================================================
    # Build Particle Measurement
    # ==========================================================

    def build_particle(
        self,
        row,
        prefix: str,
    ) -> ParticleMeasurement:
        """
        Convert one particle's columns from a dataset row
        into a ParticleMeasurement model.
        """

        return ParticleMeasurement(
            particle_type=row[f"Type{prefix}"],
            energy=float(row[f"E{prefix}"]),
            px=float(row[f"px{prefix}" if prefix == "2" else "px1 "]),
            py=float(row[f"py{prefix}"]),
            pz=float(row[f"pz{prefix}"]),
            pt=float(row[f"pt{prefix}"]),
            eta=float(row[f"eta{prefix}"]),
            phi=float(row[f"phi{prefix}"]),
            charge=int(row[f"Q{prefix}"]),
        )

    # ==========================================================
    # Build Scientific Event
    # ==========================================================

    def build_event(self, row) -> ScientificEvent:
        """
        Convert one MuRun2010B dataset row into
        a validated ScientificEvent.
        """

        payload = MuRun2010BPayload(
            run=int(row["Run"]),
            event=int(row["Event"]),
            particle_1=self.build_particle(row, "1"),
            particle_2=self.build_particle(row, "2"),
            invariant_mass=float(row["M"]),
        )

        return ScientificEvent(
            source="murun2010b",
            payload=payload,
        )

    # ==========================================================
    # Publish Dataset
    # ==========================================================

    def publish(self) -> None:
        """
        Read the dataset and publish each row as
        one ScientificEvent.
        """

        print("📂 Loading MuRun2010B dataset...")

        dataframe = pd.read_csv(self.dataset_path)

        print(f"✅ Dataset loaded: {len(dataframe)} rows")

        self.producer.connect()

        try:
            for index, row in dataframe.iterrows():

                event = self.build_event(row)

                self.producer.publish(event)

                print(
                    f"📤 Published event "
                    f"{index + 1}/{len(dataframe)} "
                    f"(Run={event.payload.run}, "
                    f"Event={event.payload.event})"
                )

        finally:
            self.producer.close()


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":

    DATASET_PATH = r"data\raw\MuRun2010B.csv"

    producer = MuRun2010BProducer(DATASET_PATH)
    producer.publish()