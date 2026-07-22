# ==========================================================
# Synthetic Detector
# ----------------------------------------------------------
# Simulates a detector by generating synthetic readings.
# ==========================================================

import random

from shared.events import ScientificEvent
from shared.payloads import SyntheticPayload
from shared.constants import SignalQuality

class SyntheticDetector:
    """
    Simulates a scientific detector.
    """

    def __init__(self, detector_id: str):
        self.detector_id = detector_id

    def generate_event(self) -> ScientificEvent:
        """
        Generate one synthetic scientific event.
        """

        # ==========================
        # Generate Energy Reading
        # ==========================
        energy = round(random.uniform(20, 200), 2)

        # ==========================
        # Determine Signal Quality
        # ==========================
        if energy < 50:
            signal_quality = SignalQuality.FAIR

        elif energy <= 170:
            signal_quality = SignalQuality.GOOD

        else:
            signal_quality = SignalQuality.POOR

        # ==========================
        # Create Payload
        # ==========================
        payload = SyntheticPayload(
            detector_id=self.detector_id,
            energy=energy,
            signal_quality=signal_quality,
        )

        # ==========================
        # Wrap inside ScientificEvent
        # ==========================
        return ScientificEvent(
            source="synthetic",
            payload=payload
        )