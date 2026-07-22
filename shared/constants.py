# ==========================================================
# Shared Enumerations
# ----------------------------------------------------------
# Defines fixed values shared across the project.
# ==========================================================

from enum import Enum


class SignalQuality(str, Enum):
    """
    Quality of the recorded detector signal.
    """

    GOOD = "GOOD"
    FAIR = "FAIR"
    POOR = "POOR"


class DetectorStatus(str, Enum):
    """
    Operational health of the detector.
    """

    OK = "OK"
    WARNING = "WARNING"