# ==========================================================
# Shared Event Models
# ----------------------------------------------------------
# This module defines the common event contract used by every
# component in the pipeline.
#
# Producer  ---> RabbitMQ ---> Worker ---> Database ---> API
#
# Every event flowing through the system follows this model.
# ==========================================================

from datetime import datetime, UTC
from shared.payloads import SyntheticPayload
from uuid import uuid4

from pydantic import BaseModel, Field


class ScientificEvent(BaseModel):
    """
    Common event envelope shared across the entire pipeline.

    The payload contains source-specific data
    (Synthetic, CMS, ATLAS, etc.).
    """

    event_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Globally unique event identifier."
    )

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Time when the event entered the pipeline."
    )

    source: str = Field(
        ...,
        description="Origin of the event."
    )

    schema_version: str = Field(
        default="1.0",
        description="Version of the event schema."
    )

    payload: SyntheticPayload = Field(
        ...,
        description="Validated payload carried by this event."
    )