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
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

from shared.payloads import (
    SyntheticPayload,
    MuRun2010BPayload,
)


class ScientificEvent(BaseModel):
    """
    Common event envelope shared across the entire pipeline.

    The payload contains source-specific scientific data.
    The source field determines which payload model is valid.
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

    payload: SyntheticPayload | MuRun2010BPayload = Field(
        ...,
        description="Validated source-specific payload carried by this event."
    )

    @model_validator(mode="after")
    def validate_source_payload(self):
        """
        Ensure that the event source matches its payload type.
        """

        if self.source == "synthetic":
            if not isinstance(self.payload, SyntheticPayload):
                raise ValueError(
                    "source='synthetic' requires SyntheticPayload"
                )

        elif self.source == "murun2010b":
            if not isinstance(self.payload, MuRun2010BPayload):
                raise ValueError(
                    "source='murun2010b' requires MuRun2010BPayload"
                )

        else:
            raise ValueError(
                f"Unsupported event source: {self.source}"
            )

        return self