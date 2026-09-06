from abc import ABC, abstractmethod

from shared.events import ScientificEvent


class EventStore(ABC):
    """
    Defines the persistence contract for scientific events.
    """

    @abstractmethod
    def save(self, event: ScientificEvent) -> None:
        """
        Persist one validated scientific event.
        """
        pass