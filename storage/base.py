from abc import ABC, abstractmethod

from shared.events import ScientificEvent


class EventStore(ABC):
    """
    Defines the persistence contract for scientific events.
    """

    @abstractmethod
    def save(self, event: ScientificEvent) -> bool:
        """
        Persist one validated scientific event.

        Returns True when a new event is persisted.
        Returns False when the event was already persisted.
        """
        pass
