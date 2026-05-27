import asyncio
import logging
from typing import Any, TypedDict

LOGGER = logging.getLogger(__name__)


class Event(TypedDict):
    type: str
    data: Any


class Broadcaster:
    def __init__(self):
        self._subscribers: list[asyncio.Queue[Event]] = []
        self._previous: dict[str, Any] = {}

    def broadcast(self, event: Event):
        if self._previous.get(event["type"]) == event["data"]:
            return

        for subscriber in self._subscribers:
            try:
                subscriber.put_nowait(event)
            except asyncio.QueueFull:
                LOGGER.error(f"Queue full, passing subscriber {subscriber}")
        self._previous[event["type"]] = event["data"]

    def subscribe(self) -> asyncio.Queue[Event]:
        subscriber: asyncio.Queue[Event] = asyncio.Queue(maxsize=10)
        self._subscribers.append(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: asyncio.Queue[Event]):
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)
