import asyncio
from typing import Any, TypedDict


class Event(TypedDict):
    type: str
    data: Any


class Broadcaster:
    def __init__(self):
        self._subscribers: list[asyncio.Queue[Event]] = []

    def broadcast(self, event: Event):
        for subscriber in self._subscribers:
            subscriber.put_nowait(event)

    def subscribe(self) -> asyncio.Queue[Event]:
        subscriber: asyncio.Queue[Event] = asyncio.Queue(maxsize=10)
        self._subscribers.append(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: asyncio.Queue[Event]):
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)
