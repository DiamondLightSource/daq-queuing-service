import asyncio
from collections.abc import Iterable, Mapping
from typing import Any, Generic, TypedDict, TypeVar

from pydantic import BaseModel

from daq_queuing_service.log import LOGGER

T = TypeVar("T", bound=str)


class Event(TypedDict, Generic[T]):
    type: T
    data: Any


def serialise(data: Any) -> Any:
    if isinstance(data, BaseModel):
        return data.model_dump()

    if isinstance(data, (str, bytes)):
        return data

    if isinstance(data, Mapping):
        return {key: serialise(value) for key, value in data.items()}  # type: ignore

    if isinstance(data, Iterable):
        return [serialise(item) for item in data]  # type: ignore

    return data


class Broadcaster(Generic[T]):
    def __init__(self, max_queue_size: int = 10):
        self._subscribers: list[asyncio.Queue[Event[T]]] = []
        self._previous: dict[str, Any] = {}
        self._max_queue_size = max_queue_size

    def broadcast(self, event: Event[T]):
        serialised = serialise(event["data"])

        if serialised == self._previous.get(event["type"]):
            return

        for subscriber in self._subscribers:
            try:
                subscriber.put_nowait(Event(type=event["type"], data=serialised))
            except asyncio.QueueFull:
                LOGGER.error(f"Queue full, passing subscriber {subscriber}")

        self._previous[event["type"]] = serialised

    def subscribe(self) -> asyncio.Queue[Event[T]]:
        subscriber: asyncio.Queue[Event[T]] = asyncio.Queue(
            maxsize=self._max_queue_size
        )
        self._subscribers.append(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: asyncio.Queue[Event[T]]):
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)
