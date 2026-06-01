import asyncio
import logging
from typing import Literal

import pytest
from pytest import LogCaptureFixture

from daq_queuing_service.broadcaster import Broadcaster, Event

TEST_EVENTS = Literal["test"]


async def test_broadcast_broadcasts_event_to_subscribers():
    broadcaster: Broadcaster[TEST_EVENTS] = Broadcaster()
    sub_1 = broadcaster.subscribe()
    sub_2 = broadcaster.subscribe()
    event: Event[TEST_EVENTS] = {"type": "test", "data": 123}
    broadcaster.broadcast(event)
    assert await sub_1.get() == event
    assert await sub_2.get() == event


def test_if_event_is_same_as_previous_one_then_event_not_broadcasted():
    broadcaster: Broadcaster[TEST_EVENTS] = Broadcaster()
    subscriber = broadcaster.subscribe()
    event: Event[TEST_EVENTS] = {"type": "test", "data": "the same"}
    broadcaster.broadcast(event)
    assert subscriber.get_nowait() == event
    broadcaster.broadcast(event)
    with pytest.raises(asyncio.QueueEmpty):
        subscriber.get_nowait()


def test_if_subscriber_reaches_max_queue_items_then_error_handled_and_logged(
    caplog: LogCaptureFixture,
):
    broadcaster: Broadcaster[TEST_EVENTS] = Broadcaster(max_queue_size=10)
    _ = broadcaster.subscribe()
    sub_2 = broadcaster.subscribe()
    for i in range(10):
        broadcaster.broadcast({"type": "test", "data": i})
        sub_2.get_nowait()

    # Now sub_1 has 10 items in it's queue
    with caplog.at_level(logging.ERROR):
        broadcaster.broadcast({"type": "test", "data": "last"})

    assert "Queue full, passing subscriber" in caplog.text

    # Other subscribers unaffected
    assert sub_2.get_nowait() == {"type": "test", "data": "last"}


def test_if_subscriber_unsubscribes_then_it_no_longer_receives_broadcasts():
    broadcaster: Broadcaster[TEST_EVENTS] = Broadcaster()
    sub_1 = broadcaster.subscribe()
    sub_2 = broadcaster.subscribe()
    broadcaster.broadcast({"type": "test", "data": 1})
    broadcaster.unsubscribe(sub_2)
    broadcaster.broadcast({"type": "test", "data": 2})

    assert sub_1.get_nowait() == {"type": "test", "data": 1}
    assert sub_2.get_nowait() == {"type": "test", "data": 1}
    assert sub_1.get_nowait() == {"type": "test", "data": 2}
    with pytest.raises(asyncio.QueueEmpty):
        sub_2.get_nowait()
