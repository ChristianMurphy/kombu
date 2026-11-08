"""QoS prefetch bounds what a consumer holds unacked (model rule 9).

A consumer with ``prefetch_count=2`` on a queue of six messages must hold
two, get a third only after it acks one, and on close give the rest back.
"""
from __future__ import annotations

import time

from kombu import Consumer

from . import driver
from .test_sequential import GRACE

PREFETCH = 2
PUBLISHED = 6


def drain(conn, held, seconds):
    """Consume until ``PREFETCH`` are held (or ``seconds`` pass), then 1s more."""
    deadline = time.monotonic() + seconds
    extra = None
    while time.monotonic() < (extra or deadline):
        if extra is None and len(held) >= PREFETCH:
            extra = time.monotonic() + 1
        try:
            conn.drain_events(timeout=0.2)
        except TimeoutError:
            pass


def test_prefetch_bounds_unacked(target, queue_name):
    publisher = driver.Publisher(target, queue_name)
    conn = driver.connect(target)
    channel = conn.channel()
    held = []
    try:
        for n in range(PUBLISHED):
            publisher.publish(n)
        consumer = Consumer(channel, [driver.make_queue(target, queue_name)],
                            callbacks=[lambda body, message: held.append(message)],
                            accept=['json'])
        consumer.qos(prefetch_count=PREFETCH)
        consumer.consume()
        drain(conn, held, 2 * GRACE + target.settle)
        assert len(held) == PREFETCH, f'holds {len(held)} unacked with prefetch {PREFETCH}'
        held.pop(0).ack()
        drain(conn, held, 2 * GRACE + target.settle)
        assert len(held) == PREFETCH, f'holds {len(held)} after one ack'
    finally:
        conn.release()
        driver.release_leaks(channel)
        publisher.close()
        driver.purge_and_delete(target, queue_name)
