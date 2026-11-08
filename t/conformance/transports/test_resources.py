"""Closing a connection gives back what it held on the broker.

A long-running consumer or producer opens and closes connections over and
over (reconnects, connection pools, one-off tools). Each close should free
the broker connection the channel used, without waiting for the garbage
collector.
"""
from __future__ import annotations

import os

from . import driver

CYCLES = int(os.environ.get('CONFORMANCE_CYCLES', '70'))


def test_close_frees_broker_connections(target, queue_name):
    """Publish, get and ack one message on a new connection, many times."""
    try:
        for n in range(CYCLES):
            with driver.connect(target) as conn:
                channel = conn.channel()
                queue = driver.make_queue(target, queue_name)
                conn.Producer(channel).publish(
                    {'n': n}, exchange=queue.exchange, routing_key=queue.routing_key,
                    declare=[queue], serializer='json')
                message = queue(channel).get(no_ack=False)
                while message is None:
                    message = queue(channel).get(no_ack=False)
                message.ack()
    finally:
        driver.release_all_leaks()
        driver.purge_and_delete(target, queue_name)
