"""Thin wrappers that drive one transport through kombu's public API."""
from __future__ import annotations

import contextlib
import gc
import time

from kombu import Connection, Exchange, Queue

from .targets import Target


def make_queue(target: Target, name: str) -> Queue:
    return Queue(name, Exchange(name, 'direct', durable=True), routing_key=name,
                 durable=True, auto_delete=False,
                 queue_arguments=target.queue_arguments or None)


def connect(target: Target, port: int | None = None, **options) -> Connection:
    return Connection(target.resolve_url(port),
                      transport_options={**target.transport_options, **options})


class Publisher:
    def __init__(self, target: Target, queue_name: str):
        self.queue = make_queue(target, queue_name)
        self.conn = connect(target)
        self.producer = self.conn.Producer()
        self.producer.declare()
        self.queue(self.producer.channel).declare()

    def publish(self, n: int) -> None:
        self.producer.publish({'n': n}, exchange=self.queue.exchange,
                              routing_key=self.queue.routing_key,
                              serializer='json', delivery_mode=2)

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.conn.release()
        release_leaks(self.producer.channel)


class Client:
    """One consumer: a connection, a channel, and the queue bound to it."""

    def __init__(self, target: Target, queue_name: str, port: int | None = None,
                 **options):
        self.target = target
        self.queue_name = queue_name
        self.port = port
        self.options = options
        self.conn = connect(target, port, **options)
        self._open_channel()

    def _open_channel(self) -> None:
        self.channel = self.conn.channel()
        self.queue = make_queue(self.target, self.queue_name)(self.channel)
        self.queue.declare()

    def get(self, wait: float = 0.0):
        """``basic_get``, retried for up to ``wait`` seconds while empty."""
        deadline = time.monotonic() + wait
        while True:
            message = self.queue.get(no_ack=False, accept=['json'])
            if message is not None or time.monotonic() >= deadline:
                return message
            time.sleep(0.05)

    def recover(self) -> None:
        self.channel.basic_recover(requeue=True)

    def close_channel(self) -> None:
        self.channel.close()
        release_leaks(self.channel)
        self._open_channel()

    def reconnect(self) -> None:
        self.conn.close()
        release_leaks(self.channel)
        self.conn = connect(self.target, self.port, **self.options)
        self._open_channel()

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.conn.release()
        release_leaks(self.channel)


def release_leaks(channel) -> None:
    """Free what a closed channel keeps, so long runs don't run out.

    The ZooKeeper channel never stops its Kazoo client
    (``test_resources.py`` checks that on its own). A closed SQLAlchemy
    channel keeps its session, and a pooled connection, until the garbage
    collector frees it; a run that opens many channels can use up the pool
    before that happens.
    """
    client = getattr(channel, '_client', None)
    if client is not None and type(client).__name__ == 'KazooClient':
        with contextlib.suppress(Exception):
            client.stop()
            client.close()
    gc.collect()


def release_all_leaks() -> None:
    """Stop every Kazoo client this process leaked (see ``release_leaks``)."""
    for obj in gc.get_objects():
        if type(obj).__name__ == 'KazooClient':
            with contextlib.suppress(Exception):
                obj.stop()
                obj.close()
    gc.collect()


def body(message) -> int:
    return message.payload['n']


def redelivered(message) -> bool:
    return bool((message.delivery_info or {}).get('redelivered'))


def expire(target: Target, queue_name: str, port: int | None = None) -> None:
    """Make every unacked message's visibility timeout pass now."""
    if target.name in ('redis', 'valkey'):
        with connect(target, port, visibility_timeout=0) as conn:
            conn.default_channel.qos.restore_visible(num=100000)
    elif target.name == 'pgmq':
        import psycopg
        dsn = target.resolve_url(port).replace('pgmq://', 'postgresql://')
        with psycopg.connect(dsn, autocommit=True) as pg:
            table = f'pgmq.q_{queue_name}'.replace('-', '_')
            pg.execute(f'UPDATE {table} SET vt = clock_timestamp()')
    else:  # pragma: no cover
        raise NotImplementedError(target.name)


def purge_and_delete(target: Target, queue_name: str) -> None:
    with contextlib.suppress(Exception), connect(target) as conn:
        channel = conn.default_channel
        queue = make_queue(target, queue_name)(channel)
        queue.purge()
        queue.delete()
    release_leaks(channel)
