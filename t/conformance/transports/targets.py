"""Transports the conformance suite runs against.

Each target names a broker URL, how to reach a local instance, and the
documented differences (``model.Semantics``) the model allows for it. A
target whose service isn't reachable is skipped, so the suite runs against
whatever is up. ``docker-compose.yml`` next to this file starts them all.
"""
from __future__ import annotations

import os
import socket
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .model import Semantics

#: Redis and PGMQ never restore a message on their own within a test run;
#: the suite expires visibility timeouts itself (``expire()`` below).
LONG_TIMEOUT = 3600


@dataclass(frozen=True)
class Target:
    name: str
    url: str | Callable[[], str]
    port: int | None = None
    transport_options: dict[str, Any] = field(default_factory=dict)
    queue_arguments: dict[str, Any] = field(default_factory=dict)
    semantics: Semantics = field(default_factory=Semantics)
    #: How many consumers may share one queue. Kafka assigns a partition to
    #: one member of a consumer group, so a second consumer gets nothing.
    consumers: int = 2
    #: Seconds a published message may take to become visible to ``get``.
    settle: float = 0.0
    #: Known gaps, by test name, each with the reason or upstream report.
    #: They run as strict xfails, so a fix shows up as an XPASS.
    known: dict[str, str] = field(default_factory=dict)

    def reachable(self) -> bool:
        if self.port is None:
            return True
        try:
            with socket.create_connection(('127.0.0.1', self.port), timeout=1):
                return True
        except OSError:
            return False

    def resolve_url(self, port: int | None = None) -> str:
        url = self.url() if callable(self.url) else self.url
        if port is not None and self.port is not None:
            url = url.replace(f':{self.port}', f':{port}')
        return url


# Child processes (test_faults.py) inherit the folder through the environment.
_FS = os.environ.setdefault('CONFORMANCE_FS_FOLDER',
                            tempfile.mkdtemp(prefix='kombu-conformance-'))
FILESYSTEM_OPTIONS = {'data_folder_in': _FS, 'data_folder_out': _FS}

#: The virtual transports below remove a message from the store when they
#: deliver it and keep it unacked only in the consumer's memory
#: (``virtual/base.py:155-314``); it goes back on close or garbage collection
#: (``restore_unacked_once``), so a consumer that dies first loses it.
CLIENT_SIDE_UNACKED = ('unacked messages live in the consumer process; '
                       'one that exits without closing loses them')

VT = Semantics(visibility_timeout=True)
#: Only AMQP and Redis set ``delivery_info['redelivered']``; the other virtual
#: transports write the flag where nothing reads it (model rule 7).
NO_FLAG = Semantics(redelivered_flag=False)
AMQP = Semantics(ack_is_confirmed=False)

TARGETS = [
    Target('redis', 'redis://127.0.0.1:6379/5', 6379, semantics=VT,
           transport_options={'visibility_timeout': LONG_TIMEOUT}),
    Target('valkey', 'redis://127.0.0.1:6380/5', 6380, semantics=VT,
           transport_options={'visibility_timeout': LONG_TIMEOUT}),
    Target('rabbitmq-classic', 'pyamqp://guest:guest@127.0.0.1:5672//', 5672,
           semantics=AMQP),
    Target('rabbitmq-quorum', 'pyamqp://guest:guest@127.0.0.1:5672//', 5672,
           semantics=AMQP, queue_arguments={'x-queue-type': 'quorum'}),
    Target('pgmq', 'pgmq://postgres:postgres@127.0.0.1:5433/postgres', 5433,
           semantics=Semantics(visibility_timeout=True, redelivered_flag=False,
                               close_waits_for_timeout=True),
           transport_options={'visibility_timeout': LONG_TIMEOUT,
                              'wait_time_seconds': 0, 'polling_interval': 0.1}),
    Target('sqlalchemy', 'sqla+postgresql://postgres:pw@127.0.0.1:5432/celery', 5432,
           semantics=NO_FLAG, transport_options={'pool_timeout': 5}),
    Target('mongodb', 'mongodb://127.0.0.1:27017/kombu_conformance', 27017,
           semantics=NO_FLAG),
    Target('filesystem', 'filesystem://', semantics=NO_FLAG,
           transport_options=FILESYSTEM_OPTIONS),
    Target('memory', 'memory://', semantics=NO_FLAG, known={
        'test_matches_model': 'drops a closed channel\'s unacked messages: '
                              'do_restore = False (memory.py:38)',
    }),
    # A get after any other read of the queue is a Consul blocking query on
    # the last index seen, so it can wait the full 10s even when messages are
    # ready (consul.py:228-231, 262-265).
    Target('consul', 'consul://127.0.0.1:8500/', 8500, semantics=NO_FLAG, settle=11.0),
    Target('etcd', 'etcd://127.0.0.1:2379/', 2379, semantics=NO_FLAG),
    Target('zookeeper', 'zookeeper://127.0.0.1:2181/', 2181, semantics=NO_FLAG, known={
        'test_close_frees_broker_connections':
            'a closed channel never stops its KazooClient, so ZooKeeper '
            'refuses new connections once maxClientCnxns (60) are open',
    }),
    Target('confluentkafka', 'confluentkafka://127.0.0.1:9092', 9092,
           semantics=NO_FLAG, consumers=1, settle=10.0,
           # A consumer that dies holds its partition until its session
           # times out (45s by default); 6s is the broker's minimum.
           transport_options={'wait_time_seconds': 1, 'kafka_consumer_config': {
               'session.timeout.ms': 6000, 'heartbeat.interval.ms': 1000}}),
]

BY_NAME = {t.name: t for t in TARGETS}

for _name in ('sqlalchemy', 'mongodb', 'filesystem', 'consul', 'etcd', 'zookeeper'):
    BY_NAME[_name].known['test_consumer_crash_before_ack'] = CLIENT_SIDE_UNACKED

KAFKA_ACK = ('ack() commits the consumer position, which settles every '
             'message fetched before it too (confluentkafka.py:132-137)')
BY_NAME['confluentkafka'].known.update({
    'test_matches_model': KAFKA_ACK,
    'test_scenario[ack_leaves_others_pending]': KAFKA_ACK,
    'test_scenario[recover_redelivers]':
        'QoS keeps pending messages in _not_yet_acked, so the inherited '
        'restore_unacked() finds none to recover (confluentkafka.py:104-130)',
})

# Every scenario that ends with a consumer still holding a message.
for _scenario in ('ack_leaves_others_pending', 'close_redelivers', 'no_double_delivery',
                  'reconnect_redelivers', 'recover_redelivers', 'requeue_redelivers'):
    BY_NAME['memory'].known[f'test_scenario[{_scenario}]'] = BY_NAME['memory'].known[
        'test_matches_model']
