"""Faults between a delivery and its ack: is the message settled exactly once?

A consumer gets a message, then its connection fails before or during the
ack. Afterwards (once the visibility timeout passes, for transports that
have one) the queue is drained. The model allows exactly two outcomes:

* the ack returned, so the message never comes back (rule 2), or
* the ack raised or never happened, so it comes back once (rules 6, 8, 10).

``test_fault_between_delivery_and_ack``
    The consumer connects through Toxiproxy, which cuts the connection,
    resets it, or delays it, before the ack.
``test_consumer_crash_before_ack``
    The consumer runs in a child process that exits without acking or
    closing, like a worker killed by the OOM killer.
``test_broker_restart_between_delivery_and_ack``
    The broker restarts before the ack. It needs the restartable services
    from ``docker-compose.yml`` (``CONFORMANCE_RESTART=1``).
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import textwrap
import time

import pytest

from . import driver, toxiproxy
from .model import Divergence
from .test_sequential import GRACE

TRIALS = int(os.environ.get('CONFORMANCE_FAULT_TRIALS', '5'))

#: Containers this suite may restart, and the port each one listens on.
RESTARTABLE = {
    'redis': ('transport-redis', 26379),
    'rabbitmq-classic': ('transport-rabbitmq', 25672),
    'rabbitmq-quorum': ('transport-rabbitmq', 25672),
    'pgmq': ('transport-pgmq', 15433),
}


def deliveries_after(target, queue_name, port=None):
    """Drain the queue; return how often each message came back."""
    if target.semantics.visibility_timeout:
        driver.expire(target, queue_name, port)
    client = connect_retrying(target, queue_name, port)
    got = {}
    try:
        while True:
            message = client.get(wait=1 + target.settle if not got else 0.5)
            if message is None:
                return got
            got[driver.body(message)] = got.get(driver.body(message), 0) + 1
            message.ack()
    finally:
        client.close()


def connect_retrying(target, queue_name, port, tries=3):
    """A pooled connection broken by the last fault can fail once."""
    for attempt in range(tries):
        try:
            return driver.Client(target, queue_name, port=port)
        except Exception:
            if attempt == tries - 1:
                raise
            time.sleep(1)


def check_outcome(target, n, acked, got, what):
    count = got.get(n, 0)
    if acked and not target.semantics.ack_is_confirmed:
        acked = None   # unknown: the broker may not have seen the ack
    if acked is None and count > 1:
        raise Divergence(f'{what}: message {n} came back {count}x')
    if acked and count:
        raise Divergence(f'{what}: the ack returned, yet message {n} came back {count}x')
    if acked is False and count != 1:
        raise Divergence(f'{what}: the ack failed, and message {n} came back {count}x')


@pytest.fixture
def proxy(target):
    if target.port is None:
        pytest.skip(f'{target.name} has no network connection to cut')
    if not toxiproxy.available():
        pytest.skip('toxiproxy is not running')
    p = toxiproxy.Proxy(f'conformance-{target.name}', target.port)
    yield p
    p.delete()


def apply(proxy, fault):
    if fault == 'cut':
        proxy.cut()
    elif fault == 'reset':
        proxy.reset_peer()
    elif fault == 'delay':
        proxy.delay(2000)


@pytest.mark.parametrize('fault', ['cut', 'reset', 'delay'])
def test_fault_between_delivery_and_ack(target, queue_name, proxy, fault):
    publisher = driver.Publisher(target, queue_name)
    failures = []
    try:
        for n in range(TRIALS):
            publisher.publish(n)
            consumer = connect_retrying(target, queue_name, proxy.port)
            message = consumer.get(wait=GRACE + target.settle)
            assert message is not None and driver.body(message) == n
            apply(proxy, fault)
            try:
                message.ack()
                acked = True
            except Exception:  # noqa: BLE001 - any failure means not acked
                acked = False
            proxy.restore()
            consumer.close()
            try:
                check_outcome(target, n, acked, deliveries_after(target, queue_name),
                              f'trial {n}, {fault}')
            except Divergence as exc:
                failures.append(str(exc))
    finally:
        proxy.restore()
        publisher.close()
        driver.purge_and_delete(target, queue_name)
    assert not failures, f'{len(failures)} of {TRIALS} trials: {failures}'


CHILD = textwrap.dedent('''
    import os, sys
    from t.conformance.transports import driver
    from t.conformance.transports.targets import BY_NAME
    target = BY_NAME[sys.argv[1]]
    message = driver.Client(target, sys.argv[2]).get(wait=float(sys.argv[3]))
    print(driver.body(message) if message is not None else 'none', flush=True)
    os._exit(0)
''')


def test_consumer_crash_before_ack(target, queue_name):
    if target.name == 'memory':
        pytest.skip('the memory transport lives in one process')
    publisher = driver.Publisher(target, queue_name)
    failures = []
    try:
        for n in range(TRIALS):
            publisher.publish(n)
            out = subprocess.run(
                [sys.executable, '-c', CHILD, target.name, queue_name,
                 str(GRACE + target.settle)],
                capture_output=True, text=True, timeout=120, check=False,
                env={**os.environ, 'PYTHONPATH': os.getcwd()})
            assert out.stdout.strip() == str(n), out.stdout + out.stderr
            try:
                check_outcome(target, n, False, deliveries_after(target, queue_name),
                              f'trial {n}, consumer exited before ack')
            except Divergence as exc:
                failures.append(str(exc))
    finally:
        publisher.close()
        driver.purge_and_delete(target, queue_name)
    assert not failures, f'{len(failures)} of {TRIALS} trials: {failures}'


def wait_for_port(port, seconds=60):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=1):
                return
        except OSError:
            time.sleep(0.5)
    raise TimeoutError(f'port {port} did not come back')


def test_broker_restart_between_delivery_and_ack(target, queue_name):
    if not os.environ.get('CONFORMANCE_RESTART') or target.name not in RESTARTABLE:
        pytest.skip('needs CONFORMANCE_RESTART=1 and a restartable service')
    container, port = RESTARTABLE[target.name]
    failures = []
    for n in range(TRIALS):
        conn = driver.connect(target, port)
        producer = conn.Producer()
        queue = driver.make_queue(target, queue_name)
        producer.publish({'n': n}, exchange=queue.exchange, routing_key=queue.routing_key,
                         declare=[queue], serializer='json', delivery_mode=2)
        conn.release()
        consumer = driver.Client(target, queue_name, port=port)
        message = consumer.get(wait=GRACE)
        assert message is not None and driver.body(message) == n
        subprocess.run(['docker', 'restart', container], check=True,
                       capture_output=True, timeout=120)
        wait_for_port(port)
        time.sleep(3)
        try:
            message.ack()
            acked = True
        except Exception:  # noqa: BLE001 - any failure means not acked
            acked = False
        consumer.close()
        try:
            check_outcome(target, n, acked, deliveries_after(target, queue_name, port=port),
                          f'trial {n}, broker restart')
        except Divergence as exc:
            failures.append(str(exc))
    assert not failures, f'{len(failures)} of {TRIALS} trials: {failures}'
