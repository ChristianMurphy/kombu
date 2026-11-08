"""A producer and two consumers on one queue: does the transport match the model?

Hypothesis generates sequences of publish, get, ack, reject, requeue,
recover, close-channel and reconnect operations, runs each against the
transport and the model (``model.py``) side by side, and checks every
delivery against the model. At the end it closes the consumers, drains the
queue and compares what is left with what the model says must be left. A
failure shrinks to the shortest sequence that shows the difference.
"""
from __future__ import annotations

import os
import uuid

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, rule

from . import driver
from .model import Divergence, QueueModel

EXAMPLES = int(os.environ.get('CONFORMANCE_EXAMPLES', '25'))
STEPS = int(os.environ.get('CONFORMANCE_STEPS', '20'))
#: Seconds to wait for a message the model says is ready.
GRACE = float(os.environ.get('CONFORMANCE_GRACE', '3'))


def check_delivery(target, model, ch, message):
    """Rules 1 and 7, for one delivery to channel ``ch``."""
    n = driver.body(message)
    expect_flag = model.deliver(ch, n)
    flag = driver.redelivered(message)
    if flag and not expect_flag:
        raise Divergence(f'first delivery of {n} is marked redelivered')
    if expect_flag and not flag and target.semantics.redelivered_flag:
        raise Divergence(f'redelivery of {n} is not marked redelivered')
    return n


def drain_and_compare(target, queue_name, model):
    """Rule 10: what the model says is outstanding comes back, once each."""
    expected = model.outstanding()
    got = {}
    client = driver.Client(target, queue_name)
    try:
        while True:
            missing = sum(expected.values()) > sum(got.values())
            message = client.get(wait=GRACE + target.settle if missing else 0.5)
            if message is None:
                break
            n = driver.body(message)
            got[n] = got.get(n, 0) + 1
            message.ack()
    finally:
        client.close()
    lost = {n: k - got.get(n, 0) for n, k in expected.items() if k > got.get(n, 0)}
    extra = {n: k - expected.get(n, 0) for n, k in got.items() if k > expected.get(n, 0)}
    if lost or extra:
        raise Divergence(f'after closing every consumer and draining: lost '
                         f'{lost or "none"}; delivered more often than the '
                         f'model allows {extra or "none"}')


def machine_class(target):
    consumer_ids = st.integers(0, target.consumers - 1)

    class TransportMachine(RuleBasedStateMachine):
        def __init__(self):
            super().__init__()
            self.queue_name = f'cf_{target.name.replace("-", "_")}_{uuid.uuid4().hex[:10]}'
            self.model = QueueModel(target.semantics)
            self.publisher = driver.Publisher(target, self.queue_name)
            self.clients = [driver.Client(target, self.queue_name)
                            for _ in range(target.consumers)]
            self.handles = [[] for _ in self.clients]   # (n, message) per consumer
            self.next_n = 0

        def _forget(self, c):
            self.handles[c] = []

        @rule()
        def publish(self):
            self.publisher.publish(self.next_n)
            self.model.publish(self.next_n)
            self.next_n += 1

        @rule(c=consumer_ids)
        def get(self, c):
            ready = sum(self.model.ready.values()) > 0
            wait = GRACE + target.settle if ready else 0
            message = self.clients[c].get(wait=wait)
            if message is None:
                if ready:
                    raise Divergence(
                        f'consumer {c} got nothing in {wait}s while '
                        f'{dict(+self.model.ready)} were ready')
                return
            n = check_delivery(target, self.model, c, message)
            self.handles[c].append((n, message))

        @rule(c=consumer_ids, pick=st.integers(0, 1000),
              how=st.sampled_from(['ack', 'reject', 'requeue']))
        def settle(self, c, pick, how):
            if not self.handles[c]:
                return
            n, message = self.handles[c].pop(pick % len(self.handles[c]))
            if how == 'ack':
                message.ack()
                self.model.ack(c, n)
            else:
                message.reject(requeue=how == 'requeue')
                self.model.reject(c, n, requeue=how == 'requeue')

        @rule(c=consumer_ids)
        def recover(self, c):
            self.clients[c].recover()
            self.model.recover(c)
            self._forget(c)

        @rule(c=consumer_ids)
        def close_channel(self, c):
            self.clients[c].close_channel()
            self.model.close(c)
            self._forget(c)

        @rule(c=consumer_ids)
        def reconnect(self, c):
            self.clients[c].reconnect()
            self.model.close(c)
            self._forget(c)

        if target.semantics.visibility_timeout:
            @rule()
            def visibility_timeout_passes(self):
                driver.expire(target, self.queue_name)
                self.model.expire()
                for c in range(len(self.clients)):
                    self._forget(c)

        def teardown(self):
            try:
                for c, client in enumerate(self.clients):
                    client.close()
                    self.model.close(c)
                if target.semantics.visibility_timeout:
                    driver.expire(target, self.queue_name)
                    self.model.expire()
                drain_and_compare(target, self.queue_name, self.model)
            finally:
                self.publisher.close()
                driver.purge_and_delete(target, self.queue_name)

    return TransportMachine


def run_machine(target):
    TransportMachine = machine_class(target)
    TransportMachine.TestCase.settings = settings(
        max_examples=EXAMPLES, stateful_step_count=STEPS, deadline=None,
        suppress_health_check=list(HealthCheck), derandomize=bool(os.environ.get('CI')))
    TransportMachine.TestCase().runTest()


def test_matches_model(target):
    run_machine(target)
