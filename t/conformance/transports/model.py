"""A reference model of what a queue does with each message.

Line numbers point at kombu 7dca15a (upstream main when this was written).

Each message on a queue is in one of three places:

``ready``
    In the queue, waiting. ``basic_get`` or a consumer can take it.
``unacked``
    Delivered to one consumer (channel) and not yet settled. Nobody else
    gets it while it sits here.
``gone``
    Settled for good. It is never delivered again.

The rules:

1. Delivery takes a ready message and makes it unacked by that channel. A
   message that is unacked or gone is never delivered (no duplicate
   delivery). AMQP 0-9-1 ``basic.get``/``basic.deliver``; the virtual
   transports pop it from the store, ``virtual/base.py:668-676``.
2. ``ack`` makes it gone (``virtual/base.py:242-244``,
   ``redis.py:499-501``, ``pgmq.py:567-580``). A message is never delivered
   again after its ack succeeded. AMQP 0-9-1 acks get no reply, so
   ``ack()`` returning only means the frame was written: if the connection
   drops first, the broker requeues the message
   (https://www.rabbitmq.com/docs/confirms#automatic-requeueing).
   ``Semantics.ack_is_confirmed`` is false there.
3. ``reject(requeue=False)`` makes it gone, or dead-letters it when the
   queue has a dead-letter exchange (not modelled).
   ``virtual/base.py:246-250``, ``redis.py:503-508``, ``pgmq.py:582-595``.
4. ``reject(requeue=True)`` (``Message.requeue()``) makes it ready again
   (``virtual/base.py:246-250``, ``redis.py:503-505``).
5. ``basic_recover(requeue=True)`` makes every message unacked by that
   channel ready again (``virtual/base.py:682-686``, ``252-275``).
6. Closing the channel or the connection makes every message unacked by it
   ready again: ``Channel.close()`` calls ``restore_unacked_once()``
   (``virtual/base.py:796-812``, ``277-314``), and an AMQP broker requeues a
   closed channel's unacked deliveries
   (https://www.rabbitmq.com/docs/confirms#automatic-requeueing).
7. A message made ready again by rules 4 to 6 has
   ``delivery_info['redelivered']`` set when it is delivered next, where the
   transport supports the flag. The virtual transports write it
   (``virtual/base.py:746-754``, ``redis.py:1265-1270``); Celery reads it
   from ``delivery_info`` (celery ``worker/request.py:170``,
   ``app/trace.py:487``).
8. Visibility-timeout transports (Redis with ``ack_emulation``, PGMQ, SQS)
   also make a message unacked by any channel ready again once its
   visibility timeout passes (``redis.py:523-549``, ``pgmq.py:1-8``). A
   channel that closes without restoring leaves its messages invisible
   until then: PGMQ sets ``restore_at_shutdown = False``
   (``pgmq.py:266``), and a Redis consumer that loses its connection can't
   restore.
9. With ``basic_qos(prefetch_count=n)``, a channel holds at most ``n``
   unacked messages delivered to its consumers (``virtual/base.py:197-204``,
   ``759-765``). ``basic_get`` is not bound by it.
10. No message is lost: every message published and not gone is ready,
    unacked, or (rule 8) invisible.

Documented differences, which the model leaves alone:

* Ordering. AMQP queues are FIFO and put a requeued message back at its
  old position; Redis requeues at the head (``redis.py:503-505``); the
  model checks membership, not order.
* Priorities. Only some transports support them; this model publishes
  without a priority.
* The redelivered flag. See rule 7; ``Semantics.redelivered_flag`` says
  which transports set it.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field


class Divergence(AssertionError):
    """The transport did something the model doesn't allow."""


@dataclass(frozen=True)
class Semantics:
    """Where a transport's documented behaviour differs from the rules."""

    #: Sets ``delivery_info['redelivered']`` on a message made ready again.
    redelivered_flag: bool = True
    #: ``ack()`` returns only once the broker has settled the message (rule 2).
    ack_is_confirmed: bool = True
    #: Has a visibility timeout (rule 8).
    visibility_timeout: bool = False
    #: Closing a channel leaves its unacked messages invisible until the
    #: visibility timeout instead of making them ready (rule 8).
    close_waits_for_timeout: bool = False


@dataclass
class QueueModel:
    semantics: Semantics = field(default_factory=Semantics)
    ready: Counter = field(default_factory=Counter)
    unacked: dict = field(default_factory=dict)      # channel -> Counter
    invisible: Counter = field(default_factory=Counter)
    gone: set = field(default_factory=set)
    returned: set = field(default_factory=set)       # made ready again once

    def publish(self, n):
        self.ready[n] += 1

    def held(self, ch):
        return self.unacked.setdefault(ch, Counter())

    def deliver(self, ch, n):
        """Rule 1. Return whether the redelivered flag is expected."""
        if self.ready[n] <= 0:
            if n in self.gone:
                raise Divergence(f'message {n} delivered again after it was settled')
            owners = [c for c, held in self.unacked.items() if held[n]]
            if owners:
                raise Divergence(f'message {n} delivered to {ch} while unacked by {owners}')
            if self.invisible[n]:
                raise Divergence(f'message {n} delivered before its visibility timeout')
            raise Divergence(f'message {n} was never published')
        self.ready[n] -= 1
        self.held(ch)[n] += 1
        return n in self.returned

    def _take(self, ch, n):
        held = self.held(ch)
        assert held[n] > 0, f'{ch} does not hold {n}'
        held[n] -= 1

    def ack(self, ch, n):                      # rule 2
        self._take(ch, n)
        self.gone.add(n)

    def reject(self, ch, n, requeue):          # rules 3 and 4
        self._take(ch, n)
        if requeue:
            self._return(n)
        else:
            self.gone.add(n)

    def _return(self, n, count=1):
        self.ready[n] += count
        self.returned.add(n)

    def recover(self, ch):                     # rule 5
        for n, k in self.unacked.pop(ch, Counter()).items():
            if k:
                self._return(n, k)

    def close(self, ch):                       # rules 6 and 8
        if self.semantics.close_waits_for_timeout:
            self.invisible.update(+self.unacked.pop(ch, Counter()))
        else:
            self.recover(ch)

    def lose(self, ch):
        """The channel's connection dropped without a clean close."""
        if self.semantics.visibility_timeout:
            self.invisible.update(+self.unacked.pop(ch, Counter()))
        else:
            self.recover(ch)

    def expire(self):                          # rule 8
        assert self.semantics.visibility_timeout
        for ch in list(self.unacked):
            self.recover(ch)
        for n, k in self.invisible.items():
            if k:
                self._return(n, k)
        self.invisible.clear()

    def outstanding(self):                     # rule 10
        total = Counter(self.ready)
        for held in self.unacked.values():
            total.update(held)
        total.update(self.invisible)
        return +total

    def unacked_count(self, ch):
        return sum(self.held(ch).values())
