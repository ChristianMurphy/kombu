=============================
 Transport conformance suite
=============================

These tests check that kombu's transports settle messages the same way,
against the services themselves rather than mocks. They are not part of the
unit suite.

``model.py`` states the rules, each with the kombu line or the document it
comes from: a message is ready, unacked by one channel, or gone; ``ack`` and
``reject`` settle it; ``requeue``, ``recover``, closing a channel and (for
Redis and PGMQ) a passed visibility timeout make it ready again; prefetch
bounds what a consumer holds; nothing is delivered twice at once, after its
ack, or lost. Where a transport documents a different behaviour (no
redelivered flag, PGMQ waiting for the visibility timeout on close, AMQP
acks without a reply) ``targets.py`` says so with ``Semantics``.

``test_sequential.py``
    Hypothesis drives a producer and two consumers on one queue with
    publish, get, ack, reject, requeue, recover, close-channel, reconnect
    and (Redis, PGMQ) visibility-timeout steps, checks each delivery against
    the model, then drains the queue and compares what is left. A failure
    shrinks to the shortest sequence that shows it.
``test_scenarios.py``
    The short sequences each rule needs, spelled out (requeue then get,
    close then get, ack one of two, ...), so every run checks them even when
    Hypothesis doesn't reach them.
``test_prefetch.py``
    A consumer with ``prefetch_count=2`` holds two messages, and a third
    only after an ack.
``test_faults.py``
    A fault between delivery and ack: Toxiproxy cuts, resets or delays the
    consumer's connection; the consumer process exits without acking; the
    broker restarts. The message must come back once, or never if the ack
    returned.
``test_resources.py``
    Opening and closing a connection 70 times in a row works.

Running
=======

Start the services you want (``docker-compose.yml`` has them all), then::

    pytest t/conformance/transports                        # every reachable transport
    pytest t/conformance/transports --transport redis      # one
    CONFORMANCE_RESTART=1 pytest t/conformance/transports/test_faults.py \
        --transport redis --transport rabbitmq-quorum --transport pgmq

Unreachable services are skipped. ``CONFORMANCE_EXAMPLES``,
``CONFORMANCE_STEPS``, ``CONFORMANCE_FAULT_TRIALS`` and ``CONFORMANCE_GRACE``
tune the run. The restart test restarts only the ``transport-*`` services.

Known gaps
==========

Each target in ``targets.py`` lists its known gaps with the reason. They run
as strict xfails, so a fix shows up as an unexpected pass:

* memory drops a closed channel's unacked messages (``do_restore = False``).
* SQLAlchemy, MongoDB, filesystem, Consul, etcd and ZooKeeper remove a
  message from the store when they deliver it, so a consumer that dies
  before ack or close loses it.
* ZooKeeper never stops a closed channel's Kazoo client, so the server
  refuses connections once 60 are open.
* Confluent Kafka's ``ack()`` commits the consumer position, which also
  settles the messages fetched before it, and ``basic_recover()`` finds
  nothing to recover.
* Consul: a get after another read of the queue waits on a blocking query
  for up to 10s, even with messages ready, so the suite allows 11s.

Known gaps in ``test_sequential.py`` are not strict, since a Hypothesis run
can miss them; the fixed sequences in ``test_scenarios.py`` are.

Mutations
=========

Each rule was checked by planting a bug in a scratch copy of kombu:
requeue dropping the message, close keeping unacked messages, recover doing
nothing, prefetch ignored (filesystem), no redelivered flag, and ack leaving
the unacked record (Redis). ``test_scenarios.py`` and ``test_prefetch.py``
caught all six on every run. ``test_sequential.py`` at 40 examples caught
each one in some runs and missed it in others, which is why the fixed
sequences exist.
