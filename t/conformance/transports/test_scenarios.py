"""Fixed operation sequences, one per model rule, on every transport.

Hypothesis (``test_sequential.py``) explores at random and can miss a short
sequence in a given run. These spell out the sequences each rule needs, so
every run checks them. Each runs on the same machine and model as the
Hypothesis test, and ends with the same drain and comparison.
"""
from __future__ import annotations

import pytest

from .test_sequential import machine_class

SCENARIOS = {
    # rule 2
    'ack_settles': ['publish', ('get', 0), ('settle', 0, 0, 'ack')],
    # rule 3
    'reject_settles': ['publish', ('get', 0), ('settle', 0, 0, 'reject')],
    # rules 4 and 7
    'requeue_redelivers': ['publish', ('get', 0), ('settle', 0, 0, 'requeue'),
                           ('get', 0)],
    # rule 5
    'recover_redelivers': ['publish', ('get', 0), ('recover', 0), ('get', 0)],
    # rule 6
    'close_redelivers': ['publish', ('get', 0), ('close_channel', 0), ('get', 0)],
    'reconnect_redelivers': ['publish', ('get', 0), ('reconnect', 0), ('get', 0)],
    # rule 1, with two consumers
    'no_double_delivery': ['publish', ('get', 0), ('get', 1)],
    # rules 2 and 10: acking one message leaves the other one pending
    'ack_leaves_others_pending': ['publish', 'publish', ('get', 0), ('get', 0),
                                  ('settle', 0, 1, 'ack')],
    # rule 8
    'visibility_timeout_redelivers': ['publish', ('get', 0),
                                      'visibility_timeout_passes', ('get', 1)],
}


@pytest.mark.parametrize('scenario', sorted(SCENARIOS))
def test_scenario(target, scenario):
    steps = SCENARIOS[scenario]
    names = {s if isinstance(s, str) else s[0] for s in steps}
    if 'visibility_timeout_passes' in names and not target.semantics.visibility_timeout:
        pytest.skip(f'{target.name} has no visibility timeout')
    if any(not isinstance(s, str) and s[0] == 'get' and s[1] >= target.consumers
           for s in steps):
        pytest.skip(f'{target.name} runs one consumer per queue')
    machine = machine_class(target)()
    try:
        for step in steps:
            name, *args = (step,) if isinstance(step, str) else step
            if name == 'settle':
                c, pick, how = args
                getattr(machine, name)(c=c, pick=pick, how=how)
            elif args:
                getattr(machine, name)(c=args[0])
            else:
                getattr(machine, name)()
    finally:
        machine.teardown()
