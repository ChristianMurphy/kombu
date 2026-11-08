from __future__ import annotations

import uuid

import pytest

from .targets import BY_NAME, TARGETS


def pytest_addoption(parser):
    parser.addoption('--transport', action='append', default=[],
                     help='Only run against these transports (targets.py names).')


def pytest_generate_tests(metafunc):
    if 'target' in metafunc.fixturenames:
        wanted = metafunc.config.getoption('--transport') or [t.name for t in TARGETS]
        metafunc.parametrize('target', [BY_NAME[n] for n in wanted], ids=wanted)


@pytest.fixture(autouse=True)
def _reachable_and_known(request):
    if 'target' not in request.fixturenames:
        return
    target = request.getfixturevalue('target')
    if not target.reachable():
        pytest.skip(f'{target.name} is not reachable')
    name = request.node.originalname
    scenario = getattr(request.node, 'callspec', None) and \
        request.node.callspec.params.get('scenario')
    reason = target.known.get(f'{name}[{scenario}]') or target.known.get(name)
    if reason:
        # Hypothesis may not reach a known gap in a given run, so only the
        # fixed sequences are strict.
        strict = name != 'test_matches_model'
        request.applymarker(pytest.mark.xfail(reason=reason, strict=strict))


@pytest.fixture
def queue_name(target):
    return f'cf_{target.name.replace("-", "_")}_{uuid.uuid4().hex[:10]}'
