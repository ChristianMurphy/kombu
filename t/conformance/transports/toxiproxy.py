"""A tiny client for Toxiproxy's HTTP API (https://github.com/Shopify/toxiproxy).

The fault tests put one consumer's connection behind a proxy, then cut or
slow that connection between a delivery and its ack, the way a network
blip or a busy broker would, without touching kombu's code.
"""
from __future__ import annotations

import contextlib
import json
import socket
import urllib.request

API = 'http://127.0.0.1:8474'


def _call(method, path, body=None):
    req = urllib.request.Request(
        API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=5) as resp:
        raw = resp.read()
    return json.loads(raw) if raw else None


def available() -> bool:
    try:
        _call('GET', '/version')
        return True
    except OSError:
        return False


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class Proxy:
    def __init__(self, name: str, upstream_port: int):
        self.name = name
        self.port = _free_port()
        with contextlib.suppress(Exception):
            _call('DELETE', f'/proxies/{name}')
        _call('POST', '/proxies', {
            'name': name, 'listen': f'127.0.0.1:{self.port}',
            'upstream': f'127.0.0.1:{upstream_port}', 'enabled': True})

    def cut(self) -> None:
        """Close every connection through the proxy and refuse new ones."""
        _call('POST', f'/proxies/{self.name}', {'enabled': False})

    def restore(self) -> None:
        for toxic in _call('GET', f'/proxies/{self.name}/toxics') or []:
            _call('DELETE', f'/proxies/{self.name}/toxics/{toxic["name"]}')
        _call('POST', f'/proxies/{self.name}', {'enabled': True})

    def reset_peer(self) -> None:
        """Send TCP RST on every connection through the proxy (new ones too)."""
        _call('POST', f'/proxies/{self.name}/toxics', {
            'name': 'reset', 'type': 'reset_peer', 'stream': 'upstream',
            'attributes': {'timeout': 0}})

    def delay(self, latency_ms: int) -> None:
        _call('POST', f'/proxies/{self.name}/toxics', {
            'name': 'slow', 'type': 'latency', 'stream': 'downstream',
            'attributes': {'latency': latency_ms, 'jitter': 0}})

    def delete(self) -> None:
        with contextlib.suppress(Exception):
            _call('DELETE', f'/proxies/{self.name}')
