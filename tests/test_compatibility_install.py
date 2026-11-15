"""The native SDK probe allows asyncio's own sockets while blocking client I/O."""

import importlib.util
from pathlib import Path
import socket
import unittest

import pytest
import redis.asyncio


@pytest.mark.parametrize("optional", [False, True], ids=["native", "redis-extra"])
def test_redis_probe_guards_client_connect_after_asyncio_bootstrap(monkeypatch, optional):
    path = Path(__file__).resolve().parents[1] / "tools/check_compatibility_install.py"
    spec = importlib.util.spec_from_file_location("compatibility_install", path)
    assert spec is not None and spec.loader is not None
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    monkeypatch.setattr(helper, "assert_installed", lambda: None)
    if optional:
        # The full unit environment already contains unrelated features. The
        # clean extra job checks their absence, independently of this regression.
        monkeypatch.setattr(helper, "OPTIONAL_MODULES", ("redis",))
        monkeypatch.setattr(helper.OptionalInstallTests, "extra", "redis")
        case = helper.OptionalInstallTests("test_independently_installed_extra")
    else:
        case = helper.NativeInstallTests("test_redis_async_real_pool_retry_and_atomic_pipeline_api")

    original_socketpair = socket.socketpair
    original_connect = socket.socket.connect
    startup = object()
    calls = []

    def connect(sock, address):
        if address is startup:
            calls.append("asyncio self-pipe")
            return
        return original_connect(sock, address)

    def socketpair(*args, **kwargs):
        # Exercise the Python-level connect used by Windows socketpair
        # without starting a TCP listener on platforms with a native socketpair.
        socket.socket.connect(None, startup)
        return original_socketpair(*args, **kwargs)

    from_url = redis.asyncio.Redis.from_url

    def checked_from_url(*args, **kwargs):
        with pytest.raises(AssertionError, match="must not acquire network resources"):
            socket.socket.connect(None, ("localhost", 6379))
        calls.append("blocked broker socket")
        return from_url(*args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket, "socketpair", socketpair)
    monkeypatch.setattr(redis.asyncio.Redis, "from_url", checked_from_url)

    result = case.run(unittest.TestResult())
    assert result.wasSuccessful(), result.failures + result.errors
    assert calls == ["asyncio self-pipe", "blocked broker socket"]
