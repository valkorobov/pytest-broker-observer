"""Opt-in reproducer for the Allure/pytest declared-floor incompatibility.

Select this file explicitly with pytest and ``--alluredir``. It is intentionally
outside the ordinary test-file naming pattern; see docs/ci-compatibility.md.
"""

import asyncio

import pytest


@pytest.fixture
def marker():
    return "fixture-ready"


def test_fixture(marker):
    assert marker == "fixture-ready"


@pytest.mark.asyncio
async def test_async():
    await asyncio.sleep(0)
    assert True
