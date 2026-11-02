"""Shared capability applicability metadata for asynchronous and blocking contracts."""

from dataclasses import dataclass
from typing import Callable

import pytest

from ...models import TransportCapabilities


@dataclass(frozen=True, slots=True)
class ContractPrecondition:
    """Describe one declared capability required by an optional contract case.

    :ivar capability: TransportCapabilities attribute inspected before optional work begins.
    :ivar value: Required Boolean value or member of a declared capability set.
    """

    capability: str
    value: str | bool


def require_capability(
    capabilities: TransportCapabilities, precondition: ContractPrecondition, record_property: Callable
) -> None:
    """Record identical applicability results for both transport execution models.

    :param capabilities: Static semantics declared by the adapter under contract test.
    :param precondition: Capability requirement for the current optional case.
    :param record_property: Pytest recorder receiving the requirement and applicability metadata.
    :returns: None when the adapter advertises the required semantics.
    :raises pytest.skip.Exception: If an optional capability is not declared, with NOT_APPLICABLE metadata.
    """
    declared = getattr(capabilities, precondition.capability)
    applicable = (
        precondition.value in declared
        if isinstance(declared, (set, frozenset))
        else declared == precondition.value
    )
    record_property("contract_precondition", f"{precondition.capability}={precondition.value}")
    record_property("contract_applicability", "APPLICABLE" if applicable else "NOT_APPLICABLE")
    if not applicable:
        pytest.skip(
            f"NOT_APPLICABLE: adapter does not declare {precondition.capability}={precondition.value}"
        )
