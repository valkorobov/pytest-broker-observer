"""Expose lazy factory fixtures through a stdlib-only pytest entry point.

Pytest loads the fixture module; importing this entry point alone imports no
optional dependencies. Projects configure their own transports and observers,
and watch contexts retain ownership of active resources.
"""

pytest_plugins = ["event_observer.integrations._pytest_factories"]
