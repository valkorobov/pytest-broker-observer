"""Expose a passive pytest entry point without installing fixtures or hooks.

Importing the plugin changes neither event-loop settings nor test collection.
Tests explicitly create observers and own their transport and watcher lifetimes;
optional integrations are loaded only when their APIs are requested.
"""
