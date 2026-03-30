"""Backwards-compatibility shim — use legalize.fetcher.fr.client instead."""

from legalize.fetcher.fr.client import *  # noqa: F401,F403
from legalize.fetcher.fr.client import LEGIClient, _id_to_subpath  # noqa: F811

__all__ = ["LEGIClient", "_id_to_subpath"]
