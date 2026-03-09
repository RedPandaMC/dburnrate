"""Runtime backends for executing queries in different contexts."""

from __future__ import annotations

from .auto import auto_backend, current_notebook_path
from .backend import Backend
from .rest_backend import RestBackend
from .spark_backend import SparkBackend

__all__ = [
    "Backend",
    "RestBackend",
    "SparkBackend",
    "auto_backend",
    "current_notebook_path",
]
