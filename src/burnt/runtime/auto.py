"""Auto-detection of execution context and backend selection."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from .rest_backend import RestBackend

if TYPE_CHECKING:
    from .backend import Backend


def auto_backend() -> Backend | None:
    """Auto-detect execution context and return appropriate backend.

    Detection order:
    1. In-cluster (Databricks Runtime) - checks DATABRICKS_RUNTIME_VERSION
    2. External with credentials - checks DATABRICKS_HOST + auth credentials
    3. Offline mode - returns None (static estimation only)

    Environment variables used:
    - DATABRICKS_RUNTIME_VERSION: Set when running inside Databricks
    - DATABRICKS_HOST: Workspace URL for external access
    - DATABRICKS_TOKEN: PAT token (legacy, not recommended)
    - DATABRICKS_CLIENT_ID: OAuth client ID for service principals
    - DATABRICKS_CLIENT_SECRET: OAuth client secret for service principals

    Returns:
        Backend instance if execution context detected, None for offline mode
    """
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return _create_spark_backend()

    if os.environ.get("DATABRICKS_HOST"):
        return _create_rest_backend()

    return None


def _create_spark_backend() -> Backend:
    """Create SparkBackend from active SparkSession."""
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "No active SparkSession found. "
            "Ensure you are running inside a Databricks notebook or "
            "have created a SparkSession."
        )

    from .spark_backend import SparkBackend

    return SparkBackend(spark)


def _create_rest_backend() -> Backend:
    """Create RestBackend using Databricks SDK with unified auth."""
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    return RestBackend(workspace_client=client)


def current_notebook_path() -> str | None:
    """Get the current notebook or script path.

    Detection order:
    1. SparkConf: spark.databricks.notebook.path (most reliable in DBR)
    2. dbutils: Notebook context (interactive notebooks)
    3. ipynbname: Local Jupyter notebooks
    4. inspect.stack(): Python scripts

    Returns:
        Path to current notebook/script, or None if undetectable
    """
    path = _get_spark_notebook_path()
    if path:
        return path

    path = _get_dbutils_notebook_path()
    if path:
        return path

    path = _get_ipynbname_path()
    if path:
        return path

    return _get_script_path()


def _get_spark_notebook_path() -> str | None:
    """Get path from SparkConf."""
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            path = spark.conf.get("spark.databricks.notebook.path", None)
            if path:
                return path
    except ImportError:
        pass

    return None


def _get_dbutils_notebook_path() -> str | None:
    """Get path from dbutils context."""
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            dbutils = DBUtils(spark)
            return dbutils.notebook.getContext().notebookPath().get()
    except ImportError:
        pass
    except Exception:
        pass

    return None


def _get_ipynbname_path() -> str | None:
    """Get path from ipynbname (local Jupyter)."""
    try:
        import ipynbname

        return ipynbname.path()
    except ImportError:
        pass
    except Exception:
        pass

    return None


def _get_script_path() -> str | None:
    """Get script path using inspect."""
    import inspect

    for frame_info in inspect.stack():
        filename = frame_info.filename
        if filename and filename != "<stdin>" and filename.endswith(".py"):
            return os.path.abspath(filename)

    return None
