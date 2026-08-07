"""Cloudflare D1 support for the Harbor Capital Comp Database.

Provides a minimal PEP-249 DBAPI over the D1 HTTP API plus a SQLAlchemy
dialect that reuses SQLAlchemy's existing SQLite dialect for SQL
compilation.  This lets the rest of the application keep using the
ORM (``Session``, ``.query()``) unchanged while the data actually lives
in Cloudflare D1.
"""

from .dialect import register_d1_dialect  # noqa: F401

__all__ = ["register_d1_dialect"]
