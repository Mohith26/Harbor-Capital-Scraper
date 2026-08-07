"""SQLAlchemy dialect that routes SQLite SQL at Cloudflare D1.

Registered as ``sqlite+d1://``.  It inherits every bit of SQL compilation from
SQLAlchemy's stock SQLite dialect and only swaps out the driver and the
transaction handling.
"""

from __future__ import annotations

from sqlalchemy.dialects import registry
from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite

from . import dbapi as d1_dbapi


class SQLiteDialect_d1(SQLiteDialect_pysqlite):
    """Talks to D1 over HTTP instead of a local SQLite file."""

    driver = "d1"
    supports_statement_cache = True

    # D1 caps bound parameters at 100 per statement, so SQLAlchemy must not
    # fold many rows into a single multi-row INSERT.
    supports_multivalues_insert = False
    use_insertmanyvalues = False
    use_insertmanyvalues_wo_returning = False

    # Every statement autocommits; there is no connection-level transaction.
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

    @classmethod
    def import_dbapi(cls):
        return d1_dbapi

    # SQLAlchemy 1.4 compatibility
    @classmethod
    def dbapi(cls):
        return d1_dbapi

    def create_connect_args(self, url):
        """Build connect() kwargs from the URL and/or the environment.

        Accepts ``sqlite+d1://`` (everything from the environment) or
        ``sqlite+d1://<account_id>/<database_id>``.
        """
        kwargs = {}
        if url.host:
            kwargs["account_id"] = url.host
        database = (url.database or "").strip("/")
        if database:
            kwargs["database_id"] = database
        kwargs.update(url.query)
        return ([], kwargs)

    # -- transaction handling -------------------------------------------------
    # D1's HTTP API applies each request immediately, so emitting BEGIN/COMMIT
    # would just produce errors.  Suppress them.

    def do_begin(self, dbapi_connection):
        pass

    def do_commit(self, dbapi_connection):
        pass

    def do_rollback(self, dbapi_connection):
        pass

    def do_begin_twophase(self, connection, xid):
        raise NotImplementedError("D1 does not support two-phase commit")

    def on_connect(self):
        # The stock pysqlite dialect registers regexp/isolation hooks that
        # assume a real sqlite3 connection object.
        return None

    # -- isolation level ------------------------------------------------------
    # D1 rejects `PRAGMA read_uncommitted` with SQLITE_AUTH, which the stock
    # SQLite dialect issues while initializing a connection.  D1 is always
    # serializable, so answer without touching the wire.

    def get_isolation_level(self, dbapi_connection):
        return "SERIALIZABLE"

    def get_default_isolation_level(self, dbapi_connection):
        return "SERIALIZABLE"

    def set_isolation_level(self, dbapi_connection, level):
        pass

    def get_isolation_level_values(self, dbapi_connection):
        return ["SERIALIZABLE"]

    def _get_server_version_info(self, connection):
        return d1_dbapi.sqlite_version_info

    def is_disconnect(self, e, connection, cursor):
        return isinstance(e, d1_dbapi.OperationalError) and "could not reach D1" in str(e)


def register_d1_dialect() -> None:
    """Make ``sqlite+d1://`` resolvable by :func:`sqlalchemy.create_engine`."""
    registry.register("sqlite.d1", "d1.dialect", "SQLiteDialect_d1")


# Register on import so simply importing the package is enough.
register_d1_dialect()

dialect = SQLiteDialect_d1
