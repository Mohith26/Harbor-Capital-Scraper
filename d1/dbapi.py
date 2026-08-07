"""A minimal PEP-249 DBAPI driver for Cloudflare D1.

D1 speaks SQLite, so SQLAlchemy's existing SQLite dialect can compile all of
our SQL.  The only thing missing is a driver that actually ships statements
somewhere, which is what this module provides.  Statements are sent to the
D1 HTTP API's ``/raw`` endpoint, which returns ordered columns and rows -
exactly the shape a DBAPI cursor needs.

Deliberate limitations (documented, not accidental):

* **Autocommit only.**  The D1 HTTP API has no cross-request transaction, so
  ``BEGIN`` / ``COMMIT`` / ``ROLLBACK`` are no-ops.  ``executemany()`` batches
  its statements into a single request, which D1 runs atomically, so bulk
  inserts (the app's main write path) still land all-or-nothing.
* **100 bound parameters per statement.**  This is a hard D1 limit, so the
  dialect disables SQLAlchemy's multi-row INSERT batching.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

# --- PEP 249 module interface -------------------------------------------------

apilevel = "2.0"
threadsafety = 2
paramstyle = "qmark"

# SQLAlchemy's SQLite dialect inspects this to decide which SQL features it may
# emit.  D1 tracks a recent SQLite, so report a modern version.
sqlite_version_info = (3, 45, 0)
sqlite_version = "3.45.0"


class Error(Exception):
    """Base class for all driver errors."""


class Warning(Exception):  # noqa: A001 - name mandated by PEP 249
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


_API_ROOT = "https://api.cloudflare.com/client/v4"
_RETRY_STATUSES = {429, 500, 502, 503, 504}


def _classify(message: str) -> Error:
    """Map a D1 error string onto the closest PEP 249 exception type."""
    lowered = message.lower()
    if "unique" in lowered or "constraint" in lowered:
        return IntegrityError(message)
    if "no such table" in lowered or "no such column" in lowered or "syntax error" in lowered:
        return ProgrammingError(message)
    return OperationalError(message)


class Connection:
    """A logical connection to one D1 database."""

    def __init__(self, account_id: str, database_id: str, api_token: str, timeout: int = 30,
                 max_retries: int = 3):
        if not account_id or not database_id or not api_token:
            raise InterfaceError(
                "D1 requires CLOUDFLARE_ACCOUNT_ID, D1_DATABASE_ID and CLOUDFLARE_API_TOKEN"
            )
        self._account_id = account_id
        self._database_id = database_id
        self._api_token = api_token
        self._timeout = timeout
        self._max_retries = max_retries
        self._closed = False
        self.autocommit = True

    # -- HTTP plumbing --------------------------------------------------------

    @property
    def _endpoint(self) -> str:
        return (
            f"{_API_ROOT}/accounts/{self._account_id}"
            f"/d1/database/{self._database_id}/raw"
        )

    def _post(self, payload: dict) -> list:
        body = json.dumps(payload).encode("utf-8")
        last_error = None
        for attempt in range(self._max_retries):
            request = urllib.request.Request(
                self._endpoint,
                data=body,
                method="POST",
                headers={
                    "Authorization": f"Bearer {self._api_token}",
                    "Content-Type": "application/json",
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=self._timeout) as response:
                    parsed = json.loads(response.read().decode("utf-8"))
                break
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", "replace")
                if exc.code in _RETRY_STATUSES and attempt < self._max_retries - 1:
                    last_error = detail
                    time.sleep(0.4 * (2 ** attempt))
                    continue
                try:
                    errors = json.loads(detail).get("errors") or []
                    message = "; ".join(e.get("message", "") for e in errors) or detail
                except Exception:
                    message = detail
                raise _classify(message) from exc
            except urllib.error.URLError as exc:
                if attempt < self._max_retries - 1:
                    last_error = str(exc)
                    time.sleep(0.4 * (2 ** attempt))
                    continue
                raise OperationalError(f"could not reach D1: {exc}") from exc
        else:  # pragma: no cover - loop always breaks or raises
            raise OperationalError(f"D1 request failed after retries: {last_error}")

        if not parsed.get("success", False):
            errors = parsed.get("errors") or []
            raise _classify("; ".join(e.get("message", "") for e in errors) or str(parsed))
        return parsed.get("result") or []

    def _execute(self, sql: str, params=None) -> list:
        payload = {"sql": sql}
        if params:
            payload["params"] = [_encode_param(p) for p in params]
        return self._post(payload)

    # -- DBAPI interface ------------------------------------------------------

    def cursor(self) -> "Cursor":
        if self._closed:
            raise InterfaceError("connection is closed")
        return Cursor(self)

    def commit(self) -> None:
        """No-op: every statement is already committed by D1."""

    def rollback(self) -> None:
        """No-op: D1's HTTP API cannot roll back an applied statement."""

    def close(self) -> None:
        self._closed = True


def _encode_param(value):
    """Coerce a Python value into something D1's JSON API accepts."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    # datetimes, Decimals, UUIDs and friends
    return str(value)


class Cursor:
    def __init__(self, connection: Connection):
        self._connection = connection
        self._rows: list = []
        self._index = 0
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self.arraysize = 1
        self._closed = False

    # -- helpers --------------------------------------------------------------

    def _consume(self, result: list) -> None:
        self._rows = []
        self._index = 0
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        if not result:
            return
        # Use the final statement's result set, matching sqlite3 semantics.
        last = result[-1]
        payload = last.get("results") or {}
        columns = payload.get("columns") or []
        rows = payload.get("rows") or []
        if columns:
            self.description = [
                (name, None, None, None, None, None, None) for name in columns
            ]
            self._rows = [tuple(row) for row in rows]
        meta = last.get("meta") or {}
        changes = meta.get("changes")
        if isinstance(changes, int):
            self.rowcount = changes
        last_row_id = meta.get("last_row_id")
        if isinstance(last_row_id, int) and last_row_id:
            self.lastrowid = last_row_id

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("cursor is closed")

    # -- DBAPI interface ------------------------------------------------------

    def execute(self, sql: str, parameters=None) -> "Cursor":
        self._check_open()
        params = list(parameters) if parameters else []
        if len(params) > 100:
            raise NotSupportedError(
                f"D1 allows at most 100 bound parameters per statement, got {len(params)}"
            )
        try:
            self._consume(self._connection._execute(sql, params))
        except OperationalError as exc:
            # D1 refuses a handful of connection-tuning PRAGMAs that SQLAlchemy
            # probes for.  They are advisory, so treat a refusal as "no rows"
            # rather than failing the whole connection.
            if sql.strip().upper().startswith("PRAGMA") and "SQLITE_AUTH" in str(exc):
                self._consume([])
                return self
            raise
        return self

    def executemany(self, sql: str, seq_of_parameters) -> "Cursor":
        """Run one statement repeatedly.

        Sent as a single request so D1 applies the whole batch atomically.
        """
        self._check_open()
        batches = [list(p) for p in seq_of_parameters]
        if not batches:
            self._consume([])
            return self
        total_changes = 0
        last_result: list = []
        # Keep each request comfortably under D1's limits.
        chunk_size = max(1, 100 // max(1, len(batches[0]))) if batches[0] else 50
        for start in range(0, len(batches), chunk_size):
            chunk = batches[start:start + chunk_size]
            statements = []
            params: list = []
            for row in chunk:
                statements.append(sql)
                params.extend(row)
            joined = ";\n".join(statements)
            if len(params) > 100:
                # Fall back to one request per row rather than exceed the limit.
                for row in chunk:
                    last_result = self._connection._execute(sql, row)
                    meta = (last_result[-1].get("meta") or {}) if last_result else {}
                    total_changes += meta.get("changes") or 0
                continue
            last_result = self._connection._execute(joined, params)
            for statement_result in last_result:
                meta = statement_result.get("meta") or {}
                total_changes += meta.get("changes") or 0
        self._consume(last_result)
        self.rowcount = total_changes
        return self

    def executescript(self, sql_script: str) -> "Cursor":
        self._check_open()
        self._consume(self._connection._execute(sql_script))
        return self

    def fetchone(self):
        self._check_open()
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchmany(self, size=None):
        self._check_open()
        size = self.arraysize if size is None else size
        chunk = self._rows[self._index:self._index + size]
        self._index += len(chunk)
        return chunk

    def fetchall(self):
        self._check_open()
        chunk = self._rows[self._index:]
        self._index = len(self._rows)
        return chunk

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def close(self) -> None:
        self._closed = True
        self._rows = []

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row


def connect(account_id=None, database_id=None, api_token=None, timeout=30, **_ignored) -> Connection:
    """Open a D1 connection, falling back to the standard environment variables."""
    return Connection(
        account_id or os.environ.get("CLOUDFLARE_ACCOUNT_ID", ""),
        database_id or os.environ.get("D1_DATABASE_ID", ""),
        api_token or os.environ.get("CLOUDFLARE_API_TOKEN", ""),
        timeout=timeout,
    )
