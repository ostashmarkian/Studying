"""sigmaelectronica.net -> PostgreSQL (product_avnet), see schema.sql.

psycopg runs synchronously in a worker thread (asyncio.to_thread), as in the
avnet scraper: its async mode wants a SelectorEventLoop on Windows, and there
is no reason to tie the scraper's event loop to the database driver.
"""

import asyncio
import hashlib
import json
import logging
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DATABASE_CONNECTION = {
    "dbname": "product_avnet",
    "user": "postgres",
    "password": "spectr",
    "host": "localhost",
    "port": 5432,
}

SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# A child of the scraper's logger, so it lands in the same log file.
log = logging.getLogger("sigmaelectronica.db")

# Everything psycopg raises derives from this, so the scraper can catch database
# failures without importing the driver itself.
DatabaseError = psycopg.Error

# Problems that belong to one row rather than to the connection: a value the
# column refuses, a constraint. Only these are worth replaying row by row.
ROW_ERRORS = (psycopg.DataError, psycopg.IntegrityError)

# A complete run that would remove more than this share of the active table
# looks like a glitch on the shop's side (a plugin hiding products, a half-built
# cache) rather than a real clear-out, so the removal is refused.
MAX_GONE_SHARE = 0.2

COLUMNS = (
    "product_id",
    "sku",
    "name",
    "url",
    "manufacturer",
    "categories",
    "images",
    "price",
    "regular_price",
    "sale_price",
    "currency",
    "on_sale",
    "in_stock",
    "stock_qty",
    "short_description",
    "description",
    "data_hash",
)

# product_id is the key; sku and url are how anyone finds the product again.
REQUIRED = ("product_id", "sku", "url")

UPSERT_SQL = """
    INSERT INTO sigma_products ({columns})
    VALUES ({placeholders})
    ON CONFLICT (product_id) DO UPDATE SET
        {assignments},
        updated_at = CASE
            WHEN sigma_products.data_hash IS DISTINCT FROM EXCLUDED.data_hash
            THEN now() ELSE sigma_products.updated_at
        END,
        last_seen_at = now()
""".format(
    columns=", ".join(COLUMNS),
    placeholders=", ".join(["%s"] * len(COLUMNS)),
    assignments=",\n        ".join(
        f"{column} = EXCLUDED.{column}" for column in COLUMNS if column != "product_id"
    ),
)

HISTORY_COLUMNS = (
    "product_id",
    "event",
    "sku",
    "price",
    "regular_price",
    "sale_price",
    "on_sale",
    "in_stock",
    "stock_qty",
    "snapshot",
    "data_hash",
)

HISTORY_SQL = "INSERT INTO sigma_history ({columns}) VALUES ({placeholders})".format(
    columns=", ".join(HISTORY_COLUMNS),
    placeholders=", ".join(["%s"] * len(HISTORY_COLUMNS)),
)


def _text(value):
    if value is None:
        return None
    # PostgreSQL text cannot hold NUL, and one such byte would fail the page
    value = str(value).replace("\x00", "").strip()
    return value or None


def _int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _num(value):
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _texts(values):
    return [text for text in (_text(value) for value in values or []) if text]


def _plain(value):
    """A value jsonb can hold: whole Decimals as int, the rest as float."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


def _hash(values):
    """Fingerprint of exactly the row we store, not of the payload we received."""
    payload = {
        column: _plain(value) for column, value in values.items() if column != "data_hash"
    }
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).digest()


def build_row(product):
    """One scraped product -> a dict shaped like the sigma_products columns."""
    values = {
        "product_id": _int(product.get("id")),
        "sku": _text(product.get("sku")),
        "name": _text(product.get("name")),
        "url": _text(product.get("url")),
        "manufacturer": _text(product.get("manufacturer")),
        "categories": _texts(product.get("categories")),
        "images": _texts(product.get("images")),
        "price": _num(product.get("price")),
        "regular_price": _num(product.get("regular_price")),
        "sale_price": _num(product.get("sale_price")),
        "currency": _text(product.get("currency")),
        "on_sale": bool(product.get("on_sale")),
        "in_stock": bool(product.get("in_stock")),
        "stock_qty": _int(product.get("stock_qty")),
        "short_description": _text(product.get("short_description")),
        "description": _text(product.get("description")),
    }
    values["data_hash"] = _hash(values)
    return values


def _history_tuple(row, event):
    snapshot = {
        column: _plain(row[column]) for column in COLUMNS if column != "data_hash"
    }
    return (
        row["product_id"],
        event,
        row["sku"],
        row["price"],
        row["regular_price"],
        row["sale_price"],
        row["on_sale"],
        row["in_stock"],
        row["stock_qty"],
        Jsonb(snapshot),
        bytes(row["data_hash"]),
    )


def _reason(error):
    """Why a row was refused, on one line.

    The class name comes from SQLSTATE (NumericValueOutOfRange, NotNullViolation,
    ...) and stays the same whatever language the server speaks - this one
    answers in Ukrainian (lc_messages), which is fine to read but not to grep.
    """
    diag = error.diag
    reason = type(error).__name__
    if diag.sqlstate:
        reason += f" [{diag.sqlstate}]"
    message = (diag.message_primary or str(error)).strip()
    if message:
        reason += f": {message.splitlines()[0]}"
    for label, value in (
        ("column", diag.column_name),
        ("constraint", diag.constraint_name),
    ):
        if value:
            reason += f", {label} {value}"
    return reason


def _init_db_sync():
    with psycopg.connect(**DATABASE_CONNECTION) as connection:
        connection.execute(SCHEMA_PATH.read_text(encoding="utf-8"))


async def init_db():
    """Create the tables if they are missing. Idempotent, safe on every run."""
    await asyncio.to_thread(_init_db_sync)


def _write(cursor, rows):
    """Upsert rows and append a history version for every one that changed.

    The previous hashes are read first, inside the same transaction: after the
    upsert there is nothing left to compare against.
    """
    ids = [row["product_id"] for row in rows]
    previous = dict(
        cursor.execute(
            "SELECT product_id, data_hash FROM sigma_products"
            " WHERE product_id = ANY(%s)",
            (ids,),
        ).fetchall()
    )
    absent = [i for i in ids if i not in previous]
    # an id that is absent now but has a past was removed once and is back
    known = (
        {
            row[0]
            for row in cursor.execute(
                "SELECT DISTINCT product_id FROM sigma_history"
                " WHERE product_id = ANY(%s)",
                (absent,),
            )
        }
        if absent
        else set()
    )

    counts, history = Counter(), []
    for row in rows:
        old = previous.get(row["product_id"])
        if old is None:
            event = "returned" if row["product_id"] in known else "new"
        elif bytes(old) != row["data_hash"]:
            event = "changed"
        else:
            counts["unchanged"] += 1
            continue
        counts[event] += 1
        history.append(_history_tuple(row, event))

    cursor.executemany(UPSERT_SQL, [tuple(row[c] for c in COLUMNS) for row in rows])
    if history:
        cursor.executemany(HISTORY_SQL, history)
    counts["written"] += len(rows)
    counts["history"] += len(history)
    return counts


def _save_products_sync(products):
    rows, skipped = [], []
    for product in products:
        row = build_row(product)
        missing = [column for column in REQUIRED if row[column] is None]
        if missing:
            skipped.append((product.get("id"), f"missing {', '.join(missing)}"))
            continue
        rows.append(row)
    # the same id twice would be counted as a change against itself
    rows = list({row["product_id"]: row for row in rows}.values())
    if not rows:
        return Counter(), skipped

    with psycopg.connect(**DATABASE_CONNECTION, autocommit=True) as connection:
        with connection.cursor() as cursor:
            try:
                with connection.transaction():
                    return _write(cursor, rows), skipped
            except ROW_ERRORS as error:
                # One bad value must not cost the rest of the page: replay the
                # rows one at a time, each in its own transaction, and lose only
                # the offenders - with the reason the server gave.
                log.warning(
                    "batch of %d refused (%s), replaying row by row",
                    len(rows),
                    _reason(error),
                )
                counts = Counter()
                for row in rows:
                    try:
                        with connection.transaction():
                            counts += _write(cursor, [row])
                    except ROW_ERRORS as row_error:
                        skipped.append((row["product_id"], _reason(row_error)))
                return counts, skipped


async def save_products(products):
    """Write one page of scraped products.

    Returns (counts, skipped). counts: written (every row that reached the table,
    unchanged ones included - their last_seen_at moves), new, changed, returned,
    unchanged, history. skipped: [(product_id, reason)] for products that were
    scraped but not written. A lost connection raises DatabaseError instead.
    """
    if not products:
        return Counter(), []
    return await asyncio.to_thread(_save_products_sync, products)


def _remove_gone_sync(seen_ids):
    if not seen_ids:
        return 0, "no products were seen"
    with psycopg.connect(**DATABASE_CONNECTION, autocommit=True) as connection:
        with connection.transaction():
            with connection.cursor(row_factory=dict_row) as cursor:
                active = cursor.execute(
                    "SELECT count(*) AS n FROM sigma_products"
                ).fetchone()["n"]
                gone = cursor.execute(
                    "SELECT count(*) AS n FROM sigma_products"
                    " WHERE product_id <> ALL(%s)",
                    (list(seen_ids),),
                ).fetchone()["n"]
                if not gone:
                    return 0, None
                if gone > active * MAX_GONE_SHARE:
                    return 0, (
                        f"would remove {gone} of {active} active products,"
                        " looks like a glitch on the shop's side"
                    )
                removed = cursor.execute(
                    f"DELETE FROM sigma_products WHERE product_id <> ALL(%s)"
                    f" RETURNING {', '.join(COLUMNS)}",
                    (list(seen_ids),),
                ).fetchall()
                cursor.executemany(
                    HISTORY_SQL, [_history_tuple(row, "gone") for row in removed]
                )
    return len(removed), None


async def remove_gone(seen_ids):
    """Move products the shop no longer lists from sigma_products to history.

    Call it only after a COMPLETE run: an incomplete one would "remove"
    everything it did not reach. Returns (removed, refused_reason).
    """
    return await asyncio.to_thread(_remove_gone_sync, seen_ids)


def _table_stats_sync():
    with psycopg.connect(**DATABASE_CONNECTION) as connection:
        return connection.execute(
            "SELECT (SELECT count(*) FROM sigma_products),"
            "       (SELECT count(*) FROM sigma_history)"
        ).fetchone()


async def table_stats():
    """(rows in sigma_products, rows in sigma_history)."""
    return await asyncio.to_thread(_table_stats_sync)
