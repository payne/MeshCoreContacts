#!/usr/bin/env python3
"""Import a MeshCore discovered-contacts JSON export into a SQLite database.

Usage:
    ./import_contacts.py <station-name> <contacts.json> [--db meshcore_contacts.db]

The station name identifies the node that discovered the contacts, so exports
from several stations can live in one database. Re-importing an export for the
same station updates the existing rows rather than duplicating them.
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone

DEFAULT_DB = "meshcore_contacts.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS stations (
    id               INTEGER PRIMARY KEY,
    name             TEXT    NOT NULL UNIQUE,
    first_import_utc TEXT    NOT NULL,
    last_import_utc  TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS contact_types (
    type INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS contacts (
    id               INTEGER PRIMARY KEY,
    station_id       INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    public_key       TEXT    NOT NULL,
    name             TEXT    NOT NULL,
    type             INTEGER NOT NULL,
    flags            INTEGER NOT NULL,
    latitude         REAL,
    longitude        REAL,
    last_advert      INTEGER,
    last_modified    INTEGER,
    advert_path_list TEXT,
    advert_path_hops INTEGER,
    first_seen_utc   TEXT    NOT NULL,
    last_seen_utc    TEXT    NOT NULL,
    UNIQUE (station_id, public_key)
);

CREATE INDEX IF NOT EXISTS idx_contacts_public_key ON contacts (public_key);
CREATE INDEX IF NOT EXISTS idx_contacts_name       ON contacts (name);

CREATE TABLE IF NOT EXISTS imports (
    id               INTEGER PRIMARY KEY,
    station_id       INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
    source_file      TEXT    NOT NULL,
    imported_utc     TEXT    NOT NULL,
    contacts_in_file INTEGER NOT NULL,
    inserted         INTEGER NOT NULL,
    updated          INTEGER NOT NULL,
    unchanged        INTEGER NOT NULL
);

CREATE VIEW IF NOT EXISTS v_contacts AS
SELECT s.name                                        AS station,
       c.name                                        AS contact,
       c.public_key,
       c.type,
       COALESCE(t.name, 'unknown')                   AS type_name,
       c.flags,
       c.latitude,
       c.longitude,
       c.advert_path_list,
       c.advert_path_hops,
       c.last_advert,
       datetime(c.last_advert, 'unixepoch')          AS last_advert_utc,
       c.last_modified,
       datetime(c.last_modified, 'unixepoch')         AS last_modified_utc,
       c.first_seen_utc,
       c.last_seen_utc
FROM contacts c
JOIN stations s ON s.id = c.station_id
LEFT JOIN contact_types t ON t.type = c.type;
"""

# MeshCore advertisement node types.
CONTACT_TYPES = {
    1: "companion",
    2: "repeater",
    3: "room server",
    4: "sensor",
}

# Columns compared to decide whether an existing contact row changed.
PAYLOAD_COLUMNS = (
    "name",
    "type",
    "flags",
    "latitude",
    "longitude",
    "last_advert",
    "last_modified",
    "advert_path_list",
    "advert_path_hops",
)


def parse_coordinate(value):
    """Return the coordinate as a float, or None when absent or exactly zero.

    MeshCore reports coordinates as strings and uses 0/0 for "no position".
    """
    if value is None or value == "":
        return None
    try:
        coord = float(value)
    except (TypeError, ValueError):
        return None
    return None if coord == 0 else coord


def path_hops(advert_path_list):
    """Hop count of a comma-separated advert path; 0 for a direct/empty path."""
    if not advert_path_list:
        return 0
    return len([hop for hop in advert_path_list.split(",") if hop.strip()])


def load_contacts(path):
    """Read the export and return its list of contact dicts."""
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)

    if isinstance(data, list):
        contacts = data
    elif isinstance(data, dict):
        contacts = data.get("discovered_contacts")
        if contacts is None:
            raise ValueError("no 'discovered_contacts' key in %s" % path)
    else:
        raise ValueError("unexpected JSON top-level type in %s" % path)

    if not isinstance(contacts, list):
        raise ValueError("'discovered_contacts' is not a list in %s" % path)
    return contacts


def to_row(contact):
    """Map one JSON contact onto the contacts table columns."""
    public_key = (contact.get("public_key") or "").strip().lower()
    if not public_key:
        raise ValueError("contact is missing a public_key: %r" % (contact,))

    advert_path = contact.get("advert_path_list") or None
    return {
        "public_key": public_key,
        "name": contact.get("name") or "",
        "type": int(contact.get("type") or 0),
        "flags": int(contact.get("flags") or 0),
        "latitude": parse_coordinate(contact.get("latitude")),
        "longitude": parse_coordinate(contact.get("longitude")),
        "last_advert": contact.get("last_advert"),
        "last_modified": contact.get("last_modified"),
        "advert_path_list": advert_path,
        "advert_path_hops": path_hops(advert_path),
    }


def connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    conn.executemany(
        "INSERT INTO contact_types (type, name) VALUES (?, ?) "
        "ON CONFLICT (type) DO UPDATE SET name = excluded.name",
        sorted(CONTACT_TYPES.items()),
    )
    return conn


def upsert_station(conn, station, now):
    conn.execute(
        "INSERT INTO stations (name, first_import_utc, last_import_utc) VALUES (?, ?, ?) "
        "ON CONFLICT (name) DO UPDATE SET last_import_utc = excluded.last_import_utc",
        (station, now, now),
    )
    return conn.execute("SELECT id FROM stations WHERE name = ?", (station,)).fetchone()["id"]


def import_contacts(conn, station, json_path, contacts, now):
    station_id = upsert_station(conn, station, now)

    existing = {
        row["public_key"]: row
        for row in conn.execute(
            "SELECT public_key, %s FROM contacts WHERE station_id = ?"
            % ", ".join(PAYLOAD_COLUMNS),
            (station_id,),
        )
    }

    inserted = updated = unchanged = 0
    seen = set()

    for contact in contacts:
        row = to_row(contact)
        key = row["public_key"]
        if key in seen:
            # Later duplicates within one file win; don't double-count them.
            conn.execute(
                "UPDATE contacts SET %s, last_seen_utc = :last_seen_utc "
                "WHERE station_id = :station_id AND public_key = :public_key"
                % ", ".join("%s = :%s" % (c, c) for c in PAYLOAD_COLUMNS),
                {**row, "station_id": station_id, "last_seen_utc": now},
            )
            continue
        seen.add(key)

        previous = existing.get(key)
        if previous is None:
            conn.execute(
                "INSERT INTO contacts (station_id, public_key, %s, first_seen_utc, last_seen_utc) "
                "VALUES (:station_id, :public_key, %s, :first_seen_utc, :last_seen_utc)"
                % (
                    ", ".join(PAYLOAD_COLUMNS),
                    ", ".join(":%s" % c for c in PAYLOAD_COLUMNS),
                ),
                {**row, "station_id": station_id, "first_seen_utc": now, "last_seen_utc": now},
            )
            inserted += 1
            continue

        changed = any(previous[c] != row[c] for c in PAYLOAD_COLUMNS)
        conn.execute(
            "UPDATE contacts SET %s, last_seen_utc = :last_seen_utc "
            "WHERE station_id = :station_id AND public_key = :public_key"
            % ", ".join("%s = :%s" % (c, c) for c in PAYLOAD_COLUMNS),
            {**row, "station_id": station_id, "last_seen_utc": now},
        )
        if changed:
            updated += 1
        else:
            unchanged += 1

    conn.execute(
        "INSERT INTO imports (station_id, source_file, imported_utc, contacts_in_file, "
        "inserted, updated, unchanged) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (station_id, os.path.abspath(json_path), now, len(contacts), inserted, updated, unchanged),
    )

    missing = conn.execute(
        "SELECT COUNT(*) AS n FROM contacts WHERE station_id = ? AND last_seen_utc <> ?",
        (station_id, now),
    ).fetchone()["n"]

    return {
        "station_id": station_id,
        "in_file": len(contacts),
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "not_in_file": missing,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Import a MeshCore discovered-contacts JSON export into SQLite."
    )
    parser.add_argument("station", help="name of the station that discovered the contacts")
    parser.add_argument("json_file", help="path to the discovered-contacts JSON export")
    parser.add_argument(
        "--db", default=DEFAULT_DB, help="SQLite database file (default: %(default)s)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="parse and report what would change, then roll back",
    )
    args = parser.parse_args(argv)

    station = args.station.strip()
    if not station:
        parser.error("station name must not be empty")

    try:
        contacts = load_contacts(args.json_file)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 1

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    db_existed = os.path.exists(args.db)
    conn = connect(args.db)
    try:
        with conn:
            stats = import_contacts(conn, station, args.json_file, contacts, now)
            if args.dry_run:
                # Raising inside the context manager rolls the transaction back.
                raise _DryRun(stats)
    except _DryRun as dry:
        stats = dry.stats
    except (sqlite3.Error, ValueError) as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 1
    finally:
        conn.close()
        if args.dry_run and not db_existed:
            # A dry run must not leave a database behind.
            try:
                os.unlink(args.db)
            except OSError:
                pass

    print(
        "%s%s: %d contacts in file -> %d new, %d updated, %d unchanged"
        % (
            "[dry run] " if args.dry_run else "",
            station,
            stats["in_file"],
            stats["inserted"],
            stats["updated"],
            stats["unchanged"],
        )
    )
    if stats["not_in_file"]:
        print(
            "  %d previously imported contact(s) for this station were absent from the file "
            "(left unchanged)" % stats["not_in_file"]
        )
    if not args.dry_run:
        print("  database: %s" % os.path.abspath(args.db))
    return 0


class _DryRun(Exception):
    def __init__(self, stats):
        super().__init__("dry run")
        self.stats = stats


if __name__ == "__main__":
    sys.exit(main())
