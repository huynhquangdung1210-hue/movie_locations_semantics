import sqlite3
from typing import Any, List, Optional, Tuple
import json
import time

def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    _init(con)
    return con

def _init(con: sqlite3.Connection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS wikidata_locations (
      tconst TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      fetched_at INTEGER NOT NULL,
      PRIMARY KEY (tconst)
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS rapidapi_locations (
      tconst TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      fetched_at INTEGER NOT NULL,
      PRIMARY KEY (tconst)
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS geocode_cache (
      query TEXT PRIMARY KEY,
      lat REAL,
      lon REAL,
      provider TEXT NOT NULL,
      fetched_at INTEGER NOT NULL
    )
    """)
    con.commit()

def get_wikidata(con: sqlite3.Connection, tconst: str) -> Optional[list]:
    row = con.execute(
        "SELECT payload_json FROM wikidata_locations WHERE tconst=?",
        (tconst,)
    ).fetchone()
    if not row:
        return None
    return json.loads(row[0])

def set_wikidata(con: sqlite3.Connection, tconst: str, rows: list) -> None:
    con.execute(
        "INSERT OR REPLACE INTO wikidata_locations (tconst, payload_json, fetched_at) VALUES (?, ?, ?)",
        (tconst, json.dumps(rows, ensure_ascii=False), int(time.time()))
    )

def get_rapidapi_locations(con: sqlite3.Connection, tconst: str) -> Optional[List[dict]]:
    row = con.execute(
        "SELECT payload_json FROM rapidapi_locations WHERE tconst=?",
        (tconst,)
    ).fetchone()
    if not row:
        return None
    return json.loads(row[0])

def set_rapidapi_locations(con: sqlite3.Connection, tconst: str, rows: List[dict]) -> None:
    con.execute(
        "INSERT OR REPLACE INTO rapidapi_locations (tconst, payload_json, fetched_at) VALUES (?, ?, ?)",
        (tconst, json.dumps(rows, ensure_ascii=False), int(time.time()))
    )

def get_geocode(con: sqlite3.Connection, query: str) -> Optional[Tuple[float, float]]:
    row = con.execute(
        "SELECT lat, lon FROM geocode_cache WHERE query=?",
        (query,)
    ).fetchone()
    if not row or row[0] is None or row[1] is None:
        return None
    return float(row[0]), float(row[1])

def set_geocode(con: sqlite3.Connection, query: str, lat: float, lon: float, provider: str) -> None:
    import time
    con.execute(
        "INSERT OR REPLACE INTO geocode_cache (query, lat, lon, provider, fetched_at) VALUES (?, ?, ?, ?, ?)",
        (query, float(lat), float(lon), provider, int(time.time()))
    )
