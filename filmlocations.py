# path: imdb_locations_rapidapi.py
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


IMDB_COM_RAPIDAPI_HOST = "imdb-com.p.rapidapi.com"
IMDB_COM_RAPIDAPI_BASE = f"https://{IMDB_COM_RAPIDAPI_HOST}"


def _chunks(seq: Sequence[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield list(seq[i : i + n])


def _build_session(
    *,
    total_retries: int = 6,
    backoff_factor: float = 0.6,
    status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
    timeout_s: float = 60.0,
) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Attach default timeout via request wrapper
    old_request = session.request

    def request_with_timeout(method: str, url: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", timeout_s)
        return old_request(method, url, **kwargs)

    session.request = request_with_timeout  # type: ignore[assignment]
    return session


def _cache_path(cache_dir: Path, tconst: str) -> Path:
    safe = tconst.replace("/", "_")
    return cache_dir / f"{safe}.json"


def _load_cached_json(cache_dir: Optional[Path], tconst: str) -> Optional[Any]:
    if not cache_dir:
        return None
    p = _cache_path(cache_dir, tconst)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_cached_json(cache_dir: Optional[Path], tconst: str, payload: Any) -> None:
    if not cache_dir:
        return
    cache_dir.mkdir(parents=True, exist_ok=True)
    p = _cache_path(cache_dir, tconst)
    try:
        p.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _extract_location_label(item: Any) -> Optional[str]:
    """
    Best-effort extraction across common shapes:
    - "location", "locationName", "text", "label", etc.
    - nested dicts
    """
    if isinstance(item, str):
        s = item.strip()
        return s or None

    if not isinstance(item, dict):
        return None

    for key in ("location", "locationName", "name", "label", "text", "place", "value"):
        v = item.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # Sometimes nested like {"location": {"text": "..."}}
    for key in ("location", "place"):
        v = item.get(key)
        if isinstance(v, dict):
            for subkey in ("text", "label", "name", "value"):
                sv = v.get(subkey)
                if isinstance(sv, str) and sv.strip():
                    return sv.strip()

    return None


def _parse_filming_locations(payload: Any) -> List[str]:
    """
    Returns list of location labels (strings).
    Handles payload shapes like:
    - list[...]
    - {"locations": [...]}
    - {"data": {"locations": [...]}}, {"results": [...]}, etc.
    """
    candidates: Any = None

    if isinstance(payload, list):
        candidates = payload
    elif isinstance(payload, dict):
        # common containers
        for k in ("locations", "filmingLocations", "results", "data"):
            if k in payload:
                candidates = payload[k]
                break

        # unwrap nested "data"
        if isinstance(candidates, dict):
            for k in ("locations", "filmingLocations", "results"):
                if k in candidates:
                    candidates = candidates[k]
                    break

    if not isinstance(candidates, list):
        return []

    out: List[str] = []
    for it in candidates:
        label = _extract_location_label(it)
        if label:
            out.append(label)

    # dedupe while preserving order
    seen = set()
    deduped = []
    for s in out:
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(s)
    return deduped


def imdb_filming_locations_via_rapidapi(
    tconsts: Iterable[str],
    *,
    rapidapi_key: str,
    rapidapi_host: str = IMDB_COM_RAPIDAPI_HOST,
    batch_size: int = 200,
    sleep_s: float = 0.2,
    cache_dir: Optional[str] = None,
    user_agent: str = "imdb-locations/1.0 (contact: you@example.com)",
) -> pd.DataFrame:
    """
    Fetch filming locations for IMDb titles using RapidAPI 'imdb-com' endpoint.

    Endpoint:
      /title/get-filming-locations?tconst=tt...

    Notes:
    - This fetches FILMING locations only.
    - Most providers return location strings, not coordinates, so lat/lon stay None.
    """
    tlist = [t for t in tconsts if isinstance(t, str) and t.startswith("tt")]
    cache_path = Path(cache_dir) if cache_dir else None

    session = _build_session()
    headers = {
        "X-RapidAPI-Key": rapidapi_key,
        "X-RapidAPI-Host": rapidapi_host,
        "User-Agent": user_agent,
        "Accept": "application/json",
    }

    # Allow caller to pass either host ("imdb-com.p.rapidapi.com") or full base URL
    base_url = IMDB_COM_RAPIDAPI_BASE
    if rapidapi_host.startswith("http://") or rapidapi_host.startswith("https://"):
        base_url = rapidapi_host.rstrip("/")
        headers["X-RapidAPI-Host"] = rapidapi_host.split("//", 1)[-1].split("/", 1)[0]
    else:
        base_url = f"https://{rapidapi_host}".rstrip("/")

    rows: List[Dict[str, Any]] = []

    for batch in _chunks(tlist, batch_size):
        for tconst in batch:
            cached = _load_cached_json(cache_path, tconst)
            payload = cached
            if payload is None:
                url = f"{base_url}/title/get-filming-locations"
                resp = session.get(url, params={"tconst": tconst}, headers=headers)

                # If provider returns HTML on errors, protect json parsing
                try:
                    payload = resp.json()
                except Exception:
                    payload = None

                # Cache even if empty to avoid re-hitting bad IDs endlessly
                _save_cached_json(cache_path, tconst, payload)

                # Light throttle regardless of status; Retry handles 429/5xx too.
                time.sleep(sleep_s)

            if not payload:
                continue

            locs = _parse_filming_locations(payload)
            for loc_label in locs:
                rows.append(
                    {
                        "tconst": tconst,
                        "location_kind": "filming",
                        "location_item": None,
                        "location_label": loc_label,
                        "lat": None,
                        "lon": None,
                        "is_fictional": False,
                    }
                )

    return pd.DataFrame(
        rows,
        columns=[
            "tconst",
            "location_kind",
            "location_item",
            "location_label",
            "lat",
            "lon",
            "is_fictional",
        ],
    )
