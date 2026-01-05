import time
from typing import Dict, List, Iterable
import requests

def chunked(lst: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_locations_batch(
    sparql_url: str,
    tconsts: List[str],
    user_agent: str,
    sleep_s: float = 1.0,
    timeout_s: int = 90,
) -> Dict[str, List[dict]]:
    """
    Returns { tconst: [ {location_kind, location_item, location_label, lat, lon}, ... ] }
    location_kind ∈ {'filming','featured'}
    Coordinates are taken from location item's P625 when present.
    """
    values = " ".join(f'"{t}"' for t in tconsts)
    query = f"""
    SELECT ?tconst ?kind ?loc ?locLabel ?coord WHERE {{
      VALUES ?tconst {{ {values} }}
      ?film wdt:P345 ?tconst .

      {{
        ?film wdt:P915 ?loc .
        BIND("filming" AS ?kind)
      }}
      UNION
      {{
        ?film wdt:P840 ?loc .
        BIND("featured" AS ?kind)
      }}

      OPTIONAL {{ ?loc wdt:P625 ?coord . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """

    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": user_agent,
    }

    r = requests.get(sparql_url, params={"query": query}, headers=headers, timeout=timeout_s)
    r.raise_for_status()
    js = r.json()

    out: Dict[str, List[dict]] = {t: [] for t in tconsts}
    for b in js["results"]["bindings"]:
        t = b["tconst"]["value"]
        kind = b["kind"]["value"]  # filming/featured
        loc_item = b["loc"]["value"]
        loc_label = b.get("locLabel", {}).get("value")

        coord = b.get("coord", {}).get("value")  # "Point(lon lat)"
        lat = lon = None
        if coord and coord.startswith("Point(") and coord.endswith(")"):
            inside = coord[len("Point("):-1].strip()
            lon_s, lat_s = inside.split()
            lon, lat = float(lon_s), float(lat_s)

        out.setdefault(t, []).append({
            "tconst": t,
            "location_kind": kind,
            "location_item": loc_item,
            "location_label": loc_label,
            "lat": lat,
            "lon": lon,
        })

    time.sleep(sleep_s)
    return out
