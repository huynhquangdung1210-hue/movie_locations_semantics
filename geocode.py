from typing import Optional, Tuple
import time
import requests

def geocode_nominatim(
    query: str,
    user_agent: str,
    sleep_s: float = 1.0,
    timeout_s: int = 30,
) -> Optional[Tuple[float, float]]:
    """
    Basic geocoder. Use only if you enable geocoding and cache results.
    """
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": query, "format": "json", "limit": 1}
    headers = {"User-Agent": user_agent}
    r = requests.get(url, params=params, headers=headers, timeout=timeout_s)
    if r.status_code != 200:
        return None
    js = r.json()
    if not js:
        return None
    lat = float(js[0]["lat"])
    lon = float(js[0]["lon"])
    time.sleep(sleep_s)
    return lat, lon
