import re
from typing import Optional, Tuple

_FICTIONAL_HINTS = [
    "gotham", "metropolis", "hogwarts", "middle-earth", "westeros",
    "atlantis", "pandora", "springfield (fictional)", "tatooine",
]
_HINT_RE = re.compile("|".join(re.escape(x) for x in _FICTIONAL_HINTS), re.IGNORECASE)

def classify_location(label: Optional[str], lat: Optional[float], lon: Optional[float]) -> Tuple[str, bool]:
    """
    Returns (location_class, is_fictional)
      location_class ∈ {'real','fictional','unknown'}
    """
    if lat is not None and lon is not None:
        return "real", False

    if label:
        if _HINT_RE.search(label):
            return "fictional", True

        # If it's something like "Gotham City" without coords, often fictional
        if re.search(r"\b(city|kingdom|planet|island)\b", label, re.IGNORECASE) and lat is None:
            # still ambiguous; keep unknown unless it matches hints
            pass

    return "unknown", False
