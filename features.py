import pandas as pd
from math import radians, sin, cos, asin, sqrt
from typing import Optional

def haversine_km(lat1, lon1, lat2, lon2) -> Optional[float]:
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return None
    # km
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    c = 2*asin(sqrt(a))
    return R*c

def compute_title_level_features(loc_long: pd.DataFrame) -> pd.DataFrame:
    """
    Given long-form locations (multiple rows per tconst), compute a few title-level stats:
      - counts of filming vs featured locations
      - whether featured contains fictional
      - min distance between any filming and any featured real coords
    """
    # Count kinds
    counts = loc_long.pivot_table(index="tconst", columns="location_kind", values="location_label", aggfunc="count").fillna(0)
    counts.columns = [f"n_{c}_locations" for c in counts.columns]
    counts = counts.reset_index()

    fictional = loc_long.groupby("tconst")["is_fictional"].any().reset_index().rename(columns={"is_fictional":"has_fictional_featured_or_unknown"})

    # Distance: compute per tconst by brute force (ok-ish; optimize later)
    dist_rows = []
    for t, g in loc_long.groupby("tconst"):
        filming = g[(g["location_kind"]=="filming") & g["lat"].notna() & g["lon"].notna()]
        featured = g[(g["location_kind"]=="featured") & g["lat"].notna() & g["lon"].notna()]
        best = None
        if len(filming) and len(featured):
            for _, fr in filming.iterrows():
                for _, fe in featured.iterrows():
                    d = haversine_km(fr["lat"], fr["lon"], fe["lat"], fe["lon"])
                    if d is not None and (best is None or d < best):
                        best = d
        dist_rows.append({"tconst": t, "min_km_film_to_featured": best})
    ddf = pd.DataFrame(dist_rows)

    return counts.merge(fictional, on="tconst", how="left").merge(ddf, on="tconst", how="left")
