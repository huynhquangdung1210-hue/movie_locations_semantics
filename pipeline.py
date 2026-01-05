import os
import pandas as pd
from tqdm import tqdm

from config import Config
import storage
from imdb_datasets import load_movies_with_ratings
from filmlocations import imdb_filming_locations_via_rapidapi
from location_classify import classify_location
from geocode import geocode_nominatim
from features import compute_title_level_features

def build_location_long_table(cfg: Config, movies_df: pd.DataFrame) -> pd.DataFrame:
    rapidapi_key = cfg.get_rapidapi_key()
    if not rapidapi_key:
        raise RuntimeError(
            f"RapidAPI key missing. Set env var {cfg.rapidapi_key_env_var} before running."
        )

    tconsts = movies_df["tconst"].dropna().unique().tolist()

    # 1) Fetch filming locations via RapidAPI (on-disk cache optional)
    loc_long = imdb_filming_locations_via_rapidapi(
        tconsts,
        rapidapi_key=rapidapi_key,
        rapidapi_host=cfg.rapidapi_host,
        batch_size=cfg.rapidapi_batch_size,
        sleep_s=cfg.rapidapi_sleep_s,
        cache_dir=cfg.rapidapi_cache_dir,
        user_agent=cfg.user_agent,
    )

    if loc_long.empty:
        # still return a consistent schema
        return pd.DataFrame(columns=[
            "tconst","location_kind","location_item","location_label","lat","lon",
            "location_class","is_fictional"
        ])

    # 2) classify real/fictional/unknown
    classes = []
    fictional_flags = []
    for _, r in loc_long.iterrows():
        c, isf = classify_location(r.get("location_label"), r.get("lat"), r.get("lon"))
        classes.append(c)
        fictional_flags.append(isf)
    loc_long["location_class"] = classes
    loc_long["is_fictional"] = fictional_flags

    # 3) optional geocoding for missing coords (ONLY for "real-ish" unknowns)
    if cfg.enable_geocoding:
        con = storage.connect(cfg.cache_db_path)
        for idx, r in tqdm(loc_long.iterrows(), total=len(loc_long), desc="Geocoding"):
            if pd.notna(r.get("lat")) and pd.notna(r.get("lon")):
                continue
            label = r.get("location_label")
            if not isinstance(label, str) or not label.strip():
                continue
            # Don't geocode obvious fictional
            if r.get("location_class") == "fictional":
                continue

            # cache
            cached = storage.get_geocode(con, label)
            if cached:
                lat, lon = cached
                loc_long.at[idx, "lat"] = lat
                loc_long.at[idx, "lon"] = lon
                loc_long.at[idx, "location_class"] = "real"
                continue

            ll = geocode_nominatim(label, user_agent=cfg.user_agent, sleep_s=cfg.geocode_sleep_s)
            if ll:
                lat, lon = ll
                storage.set_geocode(con, label, lat, lon, cfg.geocode_provider)
                loc_long.at[idx, "lat"] = lat
                loc_long.at[idx, "lon"] = lon
                loc_long.at[idx, "location_class"] = "real"

        con.commit()
        con.close()
    return loc_long

def main():
    cfg = Config()
    os.makedirs(cfg.out_dir, exist_ok=True)

    # Load IMDb movies+ratings (official bulk TSVs, not scraping)
    movies_df = load_movies_with_ratings(cfg.imdb_base_url, data_dir=cfg.out_dir, sample_n=None)
    print(f"Loaded movies: {len(movies_df):,}")

    # Build long table of locations
    loc_long = build_location_long_table(cfg, movies_df)
    print(f"Location rows: {len(loc_long):,}")

    # Merge to make a denormalized long dataset
    merged_long = movies_df.merge(loc_long, on="tconst", how="left")

    # Compute title-level features (optional)
    title_feats = compute_title_level_features(loc_long) if len(loc_long) else pd.DataFrame({"tconst": movies_df["tconst"]})
    merged_wide = movies_df.merge(title_feats, on="tconst", how="left")

    # Output
    long_path = os.path.join(cfg.out_dir, "movies_locations_long.parquet")
    wide_path = os.path.join(cfg.out_dir, "movies_locations_title_features.parquet")

    merged_long.to_parquet(long_path, index=False)
    merged_wide.to_parquet(wide_path, index=False)

    # Also small CSV samples for inspection
    merged_long.head(5000).to_csv(os.path.join(cfg.out_dir, "sample_long.csv"), index=False)
    merged_wide.head(5000).to_csv(os.path.join(cfg.out_dir, "sample_wide.csv"), index=False)

    print("Wrote:")
    print(" -", long_path)
    print(" -", wide_path)

if __name__ == "__main__":
    main()
