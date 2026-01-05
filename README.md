# IMDb Filming Locations Pipeline

Build a dataset of IMDb movies with filming locations (via RapidAPI) plus simple location features.

## Setup

- Python 3.10+ and `pip`.
- Install deps: `pip install -r requirements.txt`
- Set your RapidAPI key (required):
  - PowerShell: `$env:IMDB_RAPIDAPI_KEY = 'your-rapidapi-key'`
  - Persist (optional): `setx IMDB_RAPIDAPI_KEY "your-rapidapi-key"` (restart shell)
- Optional: enable geocoding by toggling `Config.enable_geocoding` (uses Nominatim, cached in SQLite).

## Key Files

- `pipeline.py` – orchestrates the end-to-end run.
- `config.py` – runtime settings (RapidAPI host/batching/cache dirs, UA, output paths).
- `filmlocations.py` – fetches filming locations from RapidAPI IMDb endpoint with retry + on-disk cache.
- `storage.py` – SQLite caches for RapidAPI payloads (table `rapidapi_locations`) and geocoding.
- `location_classify.py` – labels locations as real/fictional/unknown.
- `geocode.py` – Nominatim geocoder (used only if enabled).
- `features.py` – aggregates title-level features from location data.
- `imdb_datasets.py` – downloads and loads IMDb TSVs.

## Running the Pipeline

```bash
python pipeline.py
```

Outputs (written to `data_out/` by default):
- `movies_locations_long.parquet` – long form movie+location rows.
- `movies_locations_title_features.parquet` – wide title-level features.
- `sample_long.csv`, `sample_wide.csv` – small CSV samples.

## Notes

- RapidAPI endpoint: `https://imdb-com.p.rapidapi.com/title/get-filming-locations?tconst=...`
- Only filming locations are fetched (no featured locations).
- Coordinates are generally absent from the API; optional geocoding can fill gaps for non-fictional labels.
- Caching: RapidAPI payloads can also be cached to disk via `filmlocations.py` (see `Config.rapidapi_cache_dir`) and to SQLite (`storage.py`).
