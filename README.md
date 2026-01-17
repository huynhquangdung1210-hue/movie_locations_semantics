# IMDb Filming Locations Pipeline

Build a dataset of IMDb titles with filming locations from the RapidAPI IMDb endpoint, then compute simple location features. Optional geocoding fills missing coordinates for non-fictional locations.

## Requirements

- Python 3.10+
- `pip`

## Setup

```bash
pip install -r requirements.txt
```

Set your RapidAPI key (required):

PowerShell:
```powershell
$env:IMDB_RAPIDAPI_KEY = 'your-rapidapi-key'
```

Persist (optional):
```powershell
setx IMDB_RAPIDAPI_KEY "your-rapidapi-key"
```

## Configuration

Runtime settings live in `config.py`:

- RapidAPI host, batch size, and sleep between calls
- Cache locations for RapidAPI payloads and geocoding
- Output directory (`data_out/` by default)
- Toggle geocoding with `Config.enable_geocoding`

## Running

```bash
python pipeline.py
```

## Outputs

Written to `data_out/` by default:

- `movies_locations_long.parquet` - long-form movie + location rows
- `movies_locations_title_features.parquet` - title-level features
- `sample_long.csv` and `sample_wide.csv` - small CSV samples

## Data Sources

- IMDb bulk TSVs are downloaded via `imdb_datasets.py` (official datasets)
- Filming locations are fetched from RapidAPI:
  `https://imdb-com.p.rapidapi.com/title/get-filming-locations?tconst=...`

## Caching

- RapidAPI payloads can be cached to disk and to SQLite
- Geocoding results are cached in SQLite to reduce calls

## Repository Layout

- `pipeline.py` - orchestrates the end-to-end run
- `filmlocations.py` - RapidAPI client with retries and caching
- `storage.py` - SQLite cache helpers
- `location_classify.py` - simple real/fictional/unknown labeling
- `geocode.py` - Nominatim geocoder (optional)
- `features.py` - title-level feature aggregation
- `imdb_datasets.py` - downloads and loads IMDb TSVs

## Notes

- The RapidAPI endpoint returns filming locations only (not featured locations).
- Coordinates are often missing; geocoding is optional and skipped for fictional locations.
