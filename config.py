from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Config:
    imdb_base_url: str = "https://datasets.imdbws.com/"
    user_agent: str = "imdb-rapidapi-location-pipeline/1.0 (contact: you@example.com)"
    out_dir: str = "data_out"
    cache_db_path: str = "cache.sqlite"

    # RapidAPI IMDb "imdb-com" endpoint
    rapidapi_host: str = "imdb-com.p.rapidapi.com"
    rapidapi_key_env_var: str = "IMDB_RAPIDAPI_KEY"
    rapidapi_batch_size: int = 150
    rapidapi_sleep_s: float = 0.3
    rapidapi_cache_dir: str = "data_out/rapidapi_location_cache"

    # Geocoding (optional)
    enable_geocoding: bool = False
    geocode_sleep_s: float = 1.0
    geocode_provider: str = "nominatim"   # "nominatim" only in this template

    def get_rapidapi_key(self) -> str:
        """
        Fetch RapidAPI key from environment; keep secrets out of code/config.
        """
        return os.getenv(self.rapidapi_key_env_var, "")
