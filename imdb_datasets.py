import os
import urllib.request
import pandas as pd
from typing import Optional, List

IMDB_FILES = {
    "basics": "title.basics.tsv.gz",
    "ratings": "title.ratings.tsv.gz",
    "crew": "title.crew.tsv.gz",
}

def download_if_needed(imdb_base_url: str, out_dir: str, filename: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, filename)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return path
    url = imdb_base_url.rstrip("/") + "/" + filename
    print(f"Downloading {url}")
    with urllib.request.urlopen(url) as r, open(path, "wb") as f:
        f.write(r.read())
    return path

def load_tsv_gz(path: str, usecols: Optional[List[str]] = None) -> pd.DataFrame:
    return pd.read_csv(
        path,
        sep="\t",
        compression="gzip",
        dtype=str,
        na_values="\\N",
        keep_default_na=True,
        usecols=usecols,
    )

def load_movies_with_ratings(
    imdb_base_url: str,
    data_dir: str,
    sample_n: Optional[int] = None,
) -> pd.DataFrame:
    basics_path = download_if_needed(imdb_base_url, data_dir, IMDB_FILES["basics"])
    ratings_path = download_if_needed(imdb_base_url, data_dir, IMDB_FILES["ratings"])
    crew_path = download_if_needed(imdb_base_url, data_dir, IMDB_FILES["crew"])

    basics = load_tsv_gz(
        basics_path,
        usecols=[
            "tconst","titleType","primaryTitle","originalTitle","isAdult",
            "startYear","runtimeMinutes","genres"
        ]
    )
    ratings = load_tsv_gz(ratings_path, usecols=["tconst","averageRating","numVotes"])
    crew = load_tsv_gz(crew_path, usecols=["tconst","directors","writers"])

    movies = basics[basics["titleType"] == "movie"].copy()
    if sample_n:
        movies = movies.head(sample_n)

    df = movies.merge(ratings, on="tconst", how="left").merge(crew, on="tconst", how="left")

    # numeric conversion
    for col in ["startYear","runtimeMinutes","averageRating","numVotes","isAdult"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df
