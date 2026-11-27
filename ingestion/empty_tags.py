"""Fill in the songs that came back with no tags.

Reads your finished tags CSV, finds rows where "tags" is empty, and tries again
with no minimum count (so even weak tags are kept), first for the song itself
and then for its artist. Your original file is NOT changed: the result is
written to a new file.
"""

import csv
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

# ---- Configure these -------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
INPUT_CSV = ROOT / "my_spotify_data" / "songs_with_genres.csv"      # your finished 16k tags file
OUTPUT_CSV = ROOT / "my_spotify_data" / "songs_with_genres_patched.csv"
ARTIST_COLUMN = "artist_name"                          # same names as in scrape.py
TRACK_COLUMN = "track_name"
MAX_TAGS = 20         # tags to keep per song
MIN_COUNT = 1         # 1 = keep even very weak tags
PAUSE_SECONDS = 0.3
# ----------------------------------------------------------------------------

API_URL = "https://ws.audioscrobbler.com/2.0/"
MAX_RETRIES = 3
RETRYABLE_ERRORS = {11, 16, 29}
NOT_FOUND_ERRORS = {6, 7}


def get_api_key() -> str:
    load_dotenv(ROOT / ".env")
    key = os.getenv("LASTFM_API_KEY")
    if not key:
        raise RuntimeError("LASTFM_API_KEY not found. Check the variable name in your .env file.")
    return key


def safe_get(session: requests.Session, params: dict) -> requests.Response:
    """GET that waits and retries if the connection drops or Last.fm is slow."""
    for attempt in range(1, 11):
        try:
            return session.get(API_URL, params=params, timeout=30)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            wait = min(30 * attempt, 300)
            print(f"Connection problem, retrying in {wait}s (attempt {attempt}/10)")
            time.sleep(wait)
    raise RuntimeError("Connection kept failing, giving up")


def fetch_tags(session: requests.Session, api_key: str, method: str, **extra) -> list[str]:
    params = {"method": method, "api_key": api_key, "format": "json", **extra}
    for attempt in range(1, MAX_RETRIES + 1):
        resp = safe_get(session, params)
        if resp.status_code in (500, 502, 503, 504):
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)
                continue
            print(f"  Last.fm server error {resp.status_code}, skipping {extra}")
            return []
        if resp.status_code != 200:
            # status only, so the URL (and your API key) never gets printed
            raise RuntimeError(f"HTTP {resp.status_code} for {extra}")
        data = resp.json()

        if "error" not in data:
            tags = data.get("toptags", {}).get("tag", [])
            if isinstance(tags, dict):
                tags = [tags]
            kept = [t["name"] for t in tags if int(t.get("count", 100)) >= MIN_COUNT]
            return kept[:MAX_TAGS]

        code = data["error"]
        if code in NOT_FOUND_ERRORS:
            return []
        if code in RETRYABLE_ERRORS and attempt < MAX_RETRIES:
            time.sleep(2 * attempt)
            continue
        raise RuntimeError(f"Last.fm API error {code}: {data.get('message')}")
    return []


def write_output(rows: list[dict], fieldnames: list[str]) -> None:
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    api_key = get_api_key()

    with open(INPUT_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames)
    for extra_col in ("tag_source", "patched"):
        if extra_col not in fieldnames:
            fieldnames.append(extra_col)

    empty = [r for r in rows if not (r.get("tags") or "").strip()]
    print(f"{len(rows)} rows, {len(empty)} with no tags")

    fixed = 0
    try:
        with requests.Session() as session:
            session.headers["User-Agent"] = "music-genre-ingestion/0.1"
            for i, row in enumerate(empty, start=1):
                artist, track = row[ARTIST_COLUMN], row[TRACK_COLUMN]

                tags = fetch_tags(session, api_key, "track.gettoptags", artist=artist, track=track)
                source = "track"
                if not tags:
                    time.sleep(PAUSE_SECONDS)
                    tags = fetch_tags(session, api_key, "artist.gettoptags", artist=artist)
                    source = "artist"

                if tags:
                    row["tags"] = ";".join(tags)
                    row["tag_source"] = source
                    row["patched"] = "yes"
                    fixed += 1
                print(f"{i}/{len(empty)}  {artist} - {track}: {row['tags'] or '(still no tags)'}")
                time.sleep(PAUSE_SECONDS)
    finally:
        # Always save what we have, even if the run was interrupted
        write_output(rows, fieldnames)
        print(f"Saved to {OUTPUT_CSV}. Filled {fixed} of {len(empty)} empty rows.")


if __name__ == "__main__":
    main()