"""Fetch Last.fm tags (genres) for every song in a CSV file, this is what will provide the raw data for 
the Bronze Layer in a Mini Medallion Pipeline

Uses track.getTopTags: https://www.last.fm/api/show/track.getTopTags
Input CSV needs an artist column and a track column which has been aggregated in alteryx. This is the aggregated bit which is 
already being used in tableau for the initial bits but also another branch in the alteryx workflow to only obtain the distinct song 
names as well as the artist which goes with it, removing the song versions which have things such as remastered, feat and radio edit. 


Output: a copy of a CSV with one extra column, "tags" representing genres (semicolon-separated). Which have been input by users in Last.fm
This will then be put into sql to be cleaned as part of the pipeline medallion 

Something to look into - how to best get the main columns as the data will be a bit of a mess (comma and semi colon separated there are many
tags which are inaccurate and some could be grouped but i guess others not) how is it best to group these things more efficiently. Also down 
the line how can i group the songs which simply have a country as a tag.

for the silver/gold layer the country and language tags can be done. figure out if there is a way for the stuff to recognise language 
if not then i guess based onthe country then you can create new groupings. 

there are a lot of things which are lowecase and upper case, these will need to be accounted for to standardise it. and also figure out 
how well can sql read all the differnt letters. 

DONT FORGET TO SAVE ONCE TERMINAL STOPS RUNNING
"""

import csv
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

# -------------------------------------------------------------------------
# this is the part of the code which reads the input file and within the csv localises the column headers to then
# parse it out and connect to the api. 

ROOT = Path(__file__).resolve().parent.parent
INPUT_CSV = ROOT / "my_spotify_data" / "song_artist_call_all.csv"          # existing file
OUTPUT_CSV = ROOT / "my_spotify_data" / "songs_with_genres.csv"
ARTIST_COLUMN = "artist_name"                         # column names in csv
TRACK_COLUMN = "track_name"
MAX_TAGS = 15          # keep only the top N tags per song
MIN_COUNT = 50        # drop tags whose weight is below this (max is 100)
# ----------------------------------------------------------------------------

# section which determines the api url to connect to as well as the common errors which might be encountered 
# and more importantly how long it takes to make each call as doing it very quickly might trigger the website 
# and it is possible to get banned 

API_URL = "https://ws.audioscrobbler.com/2.0/"
PAUSE_SECONDS = 0.5
MAX_RETRIES = 3
RETRYABLE_ERRORS = {11, 16, 29}   # offline, temporary error, rate limit
NOT_FOUND_ERRORS = {6, 7}         # bad params / resource not found -> skip song

# I think this bit below i need to delete before i upload CHECK 

def get_api_key() -> str:
    load_dotenv(ROOT / ".env")
    key = os.getenv("LASTFM_API_KEY")
    if not key:
        raise RuntimeError("LASTFM_API_KEY not found. Check the variable name in your .env file.")
    return key

# this bit is what if there is a loss in connection or there is a time out the code will keep trying, it will fail after
# a long time but basic connection losses of a couple of secs it should get over

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

# this checks last.fms track_tags (ie song names) and pulls the user inputs from that which is waht is then giving the 
# genre, the next 'def' section is a failsafe so that in the case that there are no genre tags associated to that song
# it will default to the artist tag so that something shows up. 

# running the code i can now see that there are certain instances where there is no tag whatsoever, if another attempt at the whole 
# is needed then the MAX_TAGS and MIN_COUNT which are in the first section can be increased. 

def get_track_tags(session: requests.Session, api_key: str, artist: str, track: str) -> list[str]:
    params = {
        "method": "track.gettoptags",
        "artist": artist,
        "track": track,
        "api_key": api_key,
        "format": "json",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        resp = safe_get(session, params)
        if resp.status_code != 200:
            # print the status without the URL, so your API key doesn't appear
            raise RuntimeError(f"HTTP {resp.status_code} for '{artist}' - '{track}'")
        data = resp.json()

        if "error" not in data:
            tags = data.get("toptags", {}).get("tag", [])
            if isinstance(tags, dict):
                tags = [tags]
            kept = [t["name"] for t in tags if int(t.get("count", 0)) >= MIN_COUNT]
            return kept[:MAX_TAGS]

        code = data["error"]
        if code in NOT_FOUND_ERRORS:
            return []
        if code in RETRYABLE_ERRORS and attempt < MAX_RETRIES:
            time.sleep(2 * attempt)
            continue
        raise RuntimeError(f"Last.fm API error {code}: {data.get('message')}")
    return []

def get_artist_tags(session: requests.Session, api_key: str, artist: str) -> list[str]:
    params = {
        "method": "artist.gettoptags",
        "artist": artist,
        "api_key": api_key,
        "format": "json",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        resp = safe_get(session, params)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code} for artist '{artist}'")
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

def main() -> None:
    api_key = get_api_key()

    # Resume support: skip songs already written to the output file
    done: set[tuple[str, str]] = set()
    if OUTPUT_CSV.exists():
        with open(OUTPUT_CSV, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                done.add((row[ARTIST_COLUMN], row[TRACK_COLUMN]))

    with open(INPUT_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames + ["tags", "tag_source"]

    write_header = not OUTPUT_CSV.exists()
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8-sig") as out, requests.Session() as session:
        session.headers["User-Agent"] = "music-genre-ingestion/0.1"
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        for i, row in enumerate(rows, start=1):
            key = (row[ARTIST_COLUMN], row[TRACK_COLUMN])
            if key in done:
                continue
            tags = get_track_tags(session, api_key, *key)
            source = "track"
            if not tags:
             time.sleep(PAUSE_SECONDS)
            tags = get_artist_tags(session, api_key, key[0])
            source = "artist"
            row["tags"] = ";".join(tags)
            row["tag_source"] = source if tags else ""
            writer.writerow(row)
            out.flush()                      # keep progress if interrupted
            print(f"{i}/{len(rows)}  {key[0]} - {key[1]}: {row['tags'] or '(no tags)'}")
            time.sleep(PAUSE_SECONDS)


if __name__ == "__main__":
    main()

