import requests
from scrape import get_api_key, API_URL

for artist, track in [("Radiohead", "Paranoid Android"), ("milo j", "lo que me causa")]:
    r = requests.get(API_URL, params={
        "method": "track.gettoptags",
        "artist": artist,
        "track": track,
        "api_key": get_api_key(),
        "format": "json",
    }, timeout=15)
    print(artist, "-", track, "->", r.status_code)
    print(r.text[:600])
    print()