import csv
import sys

ANIME_IDX = 1   # anime_id
GENRES_IDX = 5  # Genres

def read_lines():
    for raw in sys.stdin.buffer:
        yield raw.decode("utf-8", errors="replace").rstrip("\n")

first = True

for line in read_lines():
    
    if not line:
        continue

    if first:
        first = False
        continue

    try:
        row = next(csv.reader([line]))
    except Exception:
        continue

    if len(row) <= GENRES_IDX:
        continue

    anime_id = row[ANIME_IDX]
    genres   = row[GENRES_IDX]

    if not anime_id or not genres:
        continue

    print(f"{anime_id}\t{genres}")
