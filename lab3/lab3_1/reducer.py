import sys
from collections import Counter

seen_anime = set()
counter = Counter()

for raw in sys.stdin.buffer:
    line = raw.decode("utf-8", errors="replace").rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue

    anime_id, genres_raw = parts

    if not anime_id:
        continue

    if anime_id in seen_anime:
        continue
    seen_anime.add(anime_id)

    if not genres_raw:
        continue

    genres_norm = str(genres_raw).replace(";", ",")
    for part in genres_norm.split(","):
        genre = part.strip()
        if genre:
            counter[genre] += 1

for genre, total in sorted(counter.items(), key=lambda x: x[1], reverse=True):
    print(f"{genre}\t{total}")
