"""Snippet: mock fetcher to drop NDJSON into data/landing.
- Demonstrates reservoir sampling (Vitter's Algorithm R) on fetched items.
"""
import os, time, random
from dotenv import load_dotenv
from libs.io import write_ndjson
from libs.config import load_logging_config

# Load environment variables
load_dotenv("conf/.env")
load_logging_config()

LANDING = os.getenv("LANDING_DIR", "data/landing")
SAMPLE_OUT = os.path.join("data","bronze","_sample")
os.makedirs(LANDING, exist_ok=True); os.makedirs(SAMPLE_OUT, exist_ok=True)

def reservoir_sample(iterable, k=64):
    sample, n = [], 0
    for x in iterable:
        n += 1
        if len(sample) < k:
            sample.append(x)
        else:
            j = random.randint(1, n)
            if j <= k:
                sample[j-1] = x
    return sample

# Pretend these came from an API
items = [{
    "post_id": f"p{i}", "text": f"hello like {i}", "lang": "en",
    "ts": int(time.time()*1000), "author_id": f"u{i%7}", "video_id": f"v{i%3}"
} for i in range(40)]

# Drop NDJSON file
fname = os.path.join(LANDING, f"events_{int(time.time())}.json")
write_ndjson(fname, items)
print("Wrote:", fname)

# Save small representative sample
sample = reservoir_sample(items, k=16)
sname = os.path.join(SAMPLE_OUT, f"sample_{int(time.time())}.json")
write_ndjson(sname, sample)
print("Sample saved:", sname)
