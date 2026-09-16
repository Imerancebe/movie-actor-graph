import json

import requests

base = "http://127.0.0.1:8000"
r = requests.get(f"{base}/api/movie/tt9364406/detail", timeout=60).json()
print("movie detail:", json.dumps(r, ensure_ascii=False)[:500])
r2 = requests.get(f"{base}/api/movie/tt999999/detail", timeout=60).json()
print("movie 404:", r2["code"], r2["msg"])
