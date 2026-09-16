"""临时工具：下载剩余镜像层并安全解包（跳过符号链接）。"""
import os
import tarfile

import requests

BASE = "https://docker.m.daocloud.io"
REPO = "library/neo4j"
OUT = r"d:\bigmaths\_neo4j_layers"
EXTRACT = r"d:\bigmaths\_neo4j_extract"
os.makedirs(OUT, exist_ok=True)
os.makedirs(EXTRACT, exist_ok=True)

LAYERS = [
    "sha256:6310eb16bf4251731feab01e8f633bf5e2d75a657ccad97f420b1f83cce457be",
    "sha256:9629395e83023ce11aafe575189a4f1ce24908db423acb27455a137f0d2ab740",
    "sha256:773fc9d993caa4e76a37cf1b7db31fbfbf96d0ea59d6f21bf59ad26fb2fd70e0",
    "sha256:a7a71a423b021d4bd558ea2be9ce4f3d35048605299916201b3610689d43b5d0",
    "sha256:4f4fb700ef54461cfa02571ae0db9a0dc1e0cdb5577484a6d75e68dc38e8acc1",
]

s = requests.Session()
tok = s.get(f"https://m.daocloud.io/auth/token?service=docker.m.daocloud.io&scope=repository:{REPO}:pull", timeout=30).json()["token"]
s.headers["Authorization"] = f"Bearer {tok}"

for digest in LAYERS:
    fname = digest.split(":")[1] + ".tar.gz"
    path = os.path.join(OUT, fname)
    if not os.path.exists(path):
        url = f"{BASE}/v2/{REPO}/blobs/{digest}"
        print("down", digest[:19], "...")
        with s.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(path + ".part", "wb") as f:
                for chunk in r.iter_content(1 << 22):
                    f.write(chunk)
        os.replace(path + ".part", path)
    print("  ok", f"{os.path.getsize(path)/1e6:.1f} MB")

print("\nextracting (skip symlinks)...")
for digest in LAYERS:
    fname = digest.split(":")[1] + ".tar.gz"
    path = os.path.join(OUT, fname)
    with tarfile.open(path, "r:gz") as tf:
        for m in tf:
            if m.isfile() or m.isdir():
                tf.extract(m, EXTRACT)
    print("  extracted", fname[:12])

print("done ->", EXTRACT)
