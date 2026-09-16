"""临时工具：解包已下载的镜像层（跳过符号链接，Windows 兼容）。"""
import os
import tarfile

OUT = r"d:\bigmaths\_neo4j_layers"
EXTRACT = r"d:\bigmaths\_neo4j_extract"
os.makedirs(EXTRACT, exist_ok=True)

links = []
for fname in sorted(os.listdir(OUT)):
    path = os.path.join(OUT, fname)
    if not fname.endswith(".tar.gz"):
        continue
    print("extract", fname, f"{os.path.getsize(path)/1e6:.1f} MB")
    with tarfile.open(path, "r:gz") as tf:
        for m in tf:
            if m.issym() or m.islnk():
                links.append((m.name, m.linkname))
                continue
            if not (m.isfile() or m.isdir()):
                continue
            tf.extract(m, EXTRACT)

print("\n--- symlinks skipped ---")
for name, target in links:
    print(f"{name} -> {target}")
