"""下载 Hadoop 发行包并仅提取 Windows 所需的 winutils.exe / hadoop.dll。"""
import os
import sys
import tarfile
import urllib.request

URL = "https://repo.huaweicloud.com/apache/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz"
TGZ = r"d:\bigmaths\_hadoop.tar.gz"
HADOOP_HOME = r"d:\bigmaths\hadoop"
BIN = os.path.join(HADOOP_HOME, "bin")
os.makedirs(BIN, exist_ok=True)

WANT = {"winutils.exe", "hadoop.dll"}

if not os.path.exists(TGZ) or os.path.getsize(TGZ) < 100_000_000:
    print("downloading", URL)
    urllib.request.urlretrieve(URL, TGZ)
print("downloaded", os.path.getsize(TGZ) / 1e6, "MB")

found = []
with tarfile.open(TGZ, "r:gz") as tf:
    for m in tf:
        base = os.path.basename(m.name)
        if base in WANT and "/bin/" in m.name:
            print("extract", m.name)
            f = tf.extractfile(m)
            dest = os.path.join(BIN, base)
            with open(dest, "wb") as out:
                out.write(f.read())
            found.append(dest)
            if len(found) == len(WANT):
                break

print("done:", found)
sys.exit(0 if found else 1)
