"""01 数据下载：从 IMDb 官方公开数据集下载三个 TSV.gz 到 data/raw/。

本地已存在且大小 > 10MB 的文件跳过（断点/重跑友好，FR-D1 / NFR-5）。
"""
import gzip
import os
import sys
import time

import requests

import common


def looks_like_gzip(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(2) == b"\x1f\x8b"
    except OSError:
        return False


def download(fname: str) -> None:
    dest = common.RAW_FILES[fname]
    if os.path.exists(dest) and os.path.getsize(dest) > 10 * 1024 * 1024 and looks_like_gzip(dest):
        print(f"[skip] {fname} 已存在 ({os.path.getsize(dest) / 1e6:.1f} MB)")
        return

    url = f"{common.IMDB_BASE}/{fname}"
    print(f"[down] {url}")
    t0 = time.time()
    for attempt in range(1, 4):
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))
                done = 0
                with open(dest + ".part", "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
                        done += len(chunk)
                        if total and done % (100 << 20) < (1 << 20):
                            print(f"  ... {done / 1e6:.0f}/{total / 1e6:.0f} MB")
            if not looks_like_gzip(dest + ".part"):
                raise ValueError("下载内容不是有效 gzip，可能被中断")
            os.replace(dest + ".part", dest)
            print(f"[done] {fname} {os.path.getsize(dest) / 1e6:.1f} MB，"
                  f"耗时 {time.time() - t0:.0f}s")
            return
        except Exception as e:  # noqa: BLE001 - 网络重试
            print(f"  第 {attempt} 次尝试失败: {e}")
            time.sleep(5 * attempt)
    raise RuntimeError(f"下载 {fname} 失败，请检查网络后重跑")


def main() -> None:
    for fname in common.RAW_FILES:
        download(fname)
    print("全部原始数据就绪。")


if __name__ == "__main__":
    sys.exit(main())
