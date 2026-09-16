"""用真实 CO_ACTOR 数据调试 PageRank 前几轮，逐步打印阶段。"""
import sys
import time

sys.path.insert(0, r"d:\bigmaths\movie-actor-graph\pipeline")
from pyspark import SparkConf, SparkContext

import common
from importlib import import_module

gm = import_module("05_graph_mining")

conf = (
    SparkConf()
    .setAppName("PR-debug-real")
    .setMaster("local[8]")
    .set("spark.default.parallelism", "8")
    .set("spark.local.dir", "d:/bigmaths/_tmp/spark-local")
    .set("spark.network.timeout", "900s")
    .set("spark.python.worker.reuse", "true")
    .set("spark.python.worker.memory", "512m")
    .set("spark.driver.memory", "2g")
)
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

t0 = time.time()
coactor = gm.read_csv_rows(sc, "edges_coactor", lambda r: (r["actorId"], r["otherActorId"], int(r["weight"])))
undirected = coactor.flatMap(lambda e: [e, (e[1], e[0], e[2])]).repartition(8).cache()
print(f"load {undirected.count()} edges in {time.time()-t0:.1f}s", flush=True)

n = undirected.map(lambda e: e[0]).distinct().count()
print(f"nodes={n}", flush=True)

d = 0.85
wsum = undirected.map(lambda e: (e[0], e[2])).reduceByKey(lambda a, b: a + b).repartition(8).cache()
print(f"wsum {wsum.count()} in {time.time()-t0:.1f}s", flush=True)

msgs = (
    undirected.map(lambda e: (e[0], (e[1], e[2])))
    .join(wsum)
    .map(lambda kv: (kv[1][0][0], (kv[0], kv[1][0][1] / kv[1][1])))
    .repartition(8)
    .cache()
)
print(f"msgs {msgs.count()} in {time.time()-t0:.1f}s", flush=True)

pr = wsum.mapValues(lambda _: 1.0 / n).cache()
for it in range(1, 3):
    t1 = time.time()
    contrib = (
        msgs.join(pr)
        .map(lambda kv: (kv[1][0][0], kv[1][1] * kv[1][0][1]))
        .reduceByKey(lambda a, b: a + b)
        .repartition(8)
        .cache()
    )
    new_pr = contrib.mapValues(lambda c: (1 - d) / n + d * c).cache()
    cnt = new_pr.count()
    print(f"iter {it}: {cnt} nodes in {time.time()-t1:.1f}s (total {time.time()-t0:.1f}s)", flush=True)
    pr.unpersist()
    pr = new_pr

print("DONE", flush=True)
sc.stop()
