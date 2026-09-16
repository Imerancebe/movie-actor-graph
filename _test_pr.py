"""最小化复现：在 1000 边小图上跑 PageRank 迭代，定位超时原因。"""
import os
import random
import sys

sys.path.insert(0, r"d:\bigmaths\movie-actor-graph\pipeline")
from pyspark import SparkConf, SparkContext

conf = (
    SparkConf()
    .setAppName("PR-debug")
    .setMaster("local[4]")
    .set("spark.default.parallelism", "4")
    .set("spark.local.dir", "d:/bigmaths/_tmp/spark-local")
    .set("spark.network.timeout", "600s")
    .set("spark.python.worker.reuse", "true")
    .set("spark.driver.memory", "2g")
)
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

random.seed(42)
nodes = [f"n{i:04d}" for i in range(300)]
edges = []
for _ in range(1000):
    u, v = random.sample(nodes, 2)
    edges.append((u, v, random.randint(1, 5)))
edges = sc.parallelize(edges, 4)
undirected = edges.flatMap(lambda e: [e, (e[1], e[0], e[2])]).cache()
print("edges:", undirected.count())

n = undirected.map(lambda e: e[0]).distinct().count()
d = 0.85
wsum = undirected.map(lambda e: (e[0], e[2])).reduceByKey(lambda a, b: a + b).cache()
print("wsum:", wsum.count())

msgs = (
    undirected.map(lambda e: (e[0], (e[1], e[2])))
    .join(wsum)
    .map(lambda kv: (kv[1][0][0], (kv[0], kv[1][0][1] / kv[1][1])))
    .cache()
)
print("msgs built:", msgs.count())

pr = wsum.mapValues(lambda _: 1.0 / n).cache()
print("pr init:", pr.count())

for it in range(1, 4):
    contrib = (
        msgs.join(pr).map(lambda kv: (kv[1][0][0], kv[1][1] * kv[1][0][1])).reduceByKey(lambda a, b: a + b)
    ).cache()
    new_pr = contrib.mapValues(lambda c: (1 - d) / n + d * c).cache()
    cnt = new_pr.count()
    delta = new_pr.join(pr).map(lambda kv: abs(kv[1][0] - kv[1][1])).sum()
    pr.unpersist()
    pr = new_pr
    print(f"iter {it}: nodes={cnt} delta={delta:.3e}")

print("sample pr:", pr.takeOrdered(5, key=lambda kv: -kv[1]))
sc.stop()
print("OK")
