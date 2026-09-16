import csv
import glob
import math
import os
from collections import defaultdict

path = glob.glob(r"d:\bigmaths\movie-actor-graph\data\processed\graph\edges_coactor\part-*.csv")[0]
adj = defaultdict(set)
with open(path, encoding="utf-8") as f:
    for row in csv.DictReader(f):
        adj[row["actorId"]].add(row["otherActorId"])
        adj[row["otherActorId"]].add(row["actorId"])

degs = {k: len(v) for k, v in adj.items()}
n = len(degs)
edges = sum(degs.values()) // 2
sum_deg2 = sum(d * d for d in degs.values())
top = sorted(degs.items(), key=lambda kv: -kv[1])[:10]

print(f"nodes={n} edges={edges} avg_deg={edges * 2 / n:.2f}")
print(f"sum_deg^2 = {sum_deg2:,}  (2-hop pair volume upper bound)")
print(f"top10 hubs: {[(k, v) for k, v in top]}")

# 共同邻居>=2 的候选对规模（抽样估计太慢则直接算）
# 直接用倒排：对每个 w，枚举 N(w) 两两组合
pair_common = defaultdict(int)
for w, nbrs in adj.items():
    if len(nbrs) < 2:
        continue
    lst = list(nbrs)
    for i in range(len(lst)):
        for j in range(i + 1, len(lst)):
            a, b = lst[i], lst[j]
            pair_common[(a, b) if a < b else (b, a)] += 1

direct = set()
with open(path, encoding="utf-8") as f:
    for row in csv.DictReader(f):
        a, b = row["actorId"], row["otherActorId"]
        direct.add((a, b) if a < b else (b, a))

cands = {p: c for p, c in pair_common.items() if p not in direct}
print(f"pairs_with_common={len(pair_common):,}  candidate_pairs(not direct)={len(cands):,}")
print(f"max_common={max(pair_common.values())}")
