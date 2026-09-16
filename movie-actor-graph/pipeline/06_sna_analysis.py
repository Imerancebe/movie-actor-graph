r"""06 社交网络分析（SNA）：小世界 / 无标度 / 介数 / K-core / 年代演化（FR-M7~M10）。

设计规约 3.6 的六项算法在此落地，混合 Spark（大规模聚合）与驱动端（小图精确算法）：

1. 三角计数与聚类系数（Spark）：邻接表广播，每条边求 |N(u)∩N(v)|，
   T = Σc/3；节点级 tri(u) = ½Σ_{v∈N(u)} c(u,v)；
   局部聚类系数 2·tri(u)/(deg(u)(deg(u)-1))；全局传递性 3T/ΣC(deg,2)
2. 多源位图 BFS（Spark）：固定种子从巨片抽 63 个源（实测网络含 1,155 个连通
   分量、巨片仅 13%，跨分量无路径，全图随机采样会被小分量主导而失真），
   节点维护 63 位掩码，每轮沿边按位 OR 传播；轮次×新增位数 = 距离直方图
   → 采样平均路径/直径；同时累计节点级 distsum → 采样接近中心性
3. K-core 剥壳（驱动端，Batagelj-Zaversnik）：小根堆按当前度移除，
   移除时的度即 core（=kshell）。22K 节点/110K 边内存瞬时完成
4. Brandes 介数（驱动端）：度数 Top-1000 诱导子图上精确 BFS+依赖反向传播；
   无向图每条最短路被计两次 → ÷2，归一化 2·BC/((n-1)(n-2))
5. 度分布（Spark）：reduceByKey 聚合直方图；Top-1% 度数份额 + log-log
   最小二乘幂律斜率验证无标度特性
6. 年代演化（Spark）：edges_acted_in 按电影分组两两配对携带年份，
   reduceByKey min 得每对首次合作年；演员出道年同理；按年聚合 + 累计

产出：data/mining/{sna_summary.json, actor_sna_metrics.csv, degree_distribution.csv,
evolution.csv}；回写 Neo4j：a.triangles / a.clusteringCoefficient / a.kshell /
a.closeness / a.betweenness（仅核心子图成员）+ kshell/betweenness 索引。

Windows 调优沿用 05 的踩坑记录（local[4]、SPARK_AUTH_SOCKET_TIMEOUT、预分区）。
"""
import csv
import glob
import heapq
import json
import math
import os
import random
import sys
from collections import Counter, deque

# 必须在 import pyspark 前设置：worker↔JVM 套接字读超时默认 15s，
# 驱动 JVM 长 GC 停顿会使 Python worker 抛 TimeoutError 中止作业
os.environ.setdefault("SPARK_AUTH_SOCKET_TIMEOUT", "600")

from pyspark import SparkConf, SparkContext

import common

P = 4  # 分区数 = worker 并发，与 SPARK_MINING_MASTER 匹配

BFS_SOURCES = 63        # 采样源数（多源 BFS，平均路径估计）
BFS_MAX_ROUNDS = 40     # BFS 迭代上限（网络直径保险值）
BRANDES_TOPN = 1000     # Brandes 核心子图规模（度数 Top-N）


def log(msg):
    print(msg, flush=True)


# ---------------- CSV 读取 ----------------
def read_csv_rows(*cols):
    files = glob.glob(os.path.join(common.GRAPH_DIR, cols[-1], "part-*.csv"))
    if not files:
        raise FileNotFoundError(f"{common.GRAPH_DIR}/{cols[-1]} 缺少 part-*.csv")
    rows = []
    for fp in files:
        with open(fp, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append(r)
    return rows


def read_coactor():
    """返回 [(u, v, w)]（单向存储，u→v）。"""
    return [(r["actorId"], r["otherActorId"], int(r["weight"])) for r in read_csv_rows("edges_coactor")]


def read_actor_names():
    return {r["actorId"]: r["name"] for r in read_csv_rows("nodes_actors")}


def read_movie_years():
    """返回 {movieId: year}，年份缺失的跳过。"""
    out = {}
    for r in read_csv_rows("nodes_movies"):
        y = r.get("year")
        if y:
            try:
                out[r["movieId"]] = int(y)
            except (TypeError, ValueError):
                continue
    return out


# ---------------- ① 三角计数与聚类系数（Spark） ----------------
def triangles(sc, undirected, adj_map):
    """undirected: RDD[(u,v,w)] 双向；adj_map: 驱动端邻接 dict（广播）。
    返回 (三角总数 T, {node: tri})。"""
    adj_b = sc.broadcast(adj_map)

    def common_cnt(e):
        u, v = e[0], e[1]
        nu = adj_b.value.get(u)
        nv = adj_b.value.get(v)
        c = len(nu & nv) if nu and nv else 0
        return (u, v, c)

    # 只保留 u<v 一种方向，每条边算一次共同邻居数
    edge_c = undirected.filter(lambda e: e[0] < e[1]).map(common_cnt).cache()
    total_c = edge_c.map(lambda e: e[2]).sum()
    T = total_c / 3.0

    # 每条边把 c 贡献给两个端点；每个三角形在节点 u 处恰好计 2 次 → ÷2
    node_tri = (
        edge_c.flatMap(lambda e: [(e[0], e[2]), (e[1], e[2])])
        .reduceByKey(lambda a, b: a + b, P)
        .mapValues(lambda s: s // 2)
    )
    return T, dict(node_tri.collect())


# ---------------- ② 多源位图 BFS（Spark） ----------------
def popcount(x):
    return bin(x).count("1")


def multisource_bfs(sc, nbr, nodes, sources):
    """nbr: RDD[(u,v)] 双向邻接（已 partitionBy）；nodes: 全体节点 list；
    sources: 源节点 list（有序）。
    返回 (距离直方图 {dist: pairs}, avg_path, diameter, {node: closeness})。
    稳定性设计（踩坑记录）：节点距离和不维护 distsum RDD 链——惰性 RDD 链随轮次
    加深，一旦缓存被逐出会触发 O(r²) 谱系重算；曾因 33 轮累计缓存压垮 2G 驱动
    JVM（长 GC → worker 套接字 15s 超时）。改为每轮 collect 22K 行 added 到
    驱动端本地累加，Spark 侧仅保留 masks 一条浅缓存链。"""
    src_mask = {s: 1 << i for i, s in enumerate(sources)}

    masks = sc.parallelize([(n, src_mask.get(n, 0)) for n in nodes]).partitionBy(P).cache()

    hist = {}
    distsum_local = {}
    total_dist = 0
    total_pairs = 0
    diameter = 0
    for rnd_i in range(1, BFS_MAX_ROUNDS + 1):
        # 沿边传播掩码：u 的掩码发给 v
        received = (
            nbr.join(masks)  # (u, (v, mask_u))，同分区器零 shuffle
            .map(lambda kv: (kv[1][0], kv[1][1]))
            .reduceByKey(lambda a, b: a | b, P)
        )
        new_masks = (
            masks.leftOuterJoin(received)  # 未收到消息的节点保留原掩码
            .mapValues(lambda x: x[0] | (x[1] or 0))  # mapValues 保持分区器，免 shuffle
            .cache()
        )
        # 本轮新增位数 = popcount(新) - popcount(旧)，即距离 rnd_i 的点对数
        added = (
            new_masks.join(masks)
            .mapValues(lambda x: popcount(x[0]) - popcount(x[1]))
            .cache()
        )
        total_new = added.map(lambda kv: kv[1]).sum()
        if total_new > 0:
            hist[rnd_i] = int(total_new)
            diameter = rnd_i
            total_dist += rnd_i * total_new
            total_pairs += total_new
        # 驱动端本地累计节点距离和（added 已被上面的 action 物化，collect 走缓存）
        for n, a in added.collect():
            if a:
                distsum_local[n] = distsum_local.get(n, 0) + a * rnd_i
        added.unpersist()
        masks.unpersist()
        masks = new_masks
        if total_new > 0:
            log(f"[bfs] round={rnd_i} new_pairs={int(total_new)} cumulative={total_pairs}")
        else:
            break

    avg_path = total_dist / total_pairs if total_pairs else 0.0

    # 采样接近中心性：reached(v) / Σ_s dist(s,v)
    closeness = {}
    for n, mask in masks.collect():
        reached = popcount(mask)
        dsum = distsum_local.get(n, 0)
        if dsum > 0 and reached > 0:
            closeness[n] = reached / dsum
    return hist, avg_path, diameter, closeness


# ---------------- ③ K-core 剥壳（驱动端，Batagelj-Zaversnik） ----------------
def kcore(adj_map):
    """adj_map: {node: set(nbrs)}。返回 {node: core}（core = kshell）。"""
    deg = {n: len(vs) for n, vs in adj_map.items()}
    nbrs = {n: set(vs) for n, vs in adj_map.items()}
    heap = [(d, n) for n, d in deg.items()]
    heapq.heapify(heap)
    core = {}
    removed = set()
    while heap:
        d, n = heapq.heappop(heap)
        if n in removed:
            continue
        if d > deg[n]:  # 度已更新过 → 惰性删除，按新度重入堆
            heapq.heappush(heap, (deg[n], n))
            continue
        core[n] = d
        removed.add(n)
        for m in nbrs[n]:
            if m not in removed:
                deg[m] -= 1
                heapq.heappush(heap, (deg[m], m))
        nbrs[n] = set()
    return core


# ---------------- ④ Brandes 介数（驱动端，核心子图精确计算） ----------------
def brandes(sub_nodes, adj_map):
    """sub_nodes: 核心子图节点集合；adj_map: 全图邻接。
    返回 {node: 归一化介数}（无向：raw/2 后 × 2/((n-1)(n-2))）。"""
    sub = {n: [m for m in adj_map[n] if m in sub_nodes] for n in sub_nodes}
    n = len(sub)
    bc = dict.fromkeys(sub, 0.0)
    for s in sub:
        # BFS 最短路径计数
        S, P = [], {w: [] for w in sub}
        sigma = dict.fromkeys(sub, 0.0)
        sigma[s] = 1.0
        dist = dict.fromkeys(sub, -1)
        dist[s] = 0
        q = deque([s])
        while q:
            v = q.popleft()
            S.append(v)
            dv = dist[v]
            for w in sub[v]:
                if dist[w] < 0:
                    dist[w] = dv + 1
                    q.append(w)
                if dist[w] == dv + 1:
                    sigma[w] += sigma[v]
                    P[w].append(v)
        # 依赖反向传播
        delta = dict.fromkeys(sub, 0.0)
        while S:
            w = S.pop()
            coef = (1.0 + delta[w]) / sigma[w]
            for v in P[w]:
                delta[v] += sigma[v] * coef
            if w != s:
                bc[w] += delta[w]
    norm = 2.0 / ((n - 1) * (n - 2)) if n > 2 else 0.0
    # 无向图每条最短路在 (s,t)/(t,s) 两次遍历中各计一次 → ÷2 后归一化
    return {v: raw / 2.0 * norm for v, raw in bc.items()}


# ---------------- ⑤ 度分布（Spark） ----------------
def degree_distribution(sc, undirected):
    """返回 [(degree, count)] 升序。"""
    hist = (
        undirected.map(lambda e: (e[0], 1))
        .reduceByKey(lambda a, b: a + b, P)
        .map(lambda kv: (kv[1], 1))
        .reduceByKey(lambda a, b: a + b, P)
        .collect()
    )
    return sorted(hist)


def powerlaw_slope(dist):
    """log-log 最小二乘斜率（k≥2 且 count>0）。返回 None 若样本不足。"""
    xs = [math.log10(k) for k, c in dist if k >= 2 and c > 0]
    ys = [math.log10(c) for k, c in dist if k >= 2 and c > 0]
    if len(xs) < 3:
        return None
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return sxy / sxx if sxx else None


# ---------------- ⑥ 年代演化（Spark） ----------------
def evolution(sc, roles, movie_year):
    """roles: RDD[(actorId, movieId)]；movie_year: {movieId: year}。
    返回按年演化行 list[dict]。"""
    year_b = sc.broadcast(movie_year)

    # 演员出道年：min(出演电影年份)
    debut = (
        roles.map(lambda r: (r[0], year_b.value.get(r[1])))
        .filter(lambda kv: kv[1] is not None)
        .reduceByKey(lambda a, b: min(a, b), P)
    )

    # 合作对首次合作年：同片演员两两配对（携带年份）→ 每对取 min
    def gen_pairs_year(item):
        movie_id, actors = item
        yr = year_b.value.get(movie_id)
        if yr is None:
            return []
        acts = sorted(set(actors))
        n = len(acts)
        return [((acts[i], acts[j]), yr) for i in range(n) for j in range(i + 1, n)]

    pair_first_year = (
        roles.map(lambda r: (r[1], r[0]))  # 换键为 movieId 再分组
        .groupByKey(P)
        .flatMap(gen_pairs_year)
        .reduceByKey(lambda a, b: min(a, b), P)
    )
    edges_per_year = dict(pair_first_year.map(lambda kv: (kv[1], 1)).reduceByKey(lambda a, b: a + b, P).collect())
    actors_per_year = dict(debut.map(lambda kv: (kv[1], 1)).reduceByKey(lambda a, b: a + b, P).collect())

    years = sorted(set(edges_per_year) | set(actors_per_year))
    rows = []
    ce = ca = 0
    for y in years:
        ne, na = edges_per_year.get(y, 0), actors_per_year.get(y, 0)
        ce += ne
        ca += na
        rows.append(
            {
                "year": y,
                "newEdges": ne,
                "newActors": na,
                "cumulativeEdges": ce,
                "cumulativeActors": ca,
            }
        )
    return rows


# ---------------- 巨片（连通分量，驱动端并查集） ----------------
def giant_component(adj_map):
    """返回 (巨片节点 list, 连通分量总数)。"""
    parent = {n: n for n in adj_map}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for u, vs in adj_map.items():
        ru = find(u)
        for v in vs:
            rv = find(v)
            if rv != ru:
                parent[rv] = ru
    groups = {}
    for n in adj_map:
        groups.setdefault(find(n), []).append(n)
    giant = max(groups.values(), key=len)
    return giant, len(groups)


# ---------------- 回写 Neo4j ----------------
def write_back(rows, query, desc, driver):
    batch = common.NEO4J_IMPORT_BATCH
    with driver.session() as s:
        for i in range(0, len(rows), batch):
            s.run(query, rows=rows[i : i + batch]).consume()
    log(f"[write] {desc}: {len(rows)}")


def main():
    conf = (
        SparkConf()
        .setAppName(f"{common.SPARK_APP_NAME}-SNA")
        .setMaster(common.SPARK_MINING_MASTER)
        .set("spark.default.parallelism", str(P))
        .set("spark.local.dir", "d:/bigmaths/_tmp/spark-local")
        .set("spark.network.timeout", "600s")
        .set("spark.task.maxFailures", "4")
        .set("spark.python.worker.reuse", "true")
        .set("spark.python.worker.memory", "512m")
        .set("spark.driver.memory", "2g")
        .set("spark.ui.enabled", "false")
    )
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    coactor = read_coactor()
    n_edges = len(coactor)
    undirected = sc.parallelize(
        [e for u, v, w in coactor for e in ((u, v, w), (v, u, w))]
    ).cache()
    log(f"[load] CO_ACTOR 单向边 {n_edges}")

    # 驱动端邻接表（广播给三角计数；K-core/Brandes/巨片直接用）
    adj_map = {}
    for u, v, _ in coactor:
        adj_map.setdefault(u, set()).add(v)
        adj_map.setdefault(v, set()).add(u)
    nodes = sorted(adj_map)
    n_nodes = len(nodes)
    deg_map = {n: len(vs) for n, vs in adj_map.items()}
    avg_deg = 2 * n_edges / n_nodes
    log(f"[load] 演员 {n_nodes}，平均度 {avg_deg:.2f}")

    # ---- ① 三角计数与聚类系数 ----
    T, node_tri = triangles(sc, undirected, adj_map)
    triples = sum(d * (d - 1) for d in deg_map.values()) / 2  # 长度 2 路径数
    transitivity = 3 * T / triples if triples else 0.0
    local_cc = {}
    for n, d in deg_map.items():
        if d >= 2:
            local_cc[n] = 2 * node_tri.get(n, 0) / (d * (d - 1))
    avg_local_cc = sum(local_cc.values()) / len(local_cc) if local_cc else 0.0
    log(f"[triangle] T={int(T)} 传递性={transitivity:.4f} 平均局部聚类={avg_local_cc:.4f}")

    # ---- 巨片与连通分量（先算：BFS 采样源从巨片中抽取） ----
    # 实测合作网络含 1,155 个连通分量（各国电影圈近乎独立），跨分量无路径；
    # 若在全图随机采样，平均路径会被大量 ~17 人的小分量主导而失真，
    # 故采样源限定巨片（标准做法：小世界指标在最大连通分量上度量）
    giant_nodes, n_components = giant_component(adj_map)
    gc_size, gc_ratio = len(giant_nodes), len(giant_nodes) / n_nodes
    log(f"[component] 连通分量 {n_components} 个，巨片 {gc_size} 人（{gc_ratio:.1%}）")

    # ---- ② 多源位图 BFS（源取自巨片） ----
    rnd = random.Random(42)
    sources = rnd.sample(giant_nodes, min(BFS_SOURCES, gc_size))
    nbr = undirected.map(lambda e: (e[0], e[1])).partitionBy(P).cache()
    hist, avg_path, diameter, closeness = multisource_bfs(sc, nbr, nodes, sources)
    log(f"[bfs] 巨片内采样源={len(sources)} 平均路径={avg_path:.3f} 直径估计={diameter}")

    # ---- ③ K-core ----
    core = kcore(adj_map)
    shell_hist = Counter(core.values())
    max_core = max(core.values())
    core_members = [n for n, c in core.items() if c == max_core]
    log(f"[kcore] max_core={max_core} 核心圈 {len(core_members)} 人，壳层分布 {dict(sorted(shell_hist.items()))}")

    # ---- ④ Brandes 介数（度数 Top-N 诱导子图） ----
    topn = sorted(nodes, key=lambda n: (-deg_map[n], n))[:BRANDES_TOPN]
    sub_set = set(topn)
    bc = brandes(sub_set, adj_map)
    bc_top = sorted(bc.items(), key=lambda kv: -kv[1])[:10]
    name_map = read_actor_names()
    log(f"[brandes] 核心子图 {len(sub_set)} 节点，介数 Top5 = " +
        ", ".join(f"{name_map.get(n, n)}({v:.4f})" for n, v in bc_top[:5]))

    # ---- ⑤ 度分布 ----
    dist = degree_distribution(sc, undirected)
    slope = powerlaw_slope(dist)
    degs_sorted = sorted(deg_map.values(), reverse=True)
    top1 = max(1, n_nodes // 100)
    top1_share = sum(degs_sorted[:top1]) / sum(degs_sorted)
    max_deg = degs_sorted[0]
    log(f"[degree] max={max_deg} Top1%度数份额={top1_share:.3f} log-log斜率={slope and round(slope, 3)}")

    # ---- ⑥ 年代演化 ----
    movie_year = read_movie_years()
    roles = sc.parallelize(
        [(r["actorId"], r["movieId"]) for r in read_csv_rows("edges_acted_in")]
    ).cache()
    evo_rows = evolution(sc, roles, movie_year)
    if evo_rows:
        log(f"[evolution] {evo_rows[0]['year']}~{evo_rows[-1]['year']} "
            f"累计合作对 {evo_rows[-1]['cumulativeEdges']}（应≈{n_edges}）累计演员 {evo_rows[-1]['cumulativeActors']}")

    # ---- 小世界基线（同规模 ER 随机图理论值） ----
    rand_cc = avg_deg / n_nodes
    rand_path = math.log(n_nodes) / math.log(avg_deg) if avg_deg > 1 else 0.0

    summary = {
        "nodes": n_nodes,
        "edges": n_edges,
        "avgDegree": round(avg_deg, 2),
        "density": round(2 * n_edges / (n_nodes * (n_nodes - 1)), 8),
        "triangles": int(T),
        "globalClustering": round(transitivity, 6),
        "avgLocalClustering": round(avg_local_cc, 6),
        "avgPathLength": round(avg_path, 4),
        "diameter": diameter,
        "sampledSources": len(sources),
        "sampledPairs": int(sum(hist.values())),
        "randomBaselineClustering": round(rand_cc, 8),
        "randomBaselineAvgPath": round(rand_path, 4),
        "giantComponent": {
            "size": gc_size,
            "ratio": round(gc_ratio, 6),
            "components": n_components,
        },
        "kcore": {
            "maxCore": max_core,
            "coreSize": len(core_members),
            "shells": {str(k): v for k, v in sorted(shell_hist.items())},
        },
        "degree": {
            "maxDegree": max_deg,
            "top1pctShare": round(top1_share, 6),
            "powerLawSlope": None if slope is None else round(slope, 4),
        },
        "betweenness": {
            "subgraphSize": len(sub_set),
            "top": [
                {"id": n, "name": name_map.get(n, n), "score": round(v, 6)}
                for n, v in bc_top
            ],
        },
    }
    os.makedirs(common.MINING_DIR, exist_ok=True)
    with open(f"{common.MINING_DIR}/sna_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    with open(f"{common.MINING_DIR}/degree_distribution.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["degree", "count"])
        w.writerows(dist)
    with open(f"{common.MINING_DIR}/evolution.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["year", "newEdges", "newActors", "cumulativeEdges", "cumulativeActors"])
        w.writeheader()
        w.writerows(evo_rows)

    # 节点级 SNA 指标（回写 Neo4j + 导出 CSV）
    metrics_rows = []
    for n in nodes:
        metrics_rows.append(
            {
                "actorId": n,
                "triangles": node_tri.get(n, 0),
                "clusteringCoefficient": round(local_cc.get(n, 0.0), 6),
                "kshell": core.get(n, 0),
                "closeness": round(closeness.get(n, 0.0), 6),
                "betweenness": round(bc[n], 6) if n in bc else None,
            }
        )
    with open(f"{common.MINING_DIR}/actor_sna_metrics.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["actorId", "triangles", "clusteringCoefficient", "kshell", "closeness", "betweenness"])
        w.writeheader()
        w.writerows(metrics_rows)
    log("[out] sna_summary.json / degree_distribution.csv / evolution.csv / actor_sna_metrics.csv")

    # ---- 回写 Neo4j ----
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(common.NEO4J_URI, auth=(common.NEO4J_USER, common.NEO4J_PASSWORD))
    with driver.session() as s:
        s.run("CREATE INDEX actor_kshell IF NOT EXISTS FOR (a:Actor) ON (a.kshell)")
        s.run("CREATE INDEX actor_betweenness IF NOT EXISTS FOR (a:Actor) ON (a.betweenness)")
    write_back(
        metrics_rows,
        """
        UNWIND $rows AS r
        MATCH (a:Actor {id: r.actorId})
        SET a.triangles = r.triangles, a.clusteringCoefficient = r.clusteringCoefficient,
            a.kshell = r.kshell, a.closeness = r.closeness
        """,
        "演员 SNA 指标（三角/聚类/K壳/接近度）",
        driver,
    )
    bc_rows = [
        {"actorId": n, "betweenness": round(v, 6)} for n, v in bc.items() if v > 0
    ]
    write_back(
        bc_rows,
        """
        UNWIND $rows AS r
        MATCH (a:Actor {id: r.actorId})
        SET a.betweenness = r.betweenness
        """,
        "介数中心性（核心子图）",
        driver,
    )
    driver.close()
    log("[done] SNA 分析完成")
    sc.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
