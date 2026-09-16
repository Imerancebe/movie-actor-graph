r"""05 图挖掘：Spark 自主实现 PageRank / LPA 社区发现 / 中心性 / 合作预测（FR-M3~M6）。

设计规约第 3 章的算法在此落地，全部基于 RDD 手写迭代实现（不调用图计算库）：

1. 加权 PageRank（无向 CO_ACTOR 图，边权=合作电影数）
   PR(v) = (1-d)/N + d * Σ_{u∈N(v)} PR(u) * w(u,v) / W(u)，W(u)=Σ_{v∈N(u)} w(u,v)
2. 标签传播 LPA：邻居边权投票，平局取最小 id，收敛后重编号 0..K-1
3. 度中心性：合作者数（degree）与加权度（weightedDegree）
4. Adamic-Adar 合作预测：score(u,v) = Σ_{w∈N(u)∩N(v)} 1/sqrt(deg(w))，
   附 Jaccard 系数；过滤已合作对；每演员取 Top-N
结果回写 Neo4j 节点属性，并导出 CSV 到 data/mining/。

Windows 调优（踩坑记录）：
- master 用 local[4]：local[*] 在 16 核机器上同时拉起 16 个 Python worker，
  曾出现全部 worker 闲置、JVM 无限等待的死锁；
- worker 与 JVM 的套接字读超时默认仅 15s（pyspark java_gateway 的
  SPARK_AUTH_SOCKET_TIMEOUT），内存紧张时 JVM 长 GC 停顿会使 worker 抛
  TimeoutError 且 local 模式下单次任务失败即中止作业——启动前需设
  SPARK_AUTH_SOCKET_TIMEOUT=600；同时 C 盘需保留页面文件空间；
- 所有迭代算法用「预分区 + 分区器继承」：静态消息 RDD 与标签 RDD 统一
  partitionBy(P)，mapValues 保持分区器，使 join 不再反复 shuffle 大 RDD，
  每轮迭代仅 1 次 shuffle（原来约 7 次）。
"""
import csv
import glob
import math
import os
import sys

from pyspark import SparkConf, SparkContext

import common

P = 4  # 分区数 = worker 并发，与 SPARK_MINING_MASTER 匹配


def log(msg):
    print(msg, flush=True)


# ---------------- CSV 读取（Spark 输出的 part 文件） ----------------
def read_edges():
    files = glob.glob(os.path.join(common.GRAPH_DIR, "edges_coactor", "part-*.csv"))
    if not files:
        raise FileNotFoundError(f"{common.GRAPH_DIR}/edges_coactor 缺少 part-*.csv")
    rows = []
    with open(files[0], encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append((r["actorId"], r["otherActorId"], int(r["weight"])))
    return rows


def read_actor_names():
    files = glob.glob(os.path.join(common.GRAPH_DIR, "nodes_actors", "part-*.csv"))
    if not files:
        raise FileNotFoundError(f"{common.GRAPH_DIR}/nodes_actors 缺少 part-*.csv")
    names = {}
    with open(files[0], encoding="utf-8") as f:
        for r in csv.DictReader(f):
            names[r["actorId"]] = r["name"]
    return names


# ---------------- 1. 加权 PageRank ----------------
def pagerank(sc, edges):
    """edges: RDD[(u, v, w)] 已含双向。返回 RDD[(node, pr)]（均值归一化到 1）。"""
    n = edges.map(lambda e: e[0]).distinct().count()
    d = common.PAGERANK_DAMPING

    # W(u) = Σ w(u,·)，reduceByKey 产生 HashPartitioner(P)
    wsum = edges.map(lambda e: (e[0], e[2])).reduceByKey(lambda a, b: a + b, P).cache()

    # 消息 (u, (v, p))：u 以权重占比 p = w(u,v)/W(u) 把自身声望传给 v。
    # partitionBy(P) 一次性落位，此后各轮 join 均免 shuffle
    msgs = (
        edges.map(lambda e: (e[0], (e[1], e[2])))
        .join(wsum)
        .map(lambda kv: (kv[0], (kv[1][0][0], kv[1][0][1] / kv[1][1])))
        .partitionBy(P)
        .cache()
    )

    pr = wsum.mapValues(lambda _: 1.0 / n).cache()
    for it in range(1, common.PAGERANK_MAX_ITER + 1):
        # msgs 与 pr 分区器相同 → join 零 shuffle；map 换键后 reduceByKey 是每轮唯一 shuffle
        contrib = (
            msgs.join(pr)  # (u, ((v, p), pr_u))
            .map(lambda kv: (kv[1][0][0], kv[1][1] * kv[1][0][1]))  # (v, pr_u * p)
            .reduceByKey(lambda a, b: a + b, P)
        )
        new_pr = contrib.mapValues(lambda c: (1 - d) / n + d * c).cache()
        delta = new_pr.join(pr).map(lambda kv: abs(kv[1][0] - kv[1][1])).sum() if it > 1 else math.inf
        pr.unpersist()
        pr = new_pr
        log(f"[pagerank] iter={it} delta={delta:.3e}")
        if delta < common.PAGERANK_TOL:
            log(f"[pagerank] 收敛于第 {it} 轮")
            break
    total = pr.map(lambda kv: kv[1]).sum()
    return pr.mapValues(lambda x: x / total * n)  # 归一化使均值≈1，便于阅读


# ---------------- 2. 标签传播 LPA ----------------
def lpa(sc, edges):
    """edges: RDD[(u, v, w)] 已含双向。返回 RDD[(node, communityId)]（连续编号）。"""
    # 邻接消息 (u, (v, w))，一次落位
    edge_msgs = edges.map(lambda e: (e[0], (e[1], e[2]))).partitionBy(P).cache()
    # edge_msgs 中每个节点 u 有 deg(u) 行，必须 reduceByKey 去重成每节点 1 行；
    # 否则 labels 键重复 → join 膨胀为 Σdeg²（实测 218 万行），且投票权重被邻居度数扭曲
    labels = (
        edge_msgs.map(lambda kv: (kv[0], kv[0]))
        .reduceByKey(lambda a, b: a, P)
        .cache()
    )

    def pick(pairs):
        best_w, best_l = -1.0, None
        for label, w in pairs:
            if w > best_w or (w == best_w and (best_l is None or label < best_l)):
                best_w, best_l = w, label
        return best_l

    for it in range(1, common.LPA_MAX_ITER + 1):
        new_labels = (
            edge_msgs.join(labels)  # (u, ((v, w), label_u))，同分区器零 shuffle
            .map(lambda kv: (kv[1][0][0], (kv[1][1], kv[1][0][1])))  # (v, (label_u, w))
            .groupByKey(P)  # 每轮唯一 shuffle
            .mapValues(pick)
            .join(labels)
            # 取 min(新标签, 旧标签) 保证标签单调递减，避免双社区震荡
            .mapValues(lambda x: min(x[0], x[1]))
            .cache()
        )
        changed = new_labels.join(labels).filter(lambda kv: kv[1][0] != kv[1][1]).count()
        labels.unpersist()
        labels = new_labels
        log(f"[lpa] iter={it} changed={changed}")
        if changed == 0:
            log(f"[lpa] 收敛于第 {it} 轮")
            break

    ordered = labels.map(lambda kv: kv[1]).distinct().sortBy(lambda x: x).zipWithIndex().collectAsMap()
    return labels.mapValues(lambda lb: ordered[lb])


# ---------------- 3. 度中心性 ----------------
def centrality(sc, edges):
    """edges 双向。返回 RDD[(node, (degree, weightedDegree))]。"""
    deg = edges.map(lambda e: (e[0], 1)).reduceByKey(lambda a, b: a + b, P)
    wdeg = edges.map(lambda e: (e[0], e[2])).reduceByKey(lambda a, b: a + b, P)
    return deg.join(wdeg).mapValues(lambda x: (x[0], x[1]))


# ---------------- 4. Adamic-Adar 合作预测 ----------------
def predict(sc, undirected, deg_map):
    """undirected: RDD[(u, v, w)] 双向；deg_map: dict node->degree（广播）。
    返回 RDD[(u, [(v, aa, jaccard, commonCount), ...Top N])]"""
    deg_b = sc.broadcast(deg_map)

    nbr = undirected.map(lambda e: (e[0], e[1])).partitionBy(P).cache()

    def emit(kv):
        # (w, (u, v))：w 为公共邻居；只保留 u<v 一种方向避免成对重复
        w, (u, v) = kv
        if u >= v:
            return []
        return [((u, v), (1.0 / math.sqrt(deg_b.value.get(w, 1)), 1))]

    # 自 join（同分区器零 shuffle）+ reduceByKey 聚合出每对的 (AA 累计, 共同邻居数)
    pair_scores = (
        nbr.join(nbr)
        .flatMap(emit)
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]), P)
        .cache()
    )

    # 过滤已直接合作的对
    direct = (
        undirected.filter(lambda e: e[0] < e[1])
        .map(lambda e: ((e[0], e[1]), None))
        .partitionBy(P)
        .cache()
    )
    cand = pair_scores.subtractByKey(direct).cache()

    def to_items(kv):
        (u, v), (aa, cnt) = kv
        du = deg_b.value.get(u, 1)
        dv = deg_b.value.get(v, 1)
        jac = cnt / (du + dv - cnt) if (du + dv - cnt) > 0 else 0.0
        return [(u, (v, aa, jac, cnt)), (v, (u, aa, jac, cnt))]

    # 注意：lambda 内不得引用模块级变量（如 common.PREDICT_TOP_N）——
    # cloudpickle 会按引用序列化模块，worker 端 import 失败导致任务反复重试
    top_n = common.PREDICT_TOP_N
    return (
        cand.flatMap(to_items)
        .groupByKey(P)
        .mapValues(lambda items: sorted(items, key=lambda x: (-x[1], -x[2], x[0]))[:top_n])
    )


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
        .setAppName(f"{common.SPARK_APP_NAME}-Mining")
        .setMaster(common.SPARK_MINING_MASTER)
        # Windows 下 Python worker 进程启动慢，限制并发避免 socket 超时；
        # shuffle 溢写目录必须指向 D 盘（C 盘空间不足会触发 IO 超时）
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

    coactor = sc.parallelize(read_edges()).cache()
    undirected = coactor.flatMap(lambda e: [e, (e[1], e[0], e[2])]).cache()
    log(f"[load] CO_ACTOR 单向边 {coactor.count()}")

    # ---- PageRank ----
    pr = pagerank(sc, undirected).cache()
    log(f"[pagerank] Top5 = {pr.takeOrdered(5, key=lambda kv: -kv[1])}")

    # ---- LPA 社区 ----
    comm = lpa(sc, undirected).cache()
    n_comm = comm.map(lambda kv: kv[1]).distinct().count()
    log(f"[lpa] 社区数 = {n_comm}")

    # ---- 中心性 ----
    cent = centrality(sc, undirected).cache()

    # ---- 合并指标 ----
    metrics = (
        pr.join(cent)
        .map(lambda kv: (kv[0], {"pagerank": round(kv[1][0], 6), "degree": kv[1][1][0], "weightedDegree": kv[1][1][1]}))
        .join(comm)
        .map(lambda kv: {"actorId": kv[0], **kv[1][0], "community": kv[1][1]})
    ).cache()
    metrics_rows = metrics.collect()
    log(f"[metrics] 演员指标 {len(metrics_rows)} 条")

    # ---- 合作预测 ----
    deg_map = {r["actorId"]: r["degree"] for r in metrics_rows}
    name_map = read_actor_names()
    topn = predict(sc, undirected, deg_map).cache()
    pred_pairs = topn.collect()
    pred_rows = []
    for u, items in pred_pairs:
        for v, aa, jac, nc in items:
            pred_rows.append(
                {
                    "actorId": u,
                    "actorName": name_map.get(u, u),
                    "candidateId": v,
                    "candidateName": name_map.get(v, v),
                    "adamicAdar": round(aa, 4),
                    "jaccard": round(jac, 4),
                    "commonNeighbors": nc,
                }
            )
    log(f"[predict] 覆盖演员 {len(pred_pairs)}，预测记录 {len(pred_rows)}")

    # ---- 导出 CSV ----
    os.makedirs(common.MINING_DIR, exist_ok=True)
    with open(f"{common.MINING_DIR}/actor_metrics.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["actorId", "pagerank", "degree", "weightedDegree", "community"])
        w.writeheader()
        w.writerows(metrics_rows)
    with open(f"{common.MINING_DIR}/predictions.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["actorId", "actorName", "candidateId", "candidateName", "adamicAdar", "jaccard", "commonNeighbors"],
        )
        w.writeheader()
        w.writerows(pred_rows)
    log("[out] data/mining/actor_metrics.csv / predictions.csv")

    # ---- 回写 Neo4j ----
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(common.NEO4J_URI, auth=(common.NEO4J_USER, common.NEO4J_PASSWORD))
    write_back(
        metrics_rows,
        """
        UNWIND $rows AS r
        MATCH (a:Actor {id: r.actorId})
        SET a.pagerank = r.pagerank, a.degree = r.degree,
            a.weightedDegree = r.weightedDegree, a.community = r.community
        """,
        "演员指标回写",
        driver,
    )
    driver.close()
    log("[done] 挖掘完成")
    sc.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
