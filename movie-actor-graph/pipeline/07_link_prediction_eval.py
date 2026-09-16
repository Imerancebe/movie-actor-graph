r"""07 链接预测对比实验：CN / Jaccard / AA(标准) / AA(√变体) / PA（FR-M11）。

协议（随机隐藏边 / 网络补全，设计规约 3.7）：
- 固定种子随机抽 20% CO_ACTOR 边为测试集并从图中移除，其余 80% 构成训练图
- 候选对 = 训练图中存在 ≥1 共同邻居且非训练边的节点对（邻域类算法适用域）
- 正样本 = 落入候选集的测试边；其余候选对为负样本
- 无共同邻居的测试边对邻域法不可恢复，单独统计占比

五种打分（N(u) 为训练图邻居集）：
  CN      = |N(u)∩N(v)|
  Jaccard = |N(u)∩N(v)| / |N(u)∪N(v)|
  AA-log  = Σ 1/ln(deg(w))（标准 Adamic-Adar）
  AA-sqrt = Σ 1/√deg(w)（05 生产预测器变体，实验顺带验证）
  PA      = deg(u)·deg(v)（优先连接基线）

评估：AUC（同分修正秩和法）+ Precision@100/500。候选对约百万级，
按 score 分桶在 Spark reduceByKey 聚合 (桶大小, 正样本数) 后驱动端遍历计算，
避免全量 collect。

产出：data/mining/prediction_eval.json。
Windows 调优沿用 05/06 踩坑记录（local[4]、SPARK_AUTH_SOCKET_TIMEOUT）。
"""
import os

os.environ.setdefault("SPARK_AUTH_SOCKET_TIMEOUT", "600")

import csv
import glob
import json
import math
import random

from pyspark import SparkConf, SparkContext

import common

P = 4
TEST_RATIO = 0.2
SEED = 42
KS = (100, 500)
TOP_N_EXAMPLES = 10

ALGOS = [
    ("CN", "|N(u)∩N(v)|"),
    ("Jaccard", "|∩|/|∪|"),
    ("AA-log", "Σ1/ln(deg)"),
    ("AA-sqrt", "Σ1/√deg（05生产变体）"),
    ("PA", "deg(u)·deg(v)"),
]


def log(msg):
    print(msg, flush=True)


# ---------------- CSV 读取（同 06） ----------------
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
    return [(r["actorId"], r["otherActorId"], int(r["weight"])) for r in read_csv_rows("edges_coactor")]


def read_actor_names():
    return {r["actorId"]: r["name"] for r in read_csv_rows("nodes_actors")}


# ---------------- 指标计算（驱动端，输入为分桶直方图） ----------------
def auc_precision(hist, ks):
    """hist: {score: (桶大小 n, 正样本数 p)} → (AUC, {k: Precision@k})。
    AUC = [Σ 正样本严格高于的负样本数 + 0.5·同分负样本数] / (n_pos·n_neg)；
    P@K 按分数降序遍历桶，跨越 K 边界的桶按桶内正样本占比折算（无偏估计）。"""
    items = sorted(hist.items(), key=lambda kv: -kv[0])
    n_pos = sum(p for _, (t, p) in items)
    n_neg = sum(t - p for _, (t, p) in items)
    suffix_neg = [0] * (len(items) + 1)
    for i in range(len(items) - 1, -1, -1):
        t, p = items[i][1]
        suffix_neg[i] = suffix_neg[i + 1] + (t - p)
    concordant = 0.0
    for i, (_, (t, p)) in enumerate(items):
        concordant += p * suffix_neg[i + 1] + 0.5 * p * (t - p)
    auc = concordant / (n_pos * n_neg) if n_pos and n_neg else 0.0

    prec = {}
    for k in ks:
        seen, hit = 0, 0.0
        for _, (t, p) in items:
            if seen >= k:
                break
            take = min(t, k - seen)
            hit += p * (take / t)
            seen += take
        prec[k] = hit / k if k else 0.0
    return auc, prec


def main():
    conf = (
        SparkConf()
        .setAppName(f"{common.SPARK_APP_NAME}-LinkPredEval")
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
    names = read_actor_names()
    n_edges = len(coactor)
    log(f"[load] CO_ACTOR 边 {n_edges}")

    # ---- 数据划分（固定种子可复现）----
    order = list(range(n_edges))
    random.Random(SEED).shuffle(order)
    n_test = int(n_edges * TEST_RATIO)
    test_ids = set(order[:n_test])
    train = [coactor[i] for i in range(n_edges) if i not in test_ids]
    test = [coactor[i] for i in test_ids]
    log(f"[split] 训练边 {len(train)} / 测试边 {len(test)}（seed={SEED}）")

    adj = {}
    for u, v, _ in train:
        adj.setdefault(u, set()).add(v)
        adj.setdefault(v, set()).add(u)
    deg = {n: len(vs) for n, vs in adj.items()}
    test_pairs = {(u, v) if u < v else (v, u) for u, v, _ in test}
    train_pairs = {(u, v) if u < v else (v, u) for u, v, _ in train}

    adj_b = sc.broadcast(adj)
    deg_b = sc.broadcast(deg)
    test_b = sc.broadcast(test_pairs)
    train_b = sc.broadcast(train_pairs)

    # ---- 候选对：共同邻居两两配对，去重后剔除训练边 ----
    def gen_pairs(w):
        nbrs = sorted(adj_b.value[w])
        for i in range(len(nbrs)):
            for j in range(i + 1, len(nbrs)):
                a, b = nbrs[i], nbrs[j]
                yield (a, b) if a < b else (b, a)

    candidates = (
        sc.parallelize(sorted(adj), P)
        .flatMap(gen_pairs)
        .distinct()
        .filter(lambda p: p not in train_b.value)
        .cache()
    )
    n_cand = candidates.count()
    n_pos = candidates.filter(lambda p: p in test_b.value).count()
    n_neg = n_cand - n_pos
    log(f"[cand] 候选对 {n_cand}（正 {n_pos} / 负 {n_neg}），"
        f"测试边可恢复率 {n_pos / len(test_pairs):.1%}")

    # ---- 打分：一次计算五算法分值 ----
    def score_pair(p):
        u, v = p
        nu = adj_b.value[u]
        nv = adj_b.value[v]
        common = nu & nv
        cn = len(common)
        union = len(nu) + len(nv) - cn
        dg = deg_b.value
        aa_log = sum(1.0 / math.log(dg[w]) for w in common)
        aa_sqrt = sum(1.0 / math.sqrt(dg[w]) for w in common)
        pa = dg[u] * dg[v]
        label = 1 if p in test_b.value else 0
        return (cn, label), (cn / union if union else 0.0, label), \
               (aa_log, label), (aa_sqrt, label), (pa, label)

    # ---- 各算法分桶直方图 → 驱动端算 AUC / P@K ----
    results = []
    for i, (name, formula) in enumerate(ALGOS):
        hist = (
            candidates.map(lambda p: score_pair(p)[i])
            .map(lambda sl: (sl[0], (1, sl[1])))
            .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]), P)
            .collectAsMap()
        )
        auc, prec = auc_precision(hist, KS)
        results.append({"name": name, "formula": formula, "auc": round(auc, 4),
                        "precisionAt": {str(k): round(prec[k], 4) for k in KS}})
        log(f"[eval] {name:<8} AUC={auc:.4f}  P@{KS[0]}={prec[KS[0]]:.3f}  P@{KS[1]}={prec[KS[1]]:.3f}")

    best = max(results, key=lambda r: r["auc"])
    best_idx = [r["name"] for r in results].index(best["name"])

    # ---- 最优算法 Top-N 新合作预测（负样本中得分最高者）----
    top = (
        candidates.map(lambda p: (score_pair(p)[best_idx], p))
        .filter(lambda t: t[0][1] == 0)
        .takeOrdered(TOP_N_EXAMPLES, key=lambda t: -t[0][0])
    )
    examples = []
    for (score, _), (u, v) in top:
        common_nb = adj[u] & adj[v]
        examples.append({
            "a": {"id": u, "name": names.get(u, u)},
            "b": {"id": v, "name": names.get(v, v)},
            "score": round(score, 4),
            "commonNeighbors": [names.get(w, w) for w in sorted(common_nb, key=lambda x: -deg[x])[:3]],
            "commonCount": len(common_nb),
        })

    out = {
        "protocol": {
            "totalEdges": n_edges,
            "trainEdges": len(train),
            "testEdges": len(test_pairs),
            "candidatePairs": n_cand,
            "positivePairs": n_pos,
            "negativePairs": n_neg,
            "recoverableRatio": round(n_pos / len(test_pairs), 4),
            "testRatio": TEST_RATIO,
            "seed": SEED,
        },
        "algorithms": results,
        "bestAlgorithm": best["name"],
        "topPredictions": examples,
    }
    out_path = os.path.join(common.MINING_DIR, "prediction_eval.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    log(f"[out] {out_path}")
    log("[done] 链接预测对比实验完成")
    sc.stop()


if __name__ == "__main__":
    main()
