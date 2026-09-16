r"""08 演员图嵌入：加权随机游走 + PPMI 共现矩阵 + 截断 SVD（FR-M12）。

DeepWalk 的矩阵分解视角（设计规约 3.8），全程自主实现（SVD 为线性代数原语）：
1. 度数 Top-5000 演员诱导子图（嵌入语义在核心结构上最有意义，
   5000×5000 共现矩阵规模可分解）
2. 加权随机游走：每节点 6 条 × 长 50，下一跳概率 ∝ 合作次数（bisect 采样）
3. PPMI 共现矩阵：窗口 3 共现计数（NumPy 向量化编码 + np.unique 聚合），
   PPMI = max(0, log(p(i,j) / (p(i)·p(j))))
4. scipy.sparse.linalg.svds(k=64)，U·√S → 64 维嵌入，行归一化后余弦 = 点积

三个应用：
- PCA 降维散点（按度数采样 2000 点，颜色 = LPA 社区，检验嵌入是否恢复社区结构）
- 相似演员推荐：余弦最近邻 Top-10（embedding_neighbors.csv）
- 嵌入法 vs AA 链接预测对比：复用 07 协议（同 seed/候选集），子图外候选得 0 分，
  另报核心子图内 AUC

产出：data/mining/{actor_embeddings.csv, embedding_scatter.csv,
embedding_neighbors.csv, embedding_summary.json}
"""
import csv
import glob
import json
import math
import os
import random
from bisect import bisect_right

import numpy as np
from scipy import sparse
from scipy.sparse.linalg import svds

import common

TOP_N = 5000
DIM = 64
WALKS_PER_NODE = 6
WALK_LEN = 50
WINDOW = 3
SEED = 42  # 与 07 一致（链接预测对比须同划分）
NEIGHBOR_TOPK = 10
SCATTER_SAMPLE = 2000
TEST_RATIO = 0.2

ACTED_IN_DIR = os.path.join(common.GRAPH_DIR, "edges_acted_in")


def log(msg):
    print(msg, flush=True)


def read_csv_rows(dirname):
    files = [f for f in glob.glob(os.path.join(dirname, "part-*.csv")) if not f.endswith(".crc")]
    if not files:
        raise FileNotFoundError(f"{dirname} 缺少 part-*.csv")
    rows = []
    for fp in files:
        with open(fp, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append(r)
    return rows


def main():
    rnd = random.Random(SEED)
    np_rng = np.random.default_rng(SEED)

    coactor = [(r["actorId"], r["otherActorId"], int(r["weight"]))
               for r in read_csv_rows(os.path.join(common.GRAPH_DIR, "edges_coactor"))]
    actors = {r["actorId"]: r["name"] for r in read_csv_rows(os.path.join(common.GRAPH_DIR, "nodes_actors"))}
    log(f"[load] CO_ACTOR 边 {len(coactor)}，演员 {len(actors)}")

    # ---- 度数排序取核心子图 ----
    deg = {}
    for u, v, _ in coactor:
        deg[u] = deg.get(u, 0) + 1
        deg[v] = deg.get(v, 0) + 1
    top = sorted(deg, key=lambda x: -deg[x])[:TOP_N]
    top_set = set(top)
    sub_edges = [(u, v, w) for u, v, w in coactor if u in top_set and v in top_set]
    idx = {a: i for i, a in enumerate(top)}

    nbr_w = [[] for _ in range(len(top))]  # [(邻居索引, 权重)]
    for u, v, w in sub_edges:
        nbr_w[idx[u]].append((idx[v], w))
        nbr_w[idx[v]].append((idx[u], w))
    # 预计算游走采样表：累积权重 + bisect
    cum_tables = []
    for lst in nbr_w:
        ids = [i for i, _ in lst]
        cw = np.cumsum([w for _, w in lst], dtype=np.float64)
        cum_tables.append((ids, cw))
    n_sub_edges = len(sub_edges)
    log(f"[subgraph] 核心子图 {len(top)} 节点 / {n_sub_edges} 边（Top-{TOP_N} 度数）")

    # ---- 加权随机游走（转移概率 ∝ 合作次数）----
    walks = []
    for start in range(len(top)):
        for _ in range(WALKS_PER_NODE):
            walk = [start]
            cur = start
            for _ in range(WALK_LEN - 1):
                ids, cw = cum_tables[cur]
                if not ids:
                    break
                x = rnd.random() * cw[-1]
                cur = ids[bisect_right(cw, x)]
                walk.append(cur)
            walks.append(walk)
    total_steps = sum(len(w) for w in walks)
    log(f"[walk] {len(walks)} 条游走 / {total_steps} 步")

    # ---- 窗口共现计数（NumPy 向量化：pair 编码 + np.unique）----
    codes_list = []
    for walk in walks:
        arr = np.asarray(walk, dtype=np.int64)
        for d in range(1, WINDOW + 1):
            if len(arr) <= d:
                break
            codes_list.append(arr[:-d] * len(top) + arr[d:])
    codes = np.concatenate(codes_list)
    uniq, counts = np.unique(codes, return_counts=True)
    log(f"[ppmi] 共现对 {len(uniq)} 种 / {int(counts.sum())} 次")

    rows_i = (uniq // len(top)).astype(np.int64)
    cols_j = (uniq % len(top)).astype(np.int64)
    total = counts.sum()
    row_marg = np.zeros(len(top))
    col_marg = np.zeros(len(top))
    np.add.at(row_marg, rows_i, counts)
    np.add.at(col_marg, cols_j, counts)
    p_ij = counts / total
    p_i = row_marg[rows_i] / total
    p_j = col_marg[cols_j] / total
    pmi = np.log(p_ij / (p_i * p_j))
    ppmi = np.maximum(pmi, 0.0)
    keep = ppmi > 0
    M = sparse.csr_matrix((ppmi[keep], (rows_i[keep], cols_j[keep])),
                          shape=(len(top), len(top)))
    log(f"[ppmi] 正 PPMI 条目 {int(keep.sum())}")

    # ---- 截断 SVD → 嵌入 ----
    U, S, _ = svds(M.astype(np.float64), k=min(DIM, min(M.shape) - 1))
    order = np.argsort(-S)
    U, S = U[:, order], S[order]
    emb = U * np.sqrt(S)
    norms = np.linalg.norm(emb, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    emb = emb / norms
    log(f"[svd] 嵌入 {emb.shape[0]}×{emb.shape[1]}，奇异值 Top5: "
        + ", ".join(f"{s:.2f}" for s in S[:5]))

    # ---- 应用 1：PCA 降维散点（颜色 = LPA 社区，从 Neo4j 读）----
    centered = emb - emb.mean(axis=0)
    Vy, Sy, _ = np.linalg.svd(centered, full_matrices=False)
    pc = (centered @ Vy[:2].T)
    evr = float((Sy[:2] ** 2).sum() / (Sy ** 2).sum())

    community = {}
    try:
        from neo4j import GraphDatabase
        drv = GraphDatabase.driver("bolt://localhost:7687",
                                   auth=("neo4j", os.environ.get("NEO4J_PASSWORD", "bigdata2026")))
        with drv.session() as s:
            for r in s.run("MATCH (a:Actor) WHERE a.community IS NOT NULL "
                           "RETURN a.id AS id, a.community AS c"):
                community[r["id"]] = r["c"]
        drv.close()
    except Exception as e:
        log(f"[warn] Neo4j 社区读取失败（散点不带社区色）: {e}")

    sample_idx = np.argsort(-np.array([deg[a] for a in top]))[:SCATTER_SAMPLE]
    scatter_rows = []
    for i in sample_idx:
        a = top[i]
        scatter_rows.append({
            "actorId": a, "name": actors.get(a, a),
            "x": round(float(pc[i, 0]), 4), "y": round(float(pc[i, 1]), 4),
            "community": community.get(a, -1),
            "degree": deg[a],
        })
    out = os.path.join(common.MINING_DIR, "embedding_scatter.csv")
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(scatter_rows[0].keys()))
        w.writeheader()
        w.writerows(scatter_rows)
    log(f"[out] {out}（{len(scatter_rows)} 点，PCA 方差解释率 {evr:.1%}）")

    # ---- 应用 2：相似演员 Top-K（余弦 = 单位向量点积）----
    sim = (emb @ emb.T).astype(np.float32)
    np.fill_diagonal(sim, -1.0)
    full_nbr = {a: [j for j, _ in nbr_w[i]] for a, i in ((a, idx[a]) for a in top)}
    nbr_rows = []
    for i in range(len(top)):
        topk = np.argpartition(-sim[i], NEIGHBOR_TOPK)[:NEIGHBOR_TOPK]
        topk = topk[np.argsort(-sim[i][topk])]
        a = top[i]
        aset = set(full_nbr[a])
        for j in topk:
            b = top[j]
            nbr_rows.append({
                "actorId": a, "neighborId": b, "sim": round(float(sim[i, j]), 4),
                "commonNeighbors": len(aset & set(full_nbr[b])),
            })
    out = os.path.join(common.MINING_DIR, "embedding_neighbors.csv")
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["actorId", "neighborId", "sim", "commonNeighbors"])
        w.writeheader()
        w.writerows(nbr_rows)
    log(f"[out] {out}（{len(top)} 人 × Top{NEIGHBOR_TOPK}）")

    # ---- 嵌入向量导出 ----
    out = os.path.join(common.MINING_DIR, "actor_embeddings.csv")
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["actorId"] + [f"d{k}" for k in range(emb.shape[1])])
        for i, a in enumerate(top):
            w.writerow([a] + [f"{v:.6f}" for v in emb[i]])
    log(f"[out] {out}")

    # ---- 应用 3：嵌入法 vs AA 链接预测对比（复用 07 协议）----
    order_ids = list(range(len(coactor)))
    random.Random(SEED).shuffle(order_ids)
    n_test = int(len(coactor) * TEST_RATIO)
    test_ids = set(order_ids[:n_test])
    train = [coactor[k] for k in range(len(coactor)) if k not in test_ids]
    test_pairs = {(u, v) if u < v else (v, u) for u, v, _ in
                  [coactor[k] for k in test_ids]}
    train_pairs = {(u, v) if u < v else (v, u) for u, v, _ in train}

    adj = {}
    for u, v, _ in train:
        adj.setdefault(u, set()).add(v)
        adj.setdefault(v, set()).add(u)

    cand = set()
    for w_ in adj:
        ns = sorted(adj[w_])
        for i in range(len(ns)):
            for j in range(i + 1, len(ns)):
                p = (ns[i], ns[j]) if ns[i] < ns[j] else (ns[j], ns[i])
                if p not in train_pairs:
                    cand.add(p)
    pos = sum(1 for p in cand if p in test_pairs)
    log(f"[lp] 候选对 {len(cand)}（正 {pos}，与 07 同 seed 应一致）")

    e_idx = {a: i for i, a in enumerate(top)}

    def scores():
        for u, v in cand:
            nu, nv = adj.get(u, set()), adj.get(v, set())
            common = nu & nv
            aa = sum(1.0 / math.log(max(len(adj.get(w_, ())), 2)) for w_ in common)
            if u in e_idx and v in e_idx:
                cos = float(emb[e_idx[u]] @ emb[e_idx[v]])
                in_core = 1
            else:
                cos = 0.0
                in_core = 0
            label = 1 if (u, v) in test_pairs else 0
            yield aa, cos, label, in_core

    def auc_from(pairs):
        """同分修正秩和法 AUC（与 07 的分桶实现一致）。"""
        hist = {}
        for s, l in pairs:
            b = hist.setdefault(s, [0, 0])
            b[0] += 1
            if l:
                b[1] += 1
        items = sorted(hist.items(), key=lambda kv: -kv[0])
        n_pos = sum(p for _, (t, p) in items)
        n_neg = sum(t - p for _, (t, p) in items)
        if not n_pos or not n_neg:
            return 0.0
        suffix_neg = [0] * (len(items) + 1)
        for i in range(len(items) - 1, -1, -1):
            t, p = items[i][1]
            suffix_neg[i] = suffix_neg[i + 1] + (t - p)
        auc = 0.0
        for i, (_, (t, p)) in enumerate(items):
            auc += p * suffix_neg[i + 1] + 0.5 * p * (t - p)
        return auc / (n_pos * n_neg)

    aa_all, cos_all, aa_core, cos_core = [], [], [], []
    for aa, cos, label, in_core in scores():
        aa_all.append((aa, label))
        cos_all.append((cos, label))
        if in_core:
            aa_core.append((aa, label))
            cos_core.append((cos, label))
    auc_aa = auc_from(aa_all)
    auc_cos = auc_from(cos_all)
    auc_aa_core = auc_from(aa_core)
    auc_cos_core = auc_from(cos_core)
    coverage = len(aa_core) / len(aa_all) if aa_all else 0
    log(f"[lp] AA AUC={auc_aa:.4f} | 嵌入 AUC={auc_cos:.4f}（全候选，覆盖外记 0 分）")
    log(f"[lp] 核心子图内：AA AUC={auc_aa_core:.4f} | 嵌入 AUC={auc_cos_core:.4f}（覆盖率 {coverage:.1%}）")

    summary = {
        "algorithm": "DeepWalk-MF（加权游走 + PPMI + 截断SVD，自主实现）",
        "dim": DIM, "topN": TOP_N, "walksPerNode": WALKS_PER_NODE,
        "walkLength": WALK_LEN, "window": WINDOW, "seed": SEED,
        "subgraphNodes": len(top), "subgraphEdges": n_sub_edges,
        "ppmiEntries": int(keep.sum()),
        "pcaExplainedVariance": round(evr, 4),
        "linkPrediction": {
            "candidates": len(cand), "positives": pos,
            "coverageOfCore": round(coverage, 4),
            "aaAuc": round(auc_aa, 4), "embeddingAuc": round(auc_cos, 4),
            "aaAucInCore": round(auc_aa_core, 4),
            "embeddingAucInCore": round(auc_cos_core, 4),
        },
    }
    out = os.path.join(common.MINING_DIR, "embedding_summary.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    log(f"[out] {out}")
    log("[done] 演员图嵌入完成")


if __name__ == "__main__":
    main()
