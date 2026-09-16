"""小图冒烟测试：验证 PageRank / LPA / 中心性 / AA 预测的算法正确性。"""
import importlib.util
import sys

from pyspark import SparkConf, SparkContext

sys.path.insert(0, r"d:\bigmaths\movie-actor-graph\pipeline")
spec = importlib.util.spec_from_file_location("mining", r"d:\bigmaths\movie-actor-graph\pipeline\05_graph_mining.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

conf = (
    SparkConf()
    .setAppName("smoke")
    .setMaster("local[2]")
    .set("spark.default.parallelism", "4")
    .set("spark.local.dir", "d:/bigmaths/_tmp/spark-local")
    .set("spark.ui.enabled", "false")
)
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 两个三角形由弱桥相连：A-B-C / D-E-F，桥 C-D
E = [
    ("A", "B", 2), ("B", "C", 2), ("C", "A", 2),
    ("D", "E", 2), ("E", "F", 2), ("F", "D", 2),
    ("C", "D", 1),
]
und = sc.parallelize(E).flatMap(lambda e: [e, (e[1], e[0], e[2])]).cache()

pr = dict(m.pagerank(sc, und).collect())
print("PR:", {k: round(v, 3) for k, v in sorted(pr.items())})
assert abs(sum(pr.values()) / len(pr) - 1.0) < 1e-9, "PR 均值应≈1"
assert pr["C"] > pr["A"] and pr["D"] > pr["E"], "桥节点 C/D 声望应更高"

comm = dict(m.lpa(sc, und).collect())
print("COMM:", comm)
# 弱桥两侧应分裂为 2 个社区；tie-break 取最小 id，结果确定
assert len(set(comm.values())) == 2, f"应得 2 个社区，实际 {sorted(set(comm.values()))}"
assert comm["A"] == comm["B"] == comm["C"], "三角形 A-B-C 应同社区"
assert comm["D"] == comm["E"] == comm["F"], "三角形 D-E-F 应同社区"
assert comm["A"] != comm["D"], "弱桥两侧应分属不同社区"

cent = dict(m.centrality(sc, und).collect())
print("CENT:", cent)
assert cent["C"][0] == 3 and cent["A"][0] == 2, "度数应为 C=3, A=2"

deg_map = {k: v[0] for k, v in cent.items()}
topn = dict(m.predict(sc, und, deg_map).collect())
print("PRED:", topn)
# A 的 2 跳：D（经 C）。AA(A,D) = 1/sqrt(deg(C)) = 1/sqrt(3)
pa = [x for x in topn.get("A", []) if x[0] == "D"]
assert pa, "A 的候选应含 D"
aa_expect = 1 / 3 ** 0.5
assert abs(pa[0][1] - aa_expect) < 1e-9, f"AA(A,D) 应为 {aa_expect:.4f}，实际 {pa[0][1]}"
# 已合作对不应出现：A 的候选不能有 B 或 C
cands = {x[0] for x in topn.get("A", [])}
assert "B" not in cands and "C" not in cands, "已合作对应被过滤"

sc.stop()
print("SMOKE PASS")
