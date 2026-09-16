"""集中配置：路径、数据圈定参数、Neo4j 连接参数。"""
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---- 路径 ----
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
GRAPH_DIR = os.path.join(PROCESSED_DIR, "graph")
MINING_DIR = os.path.join(BASE_DIR, "data", "mining")
for d in (RAW_DIR, PROCESSED_DIR, GRAPH_DIR, MINING_DIR):
    os.makedirs(d, exist_ok=True)

# ---- IMDb 公开数据集 ----
IMDB_BASE = "https://datasets.imdbws.com"
RAW_FILES = {
    "name.basics.tsv.gz": os.path.join(RAW_DIR, "name.basics.tsv.gz"),
    "title.basics.tsv.gz": os.path.join(RAW_DIR, "title.basics.tsv.gz"),
    "title.principals.tsv.gz": os.path.join(RAW_DIR, "title.principals.tsv.gz"),
}

# ---- 数据圈定策略（设计规约 2.3）----
TOP_MOVIES = 2500          # 按热度取前 N 部电影
MAX_ACTORS_PER_MOVIE = 15  # 每部电影只取 principals.ordering 前 N 位主演
MIN_NODE_TARGET = 10000    # 课程规模门槛：节点
MIN_EDGE_TARGET = 30000    # 课程规模门槛：关系边

# ---- Spark ----
SPARK_APP_NAME = "MovieActorGraph"
SPARK_MASTER = "local[*]"  # 可切换 yarn/standanlone 集群（NFR-4）
# RDD 迭代计算（05 图挖掘）专用：Windows 下 local[*] 会同时拉起全部核数的
# Python worker 进程，管道竞争易死锁；16GB 内存 + C 盘紧张时 JVM 长 GC 停顿
# 会触发 worker 15s 套接字超时（需配 SPARK_AUTH_SOCKET_TIMEOUT 环境变量调大），
# 故并发限为 4
SPARK_MINING_MASTER = "local[4]"
SPARK_WAREHOUSE = os.path.join(BASE_DIR, "data", "spark-warehouse")

# ---- Neo4j ----
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "bigdata2026")
NEO4J_IMPORT_BATCH = 5000

# ---- 挖掘算法参数（设计规约第 3 章）----
PAGERANK_DAMPING = 0.85
PAGERANK_TOL = 1e-5
PAGERANK_MAX_ITER = 15
LPA_MAX_ITER = 12
PREDICT_TOP_N = 10
