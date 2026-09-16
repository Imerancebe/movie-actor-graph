# 电影演员关系图谱可视化分析系统（Movie Actor Graph）

大数据综合实践课程项目 · 图数据方向。基于 **SDD（规范驱动开发）** 方法开发，
覆盖课程要求的四层架构：Spark 底层处理 → Neo4j 图存储 → FastAPI 业务层 → ECharts 交互层。

## 1. 项目概览

| 项 | 内容 |
|---|---|
| 数据源 | IMDb 官方公开数据集（https://datasets.imdbws.com） |
| 数据规模 | 演员 22,313 + 电影 2,500 = **24,813 节点**；ACTED_IN 25,000 + CO_ACTOR 110,822 = **135,822 关系边**（课程门槛 1 万节点 / 3 万边的 2.5×/4.5×） |
| 图挖掘（自主实现） | 加权 PageRank、标签传播 LPA 社区发现、度中心性、Adamic-Adar+Jaccard 合作预测 |
| 社交网络分析（自主实现） | 小世界特性（三角计数/聚类系数/多源位图 BFS 平均路径）、无标度度分布幂律、Brandes 介数中心性、K-core 核心边缘分解、合作网络年代演化 |
| 深度数据挖掘（自主实现） | 链接预测五算法对比实验（CN/Jaccard/AA×2/PA，AUC+Precision@K 评估协议）、DeepWalk 矩阵分解视角图嵌入（加权游走+PPMI+截断SVD，64 维）、电影类型深度挖掘（共现网络/Shannon 多样性画像/年代演化） |
| 技术栈 | PySpark 3.5 · Neo4j 5.26 · FastAPI · ECharts 5 |

## 2. 架构

```
① 交互层      原生 JS + ECharts（力导向图/榜单/社区面板/路径查询/网络分析面板）
              ↓ RESTful JSON
② 业务逻辑层  Python FastAPI（23 个接口，静态托管前端）
              ↓ Bolt Driver / Cypher
③ 数据管理与挖掘层  Neo4j（属性图存储 + 查询）；Spark 自主算法结果回写
              ↑ CSV
④ 大数据底层处理层  PySpark（IMDb TSV 清洗 → 图构建 → 挖掘计算 → SNA/深度挖掘）
```

SDD 规约文档见 `docs/`：01-需求规约、02-设计规约、03-任务分解、04-VibeCoding提示词、05-项目报告。

## 3. 环境要求

- Python 3.10+（`pip install -r backend/requirements.txt`）
- JDK 17+（Spark 与 Neo4j 共用）
- Neo4j 5.x（本仓库旁的 `d:\bigmaths\neo4j` 已部署可直接用）
- Windows 需 winutils：`d:\bigmaths\hadoop\bin`（已部署）

## 4. 运行步骤

### 4.1 数据管道（④→③层，约 30~40 分钟）

```powershell
cd d:\bigmaths\movie-actor-graph\pipeline
$env:HADOOP_HOME="d:\bigmaths\hadoop"
$env:PATH="d:\bigmaths\hadoop\bin;$env:PATH"
$env:TMP="d:\bigmaths\_tmp"; $env:TEMP="d:\bigmaths\_tmp"

python 01_download_data.py     # 下载 IMDb 三个 TSV.gz（约 1.25 GB，已缓存可跳过）
python 02_clean_transform.py   # Spark 清洗 → Parquet（过滤电影类型/演员职业/去重）
python 03_build_graph.py       # 构建 Actor/Movie 节点 + ACTED_IN/CO_ACTOR 边 CSV
python 04_import_neo4j.py --reset   # 导入 Neo4j（计数核对 PASS/FAIL）
python 05_graph_mining.py      # PageRank/LPA/中心性/预测 → 回写 Neo4j + CSV
python 06_sna_analysis.py      # SNA：小世界/度分布/介数/K-core/年代演化 → 回写 Neo4j + CSV/JSON
python 07_link_prediction_eval.py  # 链接预测五算法对比实验 → prediction_eval.json
python 08_embedding.py        # DeepWalk 图嵌入（游走+PPMI+SVD）→ 嵌入向量/散点/近邻/汇总
python 09_genre_mining.py      # 类型挖掘：共现网络/Shannon 画像/年代演化 → 回写 Neo4j + CSV/JSON
```

### 4.2 启动 Neo4j

```powershell
d:\bigmaths\neo4j\bin\neo4j.bat console
# Bolt: bolt://localhost:7687  HTTP: http://localhost:7474
# 账号 neo4j / bigdata2026
```

### 4.3 启动后端（含前端托管）

```powershell
cd d:\bigmaths\movie-actor-graph
$env:NEO4J_PASSWORD="bigdata2026"
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

### 4.4 访问系统

浏览器打开 **http://localhost:8000**

## 5. 功能清单

- 力导向合作网络总览（节点大小=PageRank，颜色=LPA 社区，边粗细=合作次数）
- 演员/电影搜索联想、演员详情抽屉（指标/电影/合作者/AA 预测推荐/类型画像/嵌入相似演员推荐）
- 两演员合作最短路径（"几度合作"）可视化
- PageRank 影响力榜、合作网络榜、社区列表与社区过滤
- 合作预测（Adamic-Adar，含共同合作者证据）
- **网络分析面板（SNA）**：小世界指标卡（聚类系数/平均路径/直径/巨片/连通分量）、
  度分布 log-log 散点（幂律拟合参考线）、K-core 壳层分布与核心圈成员、
  合作网络年代演化双轴图、介数中心性"桥接演员"榜
- **深度挖掘三区块**（SNA 面板内）：
  - 链接预测对比实验：CN/Jaccard/AA-log/AA-sqrt/PA 五算法 AUC 柱状图 + 评估表
    + 最高分潜在合作对卡片（隐藏 20% 边还原协议，实测最优 AA-sqrt AUC 0.9981）
  - 演员嵌入空间：64 维 DeepWalk-MF 嵌入 PCA 散点（按社区着色，点击下钻演员
    抽屉查看余弦相似演员推荐）
  - 类型深度挖掘：类型共现力导向网络、类型年代演化堆叠面积图、
    多面手/专一型演员双栏榜（Shannon 多样性，点击下钻）

API 文档（OpenAPI）：http://localhost:8000/docs

## 6. 常见问题

| 问题 | 解决 |
|---|---|
| Spark 报 `Did not find winutils.exe` | 设置 `$env:HADOOP_HOME="d:\bigmaths\hadoop"` 并把其 bin 加入 PATH |
| Spark 卡住/`TimeoutError: timed out` | 确认 `$env:TMP` 指向 D 盘空间充足目录；脚本已内置 `spark.local.dir` 与并行度控制 |
| C 盘空间不足 | pip/Spark 临时目录默认在 C 盘，务必按 4.1 设置 `$env:TMP` |
| Neo4j 连接失败 | 确认 `neo4j.bat console` 已启动且密码为 `bigdata2026`（首次启动前用 `neo4j-admin.bat dbms set-initial-password` 设置） |
| 8080/7474 端口占用 | 后端换 `--port 8001`；Neo4j 端口改 `conf/neo4j.conf` |

## 7. 目录结构

```
movie-actor-graph/
├── docs/                  # SDD 规约与报告
├── pipeline/              # Spark 数据管道（01~09 九步）
├── backend/               # FastAPI 服务
├── frontend/              # 静态前端（index.html / css / js）
├── data/raw|processed|mining
└── README.md
```
