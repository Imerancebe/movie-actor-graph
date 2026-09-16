# Vibe Coding 提示词记录（AI-Assisted Development Prompts）

| 项目                                        | 内容                                              |
| ----------------------------------------- | ----------------------------------------------- |
| 用途                                        | 课程交付物②要求的「所有 Vibe Coding 提示词（Markdown 格式）」      |
| 开发方式                                      | 自然语言描述意图 → AI 辅助生成/迭代代码 → 人工审查、修改、验证            |
| 声明                                        | 核心架构与算法设计（四层划分、数据模型、PageRank/LPA/Adamic-Adar 公式 |
| 与迭代方案）由小组自主完成并写入 SDD 规约；AI 仅承担代码编写辅助、调试排错 | <br />                                          |
| <br />                                    | 与文档润色（符合课程第 13 页允许的使用方式）。                       |

以下按开发时序记录关键提示词（Pxx）及其产出、人工修改点。

***

## P01 — 项目骨架

> **提示词：**
> 我要在 d:\bigmaths\movie-actor-graph 下创建一个大数据课设项目「电影演员关系图谱
> 可视化分析系统」，四层架构：PySpark 数据管道 / Neo4j 图存储 / FastAPI 后端 /
> ECharts 前端。请按 docs、pipeline、backend、frontend、data/{raw,processed,mining}
> 创建目录骨架。

**产出**：目录结构（与设计规约第 6 章一致）。
**人工修改**：无。

## P02 — 数据下载脚本

> **提示词：**
> 写 pipeline/01\_download\_data.py：从 datasets.imdbws.com 下载 name.basics.tsv.gz、
> title.basics.tsv.gz、title.principals.tsv.gz 三个文件到 data/raw/，要求：本地已存在
> 且字节数 > 10MB 则跳过；显示下载进度与最终大小；URL/路径常量放 common.py。

**产出**：下载脚本 + common.py 配置模块。
**人工修改**：把超时/重试逻辑收敛为 `requests` 简单重试；补充校验文件头（gzip magic）。

## P03 — Spark 清洗

> **提示词：**
> 写 pipeline/02\_clean\_transform.py：用 PySpark 读取 data/raw 三个 TSV.gz（sep='\t',
> header=True），清洗规则：title.basics 只留 titleType=='movie' 且 startYear 是 4 位数字；
> principals 只留 category in ('actor','actress')；name.basics 只留 primaryName 非空；
> 各自去重（tconst/nconst 主键）；把 startYear、runtimeMinutes、birthYear 转 int，
> `\N` 处理为 null。输出 Parquet 到 data/processed/，并打印每步输入输出行数与剔除率。

**产出**：清洗脚本。
**人工修改**：`runtimeMinutes` 存在非数字脏值，改为 `regexp_extract` 数字校验后再 cast；
genres 按 `,` split 存 array；增加 `spark.sql.warehouse.dir` 到项目内路径避免 Windows 权限问题。

## P04 — 图构建

> **提示词：**
> 写 pipeline/03\_build\_graph.py：输入上一步 Parquet。1) 电影按热度排序取 TOP\_MOVIES（默认
> 2500）部，principals 中 ordering<=15 的演员为其出演集合；2) 演员集合 = 这些出演记录
> 的去重演员；3) 输出 CSV：nodes\_actors(id,name,birthYear)、nodes\_movies(id,title,year,
> runtimeMinutes,genres)、edges\_acted\_in(actorId,movieId,category,ordering)；4) 用自连接
> 生成同片演员对，按对聚合得 edges\_coactor(actorId,otherActorId,weight,movies前10个)；
> 5\) 写 \_stats.json 记录四类计数。要求 CO\_ACTOR 只保留 a.id < b.id 的单向边。

**产出**：图构建脚本。
**人工修改**：自连接采用按 movieId 分组收集演员列表后 explode 两两组合（避免大表 join 倾斜）；
对单片演员数 > 15 的先截断到 ordering 前 15；统计中额外记录"平均度数"供前端展示。

## P05 — Neo4j 导入

> **提示词：**
> 写 pipeline/04\_import\_neo4j.py：用 neo4j Python driver 连接 bolt://localhost:7687。
> 先建 Actor/Movie 的 id 唯一约束与 name/community 索引；然后分批（5000/批）用
> UNWIND + MERGE 导入节点与关系（ACTED\_IN、CO\_ACTOR 带 weight 属性）；最后 count
> 核对与 CSV 行数一致则打印 PASS 否则 FAIL。参数从 common.py 读。

**产出**：导入脚本。
**人工修改**：导入前可选清库开关（--reset）；CO\_ACTOR 的 movies 属性截断为字符串列表
避免超长。

## P06 — 图挖掘（自主算法）

> **提示词：**
> 写 pipeline/05\_graph\_mining.py，基于 Spark RDD 在 CO\_ACTOR 无向加权图上自主实现：
>
> 1. 加权 PageRank：PR(v)=(1-d)/N + d\*Σ PR(u)\*w(u,v)/W(u)，d=0.85，收敛阈值 1e-6，
>    最大 30 轮，输出 (actorId, pagerank)；
> 2. 标签传播 LPA：初始标签=自身id，每轮取邻居边权投票最多的标签（平局取最小id），
>    最大 20 轮，收敛后把标签重编号为 0..K-1；
> 3. 度中心性 degree 与加权度；4) Adamic-Adar 合作预测：对每个演员计算其合作者的
>    合作者得分 Σ 1/sqrt(deg(w))，过滤已合作对象，取 Top10，附带共同合作者名字列表。
>    全部结果分批 UNWIND 回写 Neo4j 节点属性，并导出 CSV 到 data/mining/。

**产出**：挖掘脚本（核心算法模块）。
**人工修改**：**PageRank 的 W(u) 用邻居边权和而非邻居数**（设计规约 3.1 的加权公式，
AI 初版漏掉分母加权）；LPA 加轮次日志；预测部分限制候选集为 2 跳邻居避免全图 O(n²)；
回写批次加入重试。

## P07 — FastAPI 后端

> **提示词：**
> 写 backend/：FastAPI 应用，统一响应 {"code","msg","data"}，挂载 frontend 为静态目录。
> services/graph\_service.py 实现 stats/graph/overview/actor neighbors/detail/path
> （shortestPath 限制 *..6）；services/mining\_service.py 实现 rank/pagerank、rank/degree、
> communities（GROUP BY community 带规模与 Top3 成员名）、community members、
> predict（读 data/mining/predictions\_*.csv 缓存）。搜索接口用 name 正则忽略大小写，
> 前缀匹配优先，LIMIT 20。driver 用模块级单例。

**产出**：后端 12 个接口。
**人工修改**：overview 接口改为「Top pagerank 节点 + 这些节点之间的边」（原图按邻接
过滤），避免传输无关边；path 接口对起点=终点做校验；搜索加 `name CONTAINS` 兜底。

## P08 — ECharts 前端

> **提示词：**
> 写 frontend/index.html + css/style.css + js/app.js（原生 JS，ECharts 用 CDN）。
> 布局：顶栏（标题、搜索框带 300ms 防抖联想、统计卡片）、左面板（影响力榜/多产榜/
> 社区列表/路径查询双下拉）、中央力导向图、右侧滑出抽屉（演员详情：指标、电影表、
> 合作者、预测推荐）。图谱：节点 symbolSize 由 pagerank 对数映射 8\~46，颜色 = 20 色
> 调色板按 community 取模，点击节点高亮邻域（其余 focus: 'blur'），路径查询结果单独
> 渲染带序号 label。所有 fetch 走 /api/，错误 toast 提示。

**产出**：前端单页应用。
**人工修改**：力导向图 `layoutAnimation:false` + `force.friction` 调优；抽屉电影列表
按年份倒序；社区颜色图例映射与后端一致；路径图切换后提供"返回总览"按钮。

## P09 — README 与报告

> **提示词：**
> 基于项目现状写 README.md（含架构图、环境要求、五步管道运行、后端启动、访问方式、
> 常见问题：HADOOP\_HOME/winutils、Neo4j 密码、端口占用）和 docs/05-项目报告.md
> （背景、架构与选型理由、数据规模、算法原理与结果分析、功能截图占位、分工说明）。

**产出**：README、项目报告。
**人工修改**：补充真实运行统计数字（节点/边/社区数、PageRank 收敛轮数）。

***

## P10 — SNA 与深度数据挖掘扩展（06~09 管道 + 接口 + 前端区块）

> **提示词：**
> 在课设中添加深度数据挖掘功能，充分体现对数据做了深度挖掘而非简单转图：
> ① 链接预测对比实验（隐藏 20% 边，CN/Jaccard/AA 标准/AA 生产变体/PA 五算法，
> AUC 同分修正秩和法 + Precision@K，自主实现）；② 演员图嵌入（DeepWalk 矩阵分解
> 视角：加权随机游走 + PPMI 共现矩阵 + 截断 SVD 得 64 维，含 PCA 散点/相似推荐/
> 嵌入 vs AA 预测对比）；③ 电影类型深度挖掘（类型共现网络、Shannon 多样性演员
> 画像、类型年代演化，回写 Neo4j）；配套四个 API 与前端 SNA 面板三区块、抽屉
> 相似演员推荐；SDD 三文档与报告同步更新。

**产出**：07/08/09 管道、4 个接口、SNA 面板三区块、报告 5.7~5.10 节。
**人工修改**：评估协议设计（同 seed 复用候选集保证可比）、AA-sqrt 变体语义
澄清、嵌入 AUC 口径（全候选 vs 核心子图内分开报告）、Shannon 负零修正、
报告结论撰写（PA 失效与高聚类的互证关系）。

***

## 使用统计

| 阶段              | 提示词数 | 人工代码修改占比（估算）            |
| --------------- | ---- | ----------------------- |
| 管道（P02\~P05）    | 4    | \~20%（字段清洗细节、join 倾斜优化） |
| 挖掘（P06）         | 1（大） | \~40%（加权公式修正、候选集剪枝）     |
| 后端/前端（P07\~P08） | 2    | \~25%（接口裁剪、交互细节）        |
| 文档（P01/P09）     | 2    | \~15%（数据核对）             |
| 深度挖掘扩展（P10）   | 1（大） | \~35%（评估协议、AUC 口径、结论分析） |

