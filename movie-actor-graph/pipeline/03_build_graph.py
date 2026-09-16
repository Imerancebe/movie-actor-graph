"""03 图构建：清洗数据 → 圈定高热度电影 → 生成节点/边表 CSV（FR-D3、NFR-1）。

策略（设计规约 2.3）：
1. 电影热度代理 = 阵容规模（出演记录数），取 TOP_MOVIES 部；
2. 每部电影取 principals.ordering 前 MAX_ACTORS_PER_MOVIE 位主演；
3. 演员集合 = 选定电影的出演演员（保证无孤立节点）；
4. CO_ACTOR = 同片演员两两组合（a.id < b.id 单向），权重 = 合作电影数；
5. 输出 CSV + _stats.json（含课程门槛核对）。
"""
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

import common


def build_spark():
    return (
        SparkSession.builder.appName(f"{common.SPARK_APP_NAME}-BuildGraph")
        .master(common.SPARK_MASTER)
        .config("spark.sql.warehouse.dir", common.SPARK_WAREHOUSE)
        .config("spark.driver.memory", "4g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def make_pairs(members):
    ms = sorted(set(members))
    out = []
    for i in range(len(ms)):
        for j in range(i + 1, len(ms)):
            out.append((ms[i], ms[j]))
    return out


PAIR_SCHEMA = ArrayType(StructType([StructField("a", StringType()), StructField("b", StringType())]))


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    movies = spark.read.parquet(f"{common.PROCESSED_DIR}/movies_clean")
    roles = spark.read.parquet(f"{common.PROCESSED_DIR}/roles_clean")
    actors = spark.read.parquet(f"{common.PROCESSED_DIR}/actors_clean")

    # ---- 主演截断：每部电影取 ordering 前 N 位 ----
    roles_top = roles.filter(F.col("ordering") <= common.MAX_ACTORS_PER_MOVIE)

    # ---- 电影热度代理：阵容规模（仅对 movie 类型计算，避免剧集抢占名额），取 Top N ----
    movie_roles = roles_top.join(movies.select("movieId"), "movieId", "inner")
    heat = (
        movie_roles.groupBy("movieId")
        .agg(F.count("*").alias("castSize"))
        .orderBy(F.desc("castSize"), F.desc("movieId"))
        .limit(common.TOP_MOVIES)
    )
    sel_movies = movies.join(heat.select("movieId", "castSize"), "movieId", "inner")
    n_movies = sel_movies.count()
    print(f"[graph] 选定电影: {n_movies}")

    # ---- 出演边（仅选定电影） ----
    acted = roles_top.join(sel_movies.select("movieId"), "movieId", "inner").cache()
    n_acted = acted.count()
    print(f"[graph] ACTED_IN 边: {n_acted}")

    # ---- 演员节点（由出演边反推，保证无孤立；name.basics 缺失时以 id 兜底，
    #      避免 principals 与 name.basics 数据不同步导致边端点失配） ----
    sel_actor_ids = acted.select("actorId").distinct()
    sel_actors = sel_actor_ids.join(actors, "actorId", "left_outer").select(
        "actorId",
        F.coalesce(F.col("name"), F.col("actorId")).alias("name"),
        "birthYear",
    )
    n_actors = sel_actors.count()
    print(f"[graph] 演员节点: {n_actors}")

    # ---- CO_ACTOR：同片演员两两组合（按电影收集后展开，避免大表自 join 倾斜） ----
    per_movie = acted.groupBy("movieId").agg(F.collect_set("actorId").alias("members"))
    pair_df = per_movie.select(
        "movieId", F.explode(F.udf(make_pairs, PAIR_SCHEMA)("members")).alias("p")
    ).select("movieId", "p.a", "p.b")
    coactor = (
        pair_df.groupBy("a", "b")
        .agg(F.count("*").alias("weight"), F.slice(F.collect_list("movieId"), 1, 10).alias("movies"))
        .withColumnRenamed("a", "actorId")
        .withColumnRenamed("b", "otherActorId")
        .withColumn("movies", F.concat_ws(",", "movies"))
    )
    n_coactor = coactor.count()
    print(f"[graph] CO_ACTOR 边: {n_coactor}")

    # ---- 输出 CSV ----
    nodes_actors = sel_actors.select("actorId", "name", "birthYear")
    nodes_movies = sel_movies.select(
        "movieId", "title", "year", "runtimeMinutes", F.concat_ws(",", "genres").alias("genres"), "castSize"
    )
    edges_acted = acted.select("actorId", "movieId", "category", "ordering")

    for name, df in [
        ("nodes_actors", nodes_actors),
        ("nodes_movies", nodes_movies),
        ("edges_acted_in", edges_acted),
        ("edges_coactor", coactor),
    ]:
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{common.GRAPH_DIR}/{name}")
        print(f"[out] {name}")

    total_nodes = n_actors + n_movies
    total_edges = n_acted + n_coactor
    stats = {
        "actor_nodes": n_actors,
        "movie_nodes": n_movies,
        "total_nodes": total_nodes,
        "acted_in_edges": n_acted,
        "coactor_edges": n_coactor,
        "total_edges": total_edges,
        "avg_coactor_degree": round(2 * n_coactor / n_actors, 2),
        "top_movies_param": common.TOP_MOVIES,
        "max_actors_per_movie": common.MAX_ACTORS_PER_MOVIE,
    }
    with open(f"{common.GRAPH_DIR}/_stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(json.dumps(stats, ensure_ascii=False, indent=2))

    if total_nodes < common.MIN_NODE_TARGET or total_edges < common.MIN_EDGE_TARGET:
        print(
            f"[WARN] 未达课程门槛（节点≥{common.MIN_NODE_TARGET}，边≥{common.MIN_EDGE_TARGET}），"
            f"请调大 common.TOP_MOVIES 后重跑"
        )
    else:
        print("[PASS] 数据规模达到课程门槛")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
