"""04 Neo4j 导入：图 CSV → 属性图数据库（FR-M1）。

- 建立唯一约束与索引（幂等）；
- 分批 UNWIND + MERGE 导入节点与关系；
- 导入后计数核对 CSV 行数，PASS/FAIL。
用法：python 04_import_neo4j.py [--reset]  # --reset 先清空图
"""
import csv
import json
import sys

from neo4j import GraphDatabase

import common


def read_csv_rows(name):
    """读取 Spark 输出的 part-*.csv（单文件）。"""
    import glob
    import os

    files = glob.glob(os.path.join(common.GRAPH_DIR, name, "part-*.csv"))
    if not files:
        raise FileNotFoundError(f"{common.GRAPH_DIR}/{name} 下没有 part-*.csv，请先运行 03_build_graph.py")
    rows = []
    with open(files[0], encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def to_int(v):
    try:
        return int(v) if v not in (None, "", "NULL") else None
    except (TypeError, ValueError):
        return None


def main():
    reset = "--reset" in sys.argv
    driver = GraphDatabase.driver(common.NEO4J_URI, auth=(common.NEO4J_USER, common.NEO4J_PASSWORD))

    with driver.session() as s:
        # ---- 约束与索引（幂等） ----
        for stmt in [
            "CREATE CONSTRAINT actor_id IF NOT EXISTS FOR (a:Actor) REQUIRE a.id IS UNIQUE",
            "CREATE CONSTRAINT movie_id IF NOT EXISTS FOR (m:Movie) REQUIRE m.id IS UNIQUE",
            "CREATE INDEX actor_name IF NOT EXISTS FOR (a:Actor) ON (a.name)",
            "CREATE INDEX actor_community IF NOT EXISTS FOR (a:Actor) ON (a.community)",
        ]:
            s.run(stmt)
        print("[index] 约束与索引就绪")

        if reset:
            s.run("MATCH (n) DETACH DELETE n")
            print("[reset] 已清空图数据库")

        # ---- 导入函数 ----
        def batch_insert(query, rows, desc):
            batch = common.NEO4J_IMPORT_BATCH
            for i in range(0, len(rows), batch):
                s.run(query, rows=rows[i : i + batch]).consume()
            print(f"[import] {desc}: {len(rows)}")

        actors = read_csv_rows("nodes_actors")
        batch_insert(
            """
            UNWIND $rows AS r
            MERGE (a:Actor {id: r.actorId})
            SET a.name = r.name, a.birthYear = toInteger(r.birthYear)
            """,
            actors,
            "Actor 节点",
        )

        movies = read_csv_rows("nodes_movies")
        batch_insert(
            """
            UNWIND $rows AS r
            MERGE (m:Movie {id: r.movieId})
            SET m.title = r.title, m.year = toInteger(r.year),
                m.runtimeMinutes = toInteger(r.runtimeMinutes),
                m.genres = split(r.genres, ','), m.castSize = toInteger(r.castSize)
            """,
            movies,
            "Movie 节点",
        )

        acted = read_csv_rows("edges_acted_in")
        batch_insert(
            """
            UNWIND $rows AS r
            MATCH (a:Actor {id: r.actorId}), (m:Movie {id: r.movieId})
            MERGE (a)-[e:ACTED_IN]->(m)
            SET e.category = r.category, e.ordering = toInteger(r.ordering)
            """,
            acted,
            "ACTED_IN 关系",
        )

        coactor = read_csv_rows("edges_coactor")
        batch_insert(
            """
            UNWIND $rows AS r
            MATCH (a:Actor {id: r.actorId}), (b:Actor {id: r.otherActorId})
            MERGE (a)-[e:CO_ACTOR]->(b)
            SET e.weight = toInteger(r.weight), e.movies = r.movies
            """,
            coactor,
            "CO_ACTOR 关系",
        )

        # ---- 计数核对 ----
        cnt = s.run(
            """
            MATCH (a:Actor) WITH count(a) AS actors
            MATCH (m:Movie) WITH actors, count(m) AS movies
            MATCH ()-[r1:ACTED_IN]->() WITH actors, movies, count(r1) AS acted
            MATCH ()-[r2:CO_ACTOR]->() RETURN actors, movies, acted, count(r2) AS coactor
            """
        ).single().data()

    driver.close()

    with open(f"{common.GRAPH_DIR}/_stats.json", encoding="utf-8") as f:
        stats = json.load(f)

    ok = (
        cnt["actors"] == stats["actor_nodes"]
        and cnt["movies"] == stats["movie_nodes"]
        and cnt["acted"] == stats["acted_in_edges"]
        and cnt["coactor"] == stats["coactor_edges"]
    )
    print(f"[check] Neo4j: {cnt}")
    print(f"[check] CSV  : {stats}")
    print("[PASS] 导入计数一致" if ok else "[FAIL] 计数不一致，请检查日志")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
