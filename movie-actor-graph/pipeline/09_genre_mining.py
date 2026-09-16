r"""09 电影类型深度挖掘：类型共现网络 / 演员类型画像 / 类型年代演化（FR-M13）。

挖掘**非拓扑属性**（电影类型 genres），证明数据利用不止"转图"（设计规约 3.9）：
1. **类型共现网络**（Spark reduceByKey）：每部电影的类型两两配对计数
   → 19 个类型节点的加权共现网络（力导向可视化，边宽 = 共现次数）
2. **演员类型画像**（Spark 聚合 + 驱动端信息论指标）：
   edges_acted_in join 电影类型 → 每演员类型构成 p(g)，
   **Shannon 多样性指数** H = -Σ p·log₂p（衡量"戏路宽度"）+ 丰富度 + Top-3 类型；
   高 H = 多面手（跨类型发展），低 H = 专一型（深耕单一类型）
3. **类型年代演化**（Spark）：电影按十年分桶 × 类型计数 → 各年代类型份额矩阵
   （堆叠面积图可视化，观察类型兴衰）

产出：data/mining/{genre_cooccurrence.csv, actor_genre_profiles.csv,
genre_era_evolution.csv, genre_summary.json}；回写 Neo4j：
a.genreDiversity / a.genreRichness / a.topGenres + genreDiversity 索引。

Windows 调优沿用 05/06 踩坑记录（local[4]、SPARK_AUTH_SOCKET_TIMEOUT）。
"""
import csv
import glob
import json
import math
import os

# 必须在 import pyspark 前设置：worker↔JVM 套接字读超时默认 15s
os.environ.setdefault("SPARK_AUTH_SOCKET_TIMEOUT", "600")

from pyspark import SparkConf, SparkContext

import common

P = 4

MIN_MOVIES_FOR_PROFILE = 5   # 画像入榜门槛：出演电影数（低于此 Shannon 无意义）
GENERALIST_TOP_N = 10        # 多面手/专一型榜单条数


def log(msg):
    print(msg, flush=True)


def read_csv_rows(*cols):
    files = [f for f in glob.glob(os.path.join(common.GRAPH_DIR, cols[-1], "part-*.csv"))
             if not f.endswith(".crc")]
    if not files:
        raise FileNotFoundError(f"{common.GRAPH_DIR}/{cols[-1]} 缺少 part-*.csv")
    rows = []
    for fp in files:
        with open(fp, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append(r)
    return rows


def write_csv(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    log(f"[out] {path}（{len(rows)} 行）")


def write_back(rows, query, desc, driver):
    batch = common.NEO4J_IMPORT_BATCH
    with driver.session() as s:
        for i in range(0, len(rows), batch):
            s.run(query, rows=rows[i : i + batch]).consume()
    log(f"[write] {desc}: {len(rows)}")


def parse_genres(raw):
    if not raw or raw == "\\N":
        return []
    return [g.strip() for g in raw.split(",") if g.strip() and g.strip() != "\\N"]


def main():
    conf = (
        SparkConf()
        .setAppName(f"{common.SPARK_APP_NAME}-Genre")
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

    movies = read_csv_rows("nodes_movies")
    acted_in = read_csv_rows("edges_acted_in")
    actor_names = {r["actorId"]: r["name"] for r in read_csv_rows("nodes_actors")}
    log(f"[load] 电影 {len(movies)}，参演边 {len(acted_in)}，演员 {len(actor_names)}")

    movie_genres = {}   # {movieId: [genres]}
    movie_year = {}
    for m in movies:
        movie_genres[m["movieId"]] = parse_genres(m.get("genres"))
        y = m.get("year")
        if y:
            try:
                movie_year[m["movieId"]] = int(y)
            except (TypeError, ValueError):
                pass

    # ---- ① 类型共现网络（Spark reduceByKey）----
    movie_genre_rdd = sc.parallelize(
        [(mid, tuple(gs)) for mid, gs in movie_genres.items() if len(gs) >= 1]
    ).cache()

    cooc = (
        movie_genre_rdd
        .flatMap(lambda kv: [
            ((kv[1][i], kv[1][j]), 1)
            for i in range(len(kv[1]))
            for j in range(i + 1, len(kv[1]))
        ])
        .reduceByKey(lambda a, b: a + b, P)
        .collect()
    )
    cooc = sorted(cooc, key=lambda kv: -kv[1])
    genre_movie_cnt = (
        movie_genre_rdd.flatMap(lambda kv: [(g, 1) for g in kv[1]])
        .reduceByKey(lambda a, b: a + b, P)
        .collectAsMap()
    )
    n_genres = len(genre_movie_cnt)
    log(f"[cooc] 类型 {n_genres} 种，共现对 {len(cooc)}，Top3: "
        + ", ".join(f"{a}+{b}({c})" for (a, b), c in cooc[:3]))

    cooc_rows = [
        {"genreA": a, "genreB": b, "count": c,
         "shareOfA": round(c / genre_movie_cnt[a], 4) if genre_movie_cnt.get(a) else 0.0}
        for (a, b), c in cooc
    ]
    write_csv(os.path.join(common.MINING_DIR, "genre_cooccurrence.csv"),
              ["genreA", "genreB", "count", "shareOfA"], cooc_rows)

    # ---- ② 演员类型画像（Spark 聚合 + Shannon）----
    # 一部多类型电影对各类型贡献均等：参演该片对类型 g 贡献 1/len(该片类型数)，
    # 得分归一后 p(g)=得分/Σ得分 即"类型构成分布"，Shannon H = -Σ p·log₂p
    actor_genre_score = (
        sc.parallelize(
            [(r["actorId"], r["movieId"]) for r in acted_in]
        )
        .map(lambda kv: (kv[1], kv[0]))                      # (movieId, actorId)
        .join(sc.parallelize(
            [(mid, gs) for mid, gs in movie_genres.items() if gs]
        ))
        .flatMap(lambda kv: [((kv[1][0], g), 1.0 / len(kv[1][1])) for g in kv[1][1]])
        .reduceByKey(lambda a, b: a + b, P)
    ).collectAsMap()  # {(actorId, genre): 加权得分}

    # 参演电影数（画像门槛）
    actor_movies = (
        sc.parallelize([(r["actorId"], 1) for r in acted_in])
        .reduceByKey(lambda a, b: a + b, P)
    ).collectAsMap()

    profiles = {}
    for (aid, g), score in actor_genre_score.items():
        profiles.setdefault(aid, []).append((g, score))
    profile_rows = []
    for aid, gl in profiles.items():
        total = sum(s for _, s in gl)
        top3 = sorted(gl, key=lambda kv: -kv[1])[:3]
        richness = len(gl)
        # H 理论上非负；单一类型时 p=1、log2(1)=0 会产生 -0.0，取 abs 规整
        h = abs(-sum((s / total) * math.log2(s / total) for _, s in gl))
        profile_rows.append({
            "actorId": aid,
            "name": actor_names.get(aid, aid),
            "movieCount": actor_movies.get(aid, 0),
            "genreRichness": richness,
            "shannonDiversity": round(h, 4),
            "maxPossibleH": round(math.log2(richness), 4),
            "topGenres": ";".join(g for g, _ in top3),
            "topGenreShares": ";".join(f"{s / total:.3f}" for _, s in top3),
            "_hmax": math.log2(richness),
        })
    profile_rows.sort(key=lambda r: (-r["shannonDiversity"], -r["movieCount"]))
    eligible = [r for r in profile_rows if r["movieCount"] >= MIN_MOVIES_FOR_PROFILE]
    log(f"[profile] 画像 {len(profile_rows)} 人，≥{MIN_MOVIES_FOR_PROFILE} 部可入榜 "
        f"{len(eligible)} 人；平均 H={sum(r['shannonDiversity'] for r in profile_rows)/len(profile_rows):.3f}")

    out_rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in profile_rows]
    write_csv(os.path.join(common.MINING_DIR, "actor_genre_profiles.csv"),
              ["actorId", "name", "movieCount", "genreRichness", "shannonDiversity",
               "maxPossibleH", "topGenres", "topGenreShares"], out_rows)

    # ---- ③ 类型年代演化（十年分桶 × 份额）----
    era_cnt = (
        sc.parallelize([
            (((movie_year[mid] // 10) * 10, g), 1)
            for mid, gs in movie_genres.items()
            if gs and mid in movie_year
            for g in gs
        ])
        .reduceByKey(lambda a, b: a + b, P)
    ).collectAsMap()  # {(decade, genre): count}

    era_total = {}
    for (decade, _), c in era_cnt.items():
        era_total[decade] = era_total.get(decade, 0) + c
    evo_rows = []
    for (decade, g), c in sorted(era_cnt.items()):
        evo_rows.append({
            "decade": decade, "genre": g, "count": c,
            "share": round(c / era_total[decade], 4),
        })
    log(f"[evolution] 年代 {min(era_total)}–{max(era_total)}，"
        f"年代×类型组合 {len(evo_rows)} 个")
    write_csv(os.path.join(common.MINING_DIR, "genre_era_evolution.csv"),
              ["decade", "genre", "count", "share"], evo_rows)

    # ---- 汇总 JSON ----
    avg_h = sum(r["shannonDiversity"] for r in profile_rows) / len(profile_rows)
    generalists = [
        {"actorId": r["actorId"], "name": r["name"], "movieCount": r["movieCount"],
         "shannonDiversity": r["shannonDiversity"], "genreRichness": r["genreRichness"],
         "topGenres": r["topGenres"]}
        for r in eligible[:GENERALIST_TOP_N]
    ]
    specialists = [
        {"actorId": r["actorId"], "name": r["name"], "movieCount": r["movieCount"],
         "shannonDiversity": r["shannonDiversity"], "genreRichness": r["genreRichness"],
         "topGenres": r["topGenres"]}
        for r in sorted(eligible, key=lambda r: (r["shannonDiversity"], -r["movieCount"]))[:GENERALIST_TOP_N]
    ]
    top_genres_overall = sorted(genre_movie_cnt.items(), key=lambda kv: -kv[1])[:8]
    summary = {
        "nGenres": n_genres,
        "nCooccurrencePairs": len(cooc),
        "nProfiledActors": len(profile_rows),
        "avgShannonDiversity": round(avg_h, 4),
        "minMoviesForProfile": MIN_MOVIES_FOR_PROFILE,
        "topGenrePairs": [{"a": a, "b": b, "count": c} for (a, b), c in cooc[:10]],
        "topGenres": [{"genre": g, "movieCount": c} for g, c in top_genres_overall],
        "generalists": generalists,
        "specialists": specialists,
        "eraRange": [min(era_total), max(era_total)] if era_total else [],
    }
    with open(os.path.join(common.MINING_DIR, "genre_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    log(f"[out] genre_summary.json（多面手 Top1: "
        f"{generalists[0]['name']} H={generalists[0]['shannonDiversity']:.3f}）")

    # ---- 回写 Neo4j ----
    try:
        from neo4j import GraphDatabase
        drv = GraphDatabase.driver(
            common.NEO4J_URI,
            auth=(common.NEO4J_USER, common.NEO4J_PASSWORD))
        with drv.session() as s:
            s.run("CREATE INDEX actor_genre_diversity IF NOT EXISTS "
                  "FOR (a:Actor) ON (a.genreDiversity)")
        wb_rows = [
            {"id": r["actorId"], "h": r["shannonDiversity"],
             "richness": r["genreRichness"], "top": r["topGenres"].replace(";", ", ")}
            for r in profile_rows
        ]
        write_back(wb_rows, """
            UNWIND $rows AS r
            MATCH (a:Actor {id: r.id})
            SET a.genreDiversity = r.h, a.genreRichness = r.richness,
                a.topGenres = r.top
        """, "演员类型画像", drv)
        drv.close()
    except Exception as e:  # noqa: BLE001
        log(f"[warn] Neo4j 回写失败（可稍后重跑）: {e}")

    sc.stop()
    log("[done] 电影类型深度挖掘完成")


if __name__ == "__main__":
    main()
