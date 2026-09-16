"""社交网络分析服务：小世界指标 / 度分布 / 年代演化 / K-core / 介数榜（FR-B7）。

指标汇总与分布数据读 data/mining/ 下 SNA 管道产出（06_sna_analysis.py）；
K-core 与介数榜查询 Neo4j（管道已回写 a.kshell / a.betweenness 属性）。
深度挖掘扩展：链接预测对比（07）、图嵌入（08）、类型挖掘（09）产出。
"""
import csv
import json
import os

from neo4j import GraphDatabase

import config


class SnaService:
    def __init__(self):
        self._driver = None
        self._summary = None
        self._degree_dist = None
        self._evolution = None
        self._pred_eval = None
        self._embed_summary = None
        self._embed_scatter = None
        self._embed_nbr_index = None
        self._genre_summary = None
        self._genre_cooc = None
        self._genre_evo = None

    @property
    def driver(self):
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASSWORD)
            )
        return self._driver

    def close(self):
        if self._driver:
            self._driver.close()
            self._driver = None

    @staticmethod
    def _read_csv(name):
        path = os.path.join(config.MINING_DIR, name)
        if not os.path.exists(path):
            raise FileNotFoundError(f"SNA 管道尚未运行：缺少 data/mining/{name}")
        with open(path, encoding="utf-8") as f:
            return list(csv.DictReader(f))

    @staticmethod
    def _read_json(name):
        path = os.path.join(config.MINING_DIR, name)
        if not os.path.exists(path):
            raise FileNotFoundError(f"挖掘管道尚未运行：缺少 data/mining/{name}")
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    # ---------- 小世界指标汇总（FR-M7） ----------
    def metrics(self):
        if self._summary is None:
            path = os.path.join(config.MINING_DIR, "sna_summary.json")
            if not os.path.exists(path):
                raise FileNotFoundError("SNA 管道尚未运行：缺少 data/mining/sna_summary.json")
            with open(path, encoding="utf-8") as f:
                self._summary = json.load(f)
        return self._summary

    # ---------- 度分布（log-log 绘图用） ----------
    def degree_distribution(self):
        if self._degree_dist is None:
            rows = self._read_csv("degree_distribution.csv")
            self._degree_dist = [
                {"degree": int(r["degree"]), "count": int(r["count"])} for r in rows
            ]
        return self._degree_dist

    # ---------- 年代演化（FR-M10） ----------
    def evolution(self):
        if self._evolution is None:
            rows = self._read_csv("evolution.csv")
            self._evolution = [
                {
                    "year": int(r["year"]),
                    "newEdges": int(r["newEdges"]),
                    "newActors": int(r["newActors"]),
                    "cumulativeEdges": int(r["cumulativeEdges"]),
                    "cumulativeActors": int(r["cumulativeActors"]),
                }
                for r in rows
            ]
        return self._evolution

    # ---------- K-core 结构（FR-M9） ----------
    def kcore(self, member_limit=30):
        with self.driver.session() as s:
            shells = [
                dict(r)
                for r in s.run(
                    "MATCH (a:Actor) WHERE a.kshell IS NOT NULL "
                    "RETURN a.kshell AS kshell, count(a) AS size ORDER BY kshell"
                )
            ]
            max_k = shells[-1]["kshell"] if shells else 0
            members = [
                dict(r)
                for r in s.run(
                    "MATCH (a:Actor) WHERE a.kshell = $k "
                    "RETURN a.id AS id, a.name AS name, a.degree AS degree, "
                    "a.pagerank AS pagerank, a.betweenness AS betweenness, a.community AS community "
                    "ORDER BY coalesce(a.pagerank, 0) DESC LIMIT $limit",
                    k=max_k,
                    limit=int(member_limit),
                )
            ]
        return {"maxKshell": max_k, "shells": shells, "coreMembers": members}

    # ---------- 介数中心性榜（FR-M8） ----------
    def rank_betweenness(self, limit=20):
        q = """
        MATCH (a:Actor) WHERE a.betweenness IS NOT NULL
        RETURN a.id AS id, a.name AS name, a.betweenness AS score,
               a.degree AS degree, a.kshell AS kshell, a.community AS community
        ORDER BY a.betweenness DESC LIMIT $limit
        """
        with self.driver.session() as s:
            return [dict(r) for r in s.run(q, limit=int(limit))]

    # ---------- 链接预测对比实验（FR-M11 / 07 管道） ----------
    def prediction_eval(self):
        if self._pred_eval is None:
            self._pred_eval = self._read_json("prediction_eval.json")
        return self._pred_eval

    # ---------- 演员图嵌入（FR-M12 / 08 管道） ----------
    def embeddings(self, limit=2000):
        if self._embed_scatter is None:
            rows = self._read_csv("embedding_scatter.csv")
            self._embed_scatter = [
                {
                    "actorId": r["actorId"], "name": r["name"],
                    "x": float(r["x"]), "y": float(r["y"]),
                    "community": int(r["community"]), "degree": int(r["degree"]),
                }
                for r in rows
            ]
        if self._embed_summary is None:
            self._embed_summary = self._read_json("embedding_summary.json")
        return {"summary": self._embed_summary, "points": self._embed_scatter[: int(limit)]}

    # ---------- 基于嵌入的相似演员（FR-M12） ----------
    def similar_actors(self, actor_id, limit=10):
        if self._embed_nbr_index is None:
            rows = self._read_csv("embedding_neighbors.csv")
            idx = {}
            for r in rows:
                idx.setdefault(r["actorId"], []).append(r)
            self._embed_nbr_index = idx
        rows = self._embed_nbr_index.get(actor_id, [])[: int(limit)]
        with self.driver.session() as s:
            rec = s.run(
                "MATCH (a:Actor {id: $id}) "
                "RETURN a.id AS id, a.name AS name, a.degree AS degree, "
                "a.community AS community, a.genreDiversity AS genreDiversity, "
                "a.topGenres AS topGenres",
                id=actor_id,
            ).single()
            if rec is None:
                return None
            actor = dict(rec)
            if not rows:
                return {"actor": actor, "neighbors": [],
                        "note": "该演员不在嵌入核心子图（度数 Top-5000）内"}
            info = {
                dict(r)["id"]: dict(r)
                for r in s.run(
                    "MATCH (a:Actor) WHERE a.id IN $ids "
                    "RETURN a.id AS id, a.name AS name, a.degree AS degree, "
                    "a.community AS community, a.genreDiversity AS genreDiversity, "
                    "a.topGenres AS topGenres",
                    ids=[r["neighborId"] for r in rows],
                )
            }
            neighbors = []
            for r in rows:
                n = info.get(r["neighborId"], {})
                item = {
                    "id": r["neighborId"],
                    "name": n.get("name", r["neighborId"]),
                    "sim": float(r["sim"]),
                    "commonNeighbors": int(r["commonNeighbors"]),
                    "degree": n.get("degree"),
                    "community": n.get("community"),
                    "genreDiversity": n.get("genreDiversity"),
                    "topGenres": n.get("topGenres"),
                    "evidence": [],
                }
                # 共同合作者证据（Top3 相似者取名字，其余仅数量）
                if len(neighbors) < 3:
                    ev = [
                        x["name"] for x in s.run(
                            "MATCH (a:Actor {id: $x})-[:CO_ACTOR]-(w:Actor)"
                            "-[:CO_ACTOR]-(b:Actor {id: $y}) "
                            "RETURN DISTINCT w.name AS name LIMIT 3",
                            x=actor_id, y=r["neighborId"],
                        )
                    ]
                    item["evidence"] = ev
                neighbors.append(item)
        return {"actor": actor, "neighbors": neighbors}

    # ---------- 类型深度挖掘（FR-M13 / 09 管道） ----------
    def genres(self, top_series=8):
        if self._genre_summary is None:
            self._genre_summary = self._read_json("genre_summary.json")
        if self._genre_cooc is None:
            rows = self._read_csv("genre_cooccurrence.csv")
            self._genre_cooc = [
                {"genreA": r["genreA"], "genreB": r["genreB"],
                 "count": int(r["count"]), "shareOfA": float(r["shareOfA"])}
                for r in rows
            ]
        if self._genre_evo is None:
            rows = self._read_csv("genre_era_evolution.csv")
            self._genre_evo = [
                {"decade": int(r["decade"]), "genre": r["genre"],
                 "count": int(r["count"]), "share": float(r["share"])}
                for r in rows
            ]
        # 年代份额透视为堆叠面积图序列：Top-N 类型 + 其余归"其他"
        totals = {}
        for r in self._genre_evo:
            totals[r["genre"]] = totals.get(r["genre"], 0) + r["count"]
        top_genres = [g for g, _ in sorted(totals.items(), key=lambda kv: -kv[1])[:top_series]]
        series = {g: {} for g in top_genres}
        series["其他"] = {}
        for r in self._genre_evo:
            series[r["genre"] if r["genre"] in top_genres else "其他"][r["decade"]] = r["share"]
        evolution_series = {
            g: [[d, s.get(d, 0.0)] for d in sorted(s)]
            for g, s in series.items()
            if s
        }
        return {
            "summary": self._genre_summary,
            "cooccurrence": self._genre_cooc,
            "evolutionSeries": evolution_series,
        }


sna_service = SnaService()
