"""挖掘结果服务：榜单 / 社区 / 合作预测（FR-B3/B4）。

榜单与统计查询 Neo4j（挖掘脚本已回写属性）；合作预测读 data/mining/predictions.csv。
"""
import csv
import glob
import os

from neo4j import GraphDatabase

import config


class MiningService:
    def __init__(self):
        self._driver = None
        self._predictions = None
        self._pred_index = None

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

    # ---------- 榜单（FR-B3） ----------
    def rank_pagerank(self, limit=20):
        q = """
        MATCH (a:Actor) WHERE a.pagerank IS NOT NULL
        RETURN a.id AS id, a.name AS name, a.pagerank AS score,
               a.degree AS degree, a.community AS community
        ORDER BY a.pagerank DESC LIMIT $limit
        """
        with self.driver.session() as s:
            return [dict(r) for r in s.run(q, limit=int(limit))]

    def rank_degree(self, limit=20):
        q = """
        MATCH (a:Actor) WHERE a.degree IS NOT NULL
        MATCH (a)-[:ACTED_IN]->(m)
        RETURN a.id AS id, a.name AS name, a.degree AS score,
               count(m) AS movieCount, a.community AS community
        ORDER BY a.degree DESC, movieCount DESC LIMIT $limit
        """
        with self.driver.session() as s:
            return [dict(r) for r in s.run(q, limit=int(limit))]

    def rank_movies(self, limit=20):
        q = """
        MATCH (m:Movie)<-[:ACTED_IN]-(:Actor)
        RETURN m.id AS id, m.title AS title, m.year AS year,
               count(*) AS castSize, m.genres AS genres
        ORDER BY castSize DESC LIMIT $limit
        """
        with self.driver.session() as s:
            return [dict(r) for r in s.run(q, limit=int(limit))]

    # ---------- 社区（FR-B4 / FR-U5） ----------
    def communities(self, top_members=3):
        q = """
        MATCH (a:Actor) WHERE a.community IS NOT NULL
        WITH a.community AS community, count(a) AS size,
             collect({id: a.id, name: a.name, pagerank: a.pagerank})[..0] AS _
        WITH community, size
        MATCH (a:Actor {community: community})
        WITH community, size, collect({id: a.id, name: a.name, pagerank: a.pagerank}) AS members
        RETURN community, size,
               [m IN members | m] AS all_members
        ORDER BY size DESC
        """
        with self.driver.session() as s:
            out = []
            for r in s.run(q):
                d = dict(r)
                members = sorted(d.pop("all_members"), key=lambda x: -(x["pagerank"] or 0))
                d["topMembers"] = [
                    {"id": m["id"], "name": m["name"], "pagerank": m["pagerank"]} for m in members[:top_members]
                ]
                out.append(d)
            return out

    def community_members(self, community_id, limit=200):
        q = """
        MATCH (a:Actor {community: $cid})
        RETURN a.id AS id, a.name AS name, a.pagerank AS pagerank, a.degree AS degree
        ORDER BY a.pagerank DESC LIMIT $limit
        """
        with self.driver.session() as s:
            return [dict(r) for r in s.run(q, cid=int(community_id), limit=int(limit))]

    # ---------- 合作预测（FR-B4 / FR-M6） ----------
    def _load_predictions(self):
        if self._predictions is not None:
            return
        files = glob.glob(os.path.join(config.MINING_DIR, "predictions.csv"))
        if not files:
            # 挖掘尚未产出 CSV：不缓存空结果，待下次调用重试
            return
        with open(files[0], encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        self._predictions = rows
        self._pred_index = None

    def predict(self, actor_id, limit=10):
        self._load_predictions()
        if not self._predictions:
            return []
        if self._pred_index is None:
            idx = {}
            for r in self._predictions:
                idx.setdefault(r["actorId"], []).append(r)
            self._pred_index = idx
        rows = self._pred_index.get(actor_id, [])[: int(limit)]
        for r in rows:
            for k in ("adamicAdar", "jaccard"):
                try:
                    r[k] = float(r[k])
                except (TypeError, ValueError):
                    r[k] = 0.0
            # 挖掘脚本以浮点累计计数，CSV 中形如 "5.0"
            r["commonNeighbors"] = int(float(r.get("commonNeighbors") or 0))
        return rows


mining_service = MiningService()
