"""图查询服务：统计 / 总览子图 / 邻居 / 详情 / 最短路径（FR-B2）。"""
from neo4j import GraphDatabase

import config


class GraphService:
    def __init__(self):
        self._driver = None

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

    # ---------- 全局统计（FR-B6） ----------
    def stats(self):
        q = """
        MATCH (a:Actor) WITH count(a) AS actors, avg(a.degree) AS avgDegree
        MATCH (m:Movie) WITH actors, avgDegree, count(m) AS movies
        OPTIONAL MATCH ()-[r1:ACTED_IN]->() WITH actors, avgDegree, movies, count(r1) AS acted
        OPTIONAL MATCH ()-[r2:CO_ACTOR]->()
        RETURN actors, movies, acted, count(r2) AS coactor, avgDegree,
               actors + movies AS nodes, acted + count(r2) AS edges
        """
        with self.driver.session() as s:
            rec = s.run(q).single()
        if rec is None:
            return {}
        d = dict(rec)
        d["communities"] = self._community_count()
        d["avgDegree"] = round(d.pop("avgDegree") or 0, 2)
        return d

    def _community_count(self):
        q = "MATCH (a:Actor) WHERE a.community IS NOT NULL RETURN count(DISTINCT a.community) AS c"
        rec = self.driver.session().run(q).single()
        return rec["c"] if rec else 0

    # ---------- 总览子图（FR-B2 / FR-U1） ----------
    def overview(self, limit=500, community=None):
        """按 pagerank 取核心演员节点及其之间的 CO_ACTOR 边。"""
        limit = max(10, min(int(limit), 1200))
        q_nodes = """
        MATCH (a:Actor) WHERE a.pagerank IS NOT NULL
        """ + ("AND a.community = $community " if community is not None else "") + """
        RETURN a.id AS id, a.name AS name, a.pagerank AS pagerank,
               a.community AS community, a.degree AS degree
        ORDER BY a.pagerank DESC LIMIT $limit
        """
        params = {"limit": limit}
        if community is not None:
            params["community"] = int(community)
        with self.driver.session() as s:
            nodes = [dict(r) for r in s.run(q_nodes, params)]
            if not nodes:
                return {"nodes": [], "links": []}
            ids = [n["id"] for n in nodes]
            q_edges = """
            MATCH (a:Actor)-[e:CO_ACTOR]->(b:Actor)
            WHERE a.id IN $ids AND b.id IN $ids
            RETURN a.id AS source, b.id AS target, e.weight AS weight
            """
            links = [dict(r) for r in s.run(q_edges, ids=ids)]
        return {"nodes": nodes, "links": links}

    # ---------- 搜索（FR-B5） ----------
    def search(self, name, limit=20):
        q = """
        MATCH (a:Actor)
        WHERE toLower(a.name) STARTS WITH $kw
        RETURN 'actor' AS kind, a.id AS id, a.name AS name, a.degree AS degree,
               a.pagerank AS pagerank, a.community AS community
        ORDER BY a.pagerank DESC LIMIT $limit
        UNION
        MATCH (a:Actor)
        WHERE toLower(a.name) CONTAINS $kw AND NOT toLower(a.name) STARTS WITH $kw
        RETURN 'actor' AS kind, a.id AS id, a.name AS name, a.degree AS degree,
               a.pagerank AS pagerank, a.community AS community
        ORDER BY a.pagerank DESC LIMIT $limit
        UNION
        MATCH (m:Movie)
        WHERE toLower(m.title) CONTAINS $kw
        RETURN 'movie' AS kind, m.id AS id, m.title AS name, m.castSize AS degree,
               NULL AS pagerank, NULL AS community
        ORDER BY m.castSize DESC LIMIT $limit
        """
        kw = name.strip().lower()
        with self.driver.session() as s:
            return [dict(r) for r in s.run(q, kw=kw, limit=int(limit))]

    # ---------- 邻居（FR-B2） ----------
    def neighbors(self, actor_id):
        q = """
        MATCH (a:Actor {id: $id})-[e:CO_ACTOR]->(b:Actor)
        RETURN b.id AS id, b.name AS name, e.weight AS weight,
               b.pagerank AS pagerank, b.community AS community, b.degree AS degree
        ORDER BY e.weight DESC, b.pagerank DESC
        """
        q_movies = """
        MATCH (a:Actor {id: $id})-[e:ACTED_IN]->(m:Movie)
        RETURN m.id AS id, m.title AS title, m.year AS year,
               m.genres AS genres, e.category AS category, e.ordering AS ordering
        ORDER BY m.year DESC
        """
        with self.driver.session() as s:
            if not s.run("MATCH (a:Actor {id: $id}) RETURN a", id=actor_id).single():
                return None
            coactors = [dict(r) for r in s.run(q, id=actor_id)]
            movies = [dict(r) for r in s.run(q_movies, id=actor_id)]
        return {"actorId": actor_id, "coactors": coactors, "movies": movies}

    # ---------- 详情（FR-B2） ----------
    def detail(self, actor_id):
        q = """
        MATCH (a:Actor {id: $id})
        OPTIONAL MATCH (a)-[:ACTED_IN]->(m)
        RETURN a.id AS id, a.name AS name, a.birthYear AS birthYear,
               a.pagerank AS pagerank, a.degree AS degree,
               a.weightedDegree AS weightedDegree, a.community AS community,
               a.genreDiversity AS genreDiversity, a.topGenres AS topGenres,
               count(m) AS movieCount
        """
        with self.driver.session() as s:
            rec = s.run(q, id=actor_id).single()
            if rec is None:
                return None
            d = dict(rec)
            d["topMovies"] = [
                dict(r)
                for r in s.run(
                    """
                    MATCH (a:Actor {id: $id})-[e:ACTED_IN]->(m:Movie)
                    RETURN m.id AS id, m.title AS title, m.year AS year, m.genres AS genres
                    ORDER BY m.castSize DESC LIMIT 8
                    """,
                    id=actor_id,
                )
            ]
        return d

    # ---------- 电影详情（FR-B2） ----------
    def movie_detail(self, movie_id):
        q = """
        MATCH (m:Movie {id: $id})
        OPTIONAL MATCH (a:Actor)-[:ACTED_IN]->(m)
        RETURN m.id AS id, m.title AS title, m.year AS year, m.runtimeMinutes AS runtime,
               m.genres AS genres, m.castSize AS castSize, count(a) AS castCount
        """
        q_cast = """
        MATCH (a:Actor)-[e:ACTED_IN]->(m:Movie {id: $id})
        RETURN a.id AS id, a.name AS name, e.category AS category,
               a.pagerank AS pagerank, a.community AS community, a.degree AS degree
        ORDER BY e.ordering
        """
        with self.driver.session() as s:
            rec = s.run(q, id=movie_id).single()
            if rec is None:
                return None
            d = dict(rec)
            d["cast"] = [dict(r) for r in s.run(q_cast, id=movie_id)]
        return d

    # ---------- 最短合作路径（FR-B2 / FR-U4） ----------
    def path(self, from_id, to_id):
        q = """
        MATCH (a:Actor {id: $from}), (b:Actor {id: $to})
        MATCH p = shortestPath((a)-[:CO_ACTOR*..6]-(b))
        RETURN [n IN nodes(p) | {id: n.id, name: n.name, community: n.community,
                                 pagerank: n.pagerank, degree: n.degree}] AS nodes,
               [r IN relationships(p) | {weight: r.weight}] AS rels,
               length(p) AS hops
        """
        with self.driver.session() as s:
            if not s.run("MATCH (a:Actor {id: $id}) RETURN a", id=from_id).single():
                return {"error": f"演员 {from_id} 不存在"}
            if not s.run("MATCH (a:Actor {id: $id}) RETURN a", id=to_id).single():
                return {"error": f"演员 {to_id} 不存在"}
            rec = s.run(q, **{"from": from_id, "to": to_id}).single()
            if rec is None:
                return {"error": "6 跳内未找到合作路径"}
            d = dict(rec)
            # 构造链式 links
            nodes = d["nodes"]
            d["links"] = [
                {"source": nodes[i]["id"], "target": nodes[i + 1]["id"], "weight": rel["weight"]}
                for i, rel in enumerate(d.pop("rels"))
            ]
            return d


graph_service = GraphService()
