"""FastAPI 主应用：23 个 RESTful 接口 + 前端静态托管（FR-B1~B7）。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import config
from services.graph_service import graph_service
from services.mining_service import mining_service
from services.sna_service import sna_service

app = FastAPI(title="电影演员关系图谱可视化分析系统", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def ok(data):
    return {"code": 0, "msg": "ok", "data": data}


def err(code: int, msg: str):
    return {"code": code, "msg": msg, "data": None}


@app.exception_handler(HTTPException)
async def unified_http_exception(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content=err(exc.status_code, str(exc.detail)))


@app.get("/api/health")
def health():
    try:
        graph_service.driver.verify_connectivity()
        return ok({"status": "up", "neo4j": "connected"})
    except Exception as e:  # noqa: BLE001
        return err(500, f"Neo4j 连接失败: {e}")


@app.get("/api/stats")
def stats():
    return ok(graph_service.stats())


@app.get("/api/graph/overview")
def overview(limit: int = Query(500, ge=10, le=1200), community: int | None = None):
    return ok(graph_service.overview(limit, community))


@app.get("/api/actor/search")
def search(name: str = Query(..., min_length=1), limit: int = 20):
    return ok(graph_service.search(name, limit))


@app.get("/api/actor/{actor_id}/neighbors")
def neighbors(actor_id: str):
    d = graph_service.neighbors(actor_id)
    if d is None:
        raise HTTPException(404, f"演员 {actor_id} 不存在")
    return ok(d)


@app.get("/api/actor/{actor_id}/detail")
def detail(actor_id: str):
    d = graph_service.detail(actor_id)
    if d is None:
        raise HTTPException(404, f"演员 {actor_id} 不存在")
    return ok(d)


@app.get("/api/movie/{movie_id}/detail")
def movie_detail(movie_id: str):
    d = graph_service.movie_detail(movie_id)
    if d is None:
        raise HTTPException(404, f"电影 {movie_id} 不存在")
    return ok(d)


@app.get("/api/path")
def path(from_id: str = Query(..., alias="from"), to_id: str = Query(..., alias="to")):
    if from_id == to_id:
        return err(400, "起点与终点相同")
    d = graph_service.path(from_id, to_id)
    if "error" in d:
        return err(404, d["error"])
    return ok(d)


@app.get("/api/rank/pagerank")
def rank_pagerank(limit: int = Query(20, ge=1, le=200)):
    return ok(mining_service.rank_pagerank(limit))


@app.get("/api/rank/degree")
def rank_degree(limit: int = Query(20, ge=1, le=200)):
    return ok(mining_service.rank_degree(limit))


@app.get("/api/rank/movies")
def rank_movies(limit: int = Query(20, ge=1, le=200)):
    return ok(mining_service.rank_movies(limit))


@app.get("/api/communities")
def communities():
    return ok(mining_service.communities())


@app.get("/api/community/{community_id}/members")
def community_members(community_id: int, limit: int = Query(200, ge=1, le=1000)):
    return ok(mining_service.community_members(community_id, limit))


@app.get("/api/predict/{actor_id}")
def predict(actor_id: str, limit: int = Query(10, ge=1, le=50)):
    return ok(mining_service.predict(actor_id, limit))


# ---- SNA 社交网络分析（FR-B7 / FR-M7~M10） ----
def _sna_or_503(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except FileNotFoundError as e:
        raise HTTPException(503, str(e))


@app.get("/api/sna/metrics")
def sna_metrics():
    return ok(_sna_or_503(sna_service.metrics))


@app.get("/api/sna/degree-distribution")
def sna_degree_distribution():
    return ok(_sna_or_503(sna_service.degree_distribution))


@app.get("/api/sna/evolution")
def sna_evolution():
    return ok(_sna_or_503(sna_service.evolution))


@app.get("/api/sna/kcore")
def sna_kcore(member_limit: int = Query(30, ge=1, le=200)):
    return ok(sna_service.kcore(member_limit))


@app.get("/api/rank/betweenness")
def rank_betweenness(limit: int = Query(20, ge=1, le=200)):
    return ok(sna_service.rank_betweenness(limit))


# ---- 深度挖掘：链接预测 / 图嵌入 / 类型挖掘（FR-M11~M13） ----
@app.get("/api/sna/prediction-eval")
def sna_prediction_eval():
    return ok(_sna_or_503(sna_service.prediction_eval))


@app.get("/api/sna/embeddings")
def sna_embeddings(limit: int = Query(2000, ge=100, le=5000)):
    return ok(_sna_or_503(sna_service.embeddings, limit))


@app.get("/api/actor/{actor_id}/similar")
def actor_similar(actor_id: str, limit: int = Query(10, ge=1, le=50)):
    d = _sna_or_503(sna_service.similar_actors, actor_id, limit)
    if d is None:
        raise HTTPException(404, f"演员 {actor_id} 不存在")
    return ok(d)


@app.get("/api/sna/genres")
def sna_genres():
    return ok(_sna_or_503(sna_service.genres))


# ---- 前端静态托管（设计规约第 4 章） ----
app.mount("/static", StaticFiles(directory=str(Path(config.FRONTEND_DIR))), name="static")


@app.get("/")
def index():
    return FileResponse(Path(config.FRONTEND_DIR) / "index.html")
