"""后端配置：Neo4j 连接与路径（环境变量可覆盖）。"""
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "bigdata2026")

FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")
MINING_DIR = os.path.join(BASE_DIR, "data", "mining")

API_PREFIX = "/api"
