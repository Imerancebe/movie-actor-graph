/* 电影演员关系图谱 · 前端逻辑（FR-U1~U6）
 * 数据映射（设计规约 5.2）：节点大小=pagerank 对数缩放；颜色=社区；边粗细=合作次数 */

const API = "/api";

const PALETTE = ["#4f8cff","#36cfc9","#f5a623","#b37feb","#ff85c0","#59d16f","#ff7a45","#e8c268",
  "#7dd3fc","#c084fc","#f472b6","#4ade80","#fb923c","#facc15","#38bdf8","#a78bfa",
  "#fb7185","#34d399","#fbbf24","#60a5fa"];

const $ = (s) => document.querySelector(s);

const chart = echarts.init($("#graph"), null, { renderer: "canvas" });
window.addEventListener("resize", () => chart.resize());

let overviewCache = null;
let topActors = [];   // 供路径查询下拉
let currentMode = "overview"; // overview | path | community

/* ---------------- 工具 ---------------- */
async function fetchJSON(url) {
  const r = await fetch(url);
  const j = await r.json();
  if (j.code !== 0) throw new Error(j.msg || "请求失败");
  return j.data;
}

function toast(msg) {
  const t = $("#toast");
  t.textContent = msg;
  t.hidden = false;
  clearTimeout(t._timer);
  t._timer = setTimeout(() => (t.hidden = true), 2600);
}

function fmtNum(n) {
  if (n == null) return "-";
  if (n >= 10000) return (n / 10000).toFixed(1) + "w";
  if (n >= 1000) return (n / 1000).toFixed(1) + "k";
  return String(n);
}

function colorOf(community) {
  if (community == null) return "#7d8ba3";
  return PALETTE[community % PALETTE.length];
}

function sizeOf(pagerank) {
  if (!pagerank) return 8;
  const s = 8 + 38 * Math.log10(1 + pagerank) / 2;
  return Math.max(8, Math.min(46, s));
}

function setLoading(on) { $("#loading").hidden = !on; }

/* ---------------- 统计卡片（FR-U6） ---------------- */
async function loadStats() {
  const s = await fetchJSON(`${API}/stats`);
  $("#statsCards").innerHTML = [
    ["演员", fmtNum(s.actors)], ["电影", fmtNum(s.movies)],
    ["合作关系", fmtNum(s.coactor)], ["出演记录", fmtNum(s.acted)],
    ["社区", fmtNum(s.communities)],
  ].map(([k, v]) => `<div class="stat-card"><div class="num">${v}</div><div class="label">${k}</div></div>`).join("");
  return s;
}

/* ---------------- 总览图谱（FR-U1） ---------------- */
async function loadOverview(limit = 500, community = null) {
  setLoading(true);
  try {
    let data;
    if (!community && overviewCache) data = overviewCache;
    else {
      const q = community != null ? `?limit=${limit}&community=${community}` : `?limit=${limit}`;
      data = await fetchJSON(`${API}/graph/overview${q}`);
      if (!community) overviewCache = data;
    }
    currentMode = "overview";
    $("#btnBack").hidden = true;
    $("#chartLabel").textContent = community != null
      ? `社区 #${community} 核心子图`
      : "全网络核心子图（按影响力采样）";
    renderGraph(data, { focusBlur: true });
  } catch (e) {
    toast("加载图谱失败: " + e.message);
  } finally {
    setLoading(false);
  }
}

function renderGraph(data, opts = {}) {
  // ECharts 的 category 字段必须是 categories 数组索引，而非社区 ID 原始值
  const categories = [];
  const catIndex = new Map();
  const nodes = data.nodes.map((n) => {
    let cat = null;
    if (n.community != null) {
      if (!catIndex.has(n.community)) {
        catIndex.set(n.community, categories.length);
        categories.push({ name: "社区 " + n.community });
      }
      cat = catIndex.get(n.community);
    }
    return {
      id: n.id,
      name: n.name,
      symbolSize: sizeOf(n.pagerank),
      category: cat,
      itemStyle: { color: colorOf(n.community) },
      value: n.pagerank,
      degree: n.degree,
      community: n.community,
    };
  });
  const links = (data.links || []).map((l) => ({
    source: l.source,
    target: l.target,
    lineStyle: {
      width: Math.min(3, 0.6 + (l.weight || 1) * 0.35),
      opacity: Math.min(0.6, 0.15 + (l.weight || 1) * 0.08),
      color: "#8fa3c2",
      curveness: 0.1,
    },
    weight: l.weight,
  }));

  chart.setOption(
    {
      backgroundColor: "#0d1421",
      tooltip: {
        formatter: (p) =>
          p.dataType === "node"
            ? `<b>${p.name}</b><br>PageRank: ${(p.value ?? 0).toFixed(3)}<br>合作者: ${p.data.degree ?? "-"}<br>社区: ${p.data.community ?? "-"}`
            : `合作 ${p.data.weight} 部电影`,
      },
      legend: categories.length
        ? { data: categories.map((c) => c.name), type: "scroll", bottom: 0,
            textStyle: { color: "#7d8ba3", fontSize: 10 }, height: 30 }
        : undefined,
      series: [{
        type: "graph",
        layout: "force",
        force: { repulsion: 130, edgeLength: [40, 110], gravity: 0.08, friction: 0.25 },
        roam: true,
        draggable: true,
        layoutAnimation: nodes.length <= 600,
        label: { show: true, color: "#c7d3e6", fontSize: 10, position: "right",
                 formatter: (p) => (p.data.symbolSize > 22 ? p.name : "") },
        emphasis: { focus: opts.focusBlur ? "adjacency" : "self", label: { show: true } },
        data: nodes,
        links,
        categories,
      }],
    },
    true
  );

  chart.off("click");
  chart.on("click", (p) => {
    if (p.dataType === "node") openActor(p.data.id);
  });
}

/* ---------------- 搜索（FR-U3） ---------------- */
let searchTimer = null;
$("#searchInput").addEventListener("input", (e) => {
  clearTimeout(searchTimer);
  const q = e.target.value.trim();
  if (!q) { $("#searchResults").hidden = true; return; }
  searchTimer = setTimeout(() => doSearch(q), 300);
});

async function doSearch(q) {
  try {
    const rows = await fetchJSON(`${API}/actor/search?name=${encodeURIComponent(q)}`);
    const box = $("#searchResults");
    if (!rows.length) { box.hidden = true; return; }
    box.innerHTML = rows.slice(0, 12).map((r) => {
      const sc = r.kind === "actor" ? (r.pagerank != null ? `PR ${(+r.pagerank).toFixed(2)}` : "") : `${r.degree ?? ""}`;
      return `<div class="search-item" data-kind="${r.kind}" data-id="${r.id}">
        <span>${r.name}</span><span class="kind ${r.kind}">${r.kind === "actor" ? "演员" : "电影"} ${sc}</span></div>`;
    }).join("");
    box.hidden = false;
    box.querySelectorAll(".search-item").forEach((el) => {
      el.onclick = () => {
        box.hidden = true;
        $("#searchInput").value = "";
        if (el.dataset.kind === "actor") openActor(el.dataset.id);
        else openMovie(el.dataset.id);
      };
    });
  } catch (e) {
    toast("搜索失败: " + e.message);
  }
}

document.addEventListener("click", (e) => {
  if (!e.target.closest(".searchbox")) $("#searchResults").hidden = true;
});

/* ---------------- 演员详情抽屉（FR-U2） ---------------- */
async function openActor(id) {
  const drawer = $("#drawer");
  drawer.hidden = false;
  $("#drawerContent").innerHTML = '<div class="actor-head"><h3>加载中…</h3></div>';
  try {
    const [nb, dt, pred, sim] = await Promise.all([
      fetchJSON(`${API}/actor/${id}/neighbors`),
      fetchJSON(`${API}/actor/${id}/detail`),
      fetchJSON(`${API}/predict/${id}?limit=6`).catch(() => []),
      fetchJSON(`${API}/actor/${id}/similar?limit=6`).catch(() => null),
    ]);

    const topCo = nb.coactors.slice(0, 14);
    const movies = nb.movies.slice(0, 20);
    const simList = sim && sim.neighbors && sim.neighbors.length ? sim.neighbors : [];
    const html = `
      <div class="actor-head">
        <h3>${dt.name}</h3>
        <div class="sub">${dt.id}${dt.birthYear ? " · 生于 " + dt.birthYear : ""} · 社区 #${dt.community ?? "-"}</div>
      </div>
      <div class="metric-grid">
        <div class="metric-box"><div class="v">${(+dt.pagerank || 0).toFixed(3)}</div><div class="k">PageRank 影响力</div></div>
        <div class="metric-box"><div class="v">${dt.degree ?? "-"}</div><div class="k">合作者数</div></div>
        <div class="metric-box"><div class="v">${dt.weightedDegree ?? "-"}</div><div class="k">加权合作度</div></div>
        <div class="metric-box"><div class="v">${dt.movieCount ?? movies.length}</div><div class="k">参演电影</div></div>
        ${dt.genreDiversity != null ? `<div class="metric-box"><div class="v">${(+dt.genreDiversity).toFixed(2)}</div><div class="k">类型多样性 H</div></div>` : ""}
      </div>
      ${dt.topGenres ? `<h4>类型画像（Shannon 多样性 Top 类型）</h4>
      <div class="chip-list">${dt.topGenres.split(",").map((g) => `<span class="chip">${g.trim()}</span>`).join("")}</div>` : ""}
      <h4>主要合作者（点击跳转）</h4>
      <div class="chip-list">${topCo.map((c) =>
        `<span class="chip" data-actor="${c.id}" style="border-color:${colorOf(c.community)}55">${c.name} (${c.weight})</span>`).join("")}
      </div>
      <h4>参演电影（${nb.movies.length}）</h4>
      <div class="movie-list">${movies.map((m) =>
        `<div class="movie-item"><span>${m.title}</span><span class="yr">${m.year ?? ""}</span></div>`).join("") || "无记录"}</div>
      <h4>潜在合作预测（Adamic-Adar）</h4>
      <div class="predict-list">${pred.length ? pred.map((p) =>
        `<div class="predict-item" data-actor="${p.candidateId}">
          <span>${p.candidateName}</span>
          <span class="pf">AA ${(+p.adamicAdar).toFixed(2)} · 共同合作者 ${p.commonNeighbors}</span>
        </div>`).join("") : "暂无预测"}</div>
      <h4>相似演员（64 维嵌入余弦推荐）</h4>
      <div class="predict-list">${simList.length ? simList.map((n) =>
        `<div class="predict-item" data-actor="${n.id}">
          <span>${n.name}</span>
          <span class="pf">余弦 ${(+n.sim).toFixed(3)} · ${n.commonNeighbors} 位共同合作者${n.evidence && n.evidence.length ? "：" + n.evidence.join("、") : ""}</span>
        </div>`).join("") : (sim && sim.note ? sim.note : "暂无推荐")}</div>
    `;
    $("#drawerContent").innerHTML = html;

    $("#drawerContent").querySelectorAll("[data-actor]").forEach((el) => {
      el.onclick = () => openActor(el.dataset.actor);
    });

    // 图谱聚焦该演员邻域
    focusNeighborhood(dt, nb);
  } catch (e) {
    $("#drawerContent").innerHTML = `<div class="actor-head"><h3>加载失败</h3><div class="sub">${e.message}</div></div>`;
  }
}

async function openMovie(id) {
  const drawer = $("#drawer");
  drawer.hidden = false;
  $("#drawerContent").innerHTML = '<div class="actor-head"><h3>加载中…</h3></div>';
  try {
    const dt = await fetchJSON(`${API}/movie/${id}/detail`);
    const cast = dt.cast || [];
    const html = `
      <div class="actor-head">
        <h3>${dt.title}</h3>
        <div class="sub">${dt.id}${dt.year ? " · " + dt.year : ""}${dt.runtime ? " · " + dt.runtime + " 分钟" : ""}</div>
      </div>
      <div class="metric-grid">
        <div class="metric-box"><div class="v">${dt.castCount ?? cast.length}</div><div class="k">出演演员</div></div>
        <div class="metric-box"><div class="v genre-v">${(dt.genres || []).slice(0, 3).join(" / ") || "-"}</div><div class="k">类型</div></div>
      </div>
      <h4>出演演员（点击查看合作网络）</h4>
      <div class="chip-list">${cast.map((c) =>
        `<span class="chip" data-actor="${c.id}" style="border-color:${colorOf(c.community)}55">${c.name}</span>`).join("") || "无记录"}
      </div>
    `;
    $("#drawerContent").innerHTML = html;
    $("#drawerContent").querySelectorAll("[data-actor]").forEach((el) => {
      el.onclick = () => openActor(el.dataset.actor);
    });
  } catch (e) {
    $("#drawerContent").innerHTML = `<div class="actor-head"><h3>加载失败</h3><div class="sub">${e.message}</div></div>`;
  }
}

/* 聚焦：以演员为中心的邻域子图 */
function focusNeighborhood(dt, nb) {
  const nodes = [
    { id: dt.id, name: dt.name, pagerank: dt.pagerank, community: dt.community, degree: dt.degree },
    ...nb.coactors.slice(0, 60),
  ];
  const idSet = new Set(nodes.map((n) => n.id));
  const links = nb.coactors
    .filter((c) => idSet.has(c.id))
    .map((c) => ({ source: dt.id, target: c.id, weight: c.weight }));
  currentMode = "focus";
  $("#btnBack").hidden = false;
  $("#chartLabel").textContent = `${dt.name} 的合作邻域（前 60 位合作者）`;
  renderGraph({ nodes, links }, { focusBlur: false });
}

$("#drawerClose").onclick = () => { $("#drawer").hidden = true; };
$("#btnBack").onclick = () => loadOverview();
$("#btnOverview").onclick = () => { closeSna(); loadOverview(); };

/* ---------------- 榜单（FR-U5） ---------------- */
async function loadRanks() {
  const [pr, deg] = await Promise.all([
    fetchJSON(`${API}/rank/pagerank?limit=15`),
    fetchJSON(`${API}/rank/degree?limit=15`),
  ]);
  topActors = pr;
  renderRank("#rankPagerank", pr, (x) => (+x.score).toFixed(3));
  renderRank("#rankDegree", deg, (x) => `${x.score} 人`);

  // 路径查询下拉填充
  const opts = topActors.map((a) => `<option value="${a.id}">${a.name}</option>`).join("");
  $("#pathFrom").innerHTML = opts;
  $("#pathTo").innerHTML = opts;
  if (topActors.length > 1) $("#pathTo").selectedIndex = 1;
}

function renderRank(sel, rows, fmt) {
  $(sel).innerHTML = rows
    .map((r, i) => `<li data-actor="${r.id}">
      <span class="idx">${i + 1}</span><span class="nm" title="${r.name}">${r.name}</span>
      <span class="sc">${fmt(r)}</span></li>`)
    .join("");
  $(sel).querySelectorAll("li").forEach((el) => {
    el.onclick = () => openActor(el.dataset.actor);
  });
}

/* ---------------- 社区（FR-U5） ---------------- */
async function loadCommunities() {
  const list = await fetchJSON(`${API}/communities`);
  $("#communityList").innerHTML = list.slice(0, 40)
    .map((c) => `<div class="community-item" data-cid="${c.community}">
      <div class="cm-head">
        <span class="dot" style="background:${colorOf(c.community)}"></span>
        <span>社区 #${c.community}</span>
        <span class="cm-size">${c.size} 人</span>
      </div>
      <div class="cm-members">${c.topMembers.map((m) => m.name).join(" · ")}</div>
    </div>`)
    .join("");
  $("#communityList").querySelectorAll("[data-cid]").forEach((el) => {
    el.onclick = () => loadOverview(500, +el.dataset.cid);
  });
}

/* ---------------- 合作路径（FR-U4） ---------------- */
$("#pathBtn").onclick = async () => {
  const from = $("#pathFrom").value;
  const to = $("#pathTo").value;
  if (!from || !to) return toast("请先加载影响力榜");
  if (from === to) return toast("请选择两位不同演员");
  setLoading(true);
  try {
    const d = await fetchJSON(`${API}/path?from=${from}&to=${to}`);
    currentMode = "path";
    $("#btnBack").hidden = false;
    $("#chartLabel").textContent = `合作路径（${d.hops} 度合作）`;
    d.nodes.forEach((n, i) => { n.pathOrder = i + 1; });
    const nodes = d.nodes.map((n) => ({
      ...n,
      name: `${n.pathOrder}. ${n.name}`,
    }));
    renderGraph({ nodes, links: d.links }, { focusBlur: false });
    // 路径链路高亮
    chart.setOption({
      series: [{
        links: d.links.map((l) => ({
          ...l,
          lineStyle: { width: 3, opacity: 0.95, color: "#f5a623", curveness: 0 },
        })),
        label: { show: true, fontSize: 11 },
      }],
    });
    const names = d.nodes.map((n) => n.name.replace(/^\d+\.\s*/, "")).join(" → ");
    $("#pathResult").innerHTML = `<span class="hl">${d.hops} 度合作</span>：${names}`;
  } catch (e) {
    toast(e.message);
  } finally {
    setLoading(false);
  }
};

/* ---------------- SNA 网络分析面板（FR-U7） ---------------- */
let snaLoaded = false;
const snaCharts = {};

function getSnaChart(id) {
  if (!snaCharts[id]) snaCharts[id] = echarts.init(document.getElementById(id));
  return snaCharts[id];
}

window.addEventListener("resize", () => Object.values(snaCharts).forEach((c) => c.resize()));

function renderSnaCards(m) {
  const gc = m.giantComponent || {};
  const cards = [
    { v: fmtNum(m.triangles), k: "三角形总数", hl: true },
    { v: m.globalClustering, k: `全局聚类系数（随机图基线 ${m.randomBaselineClustering}）` },
    { v: m.avgLocalClustering, k: "平均局部聚类系数" },
    { v: m.avgPathLength, k: `平均路径长度（巨片内 ${m.sampledSources} 源采样）`, hl: true },
    { v: m.diameter, k: "直径（巨片内采样估计）" },
    { v: gc.ratio != null ? (gc.ratio * 100).toFixed(1) + "%" : "-", k: `巨片占比（共 ${fmtNum(gc.components ?? "-")} 个连通分量）` },
    { v: m.kcore.maxCore, k: "最大 K 壳（核心圈层数）" },
    { v: fmtNum(m.nodes), k: `演员节点（平均度 ${m.avgDegree}）` },
    { v: fmtNum(m.edges), k: "合作关系边" },
  ];
  $("#snaCards").innerHTML = cards.map((c) =>
    `<div class="sna-card${c.hl ? " hl" : ""}"><div class="v">${c.v}</div><div class="k">${c.k}</div></div>`
  ).join("");

  const ratio = m.randomBaselineClustering ? (m.globalClustering / m.randomBaselineClustering) : 0;
  const pathRatio = m.randomBaselineAvgPath ? (m.avgPathLength / m.randomBaselineAvgPath) : 0;
  $("#snaWorldNote").innerHTML =
    `合作网络由 <b>${fmtNum(gc.components)}</b> 个连通分量构成（各国电影圈近乎独立成簇），` +
    `巨片（最大连通分量）<b>${fmtNum(gc.size)}</b> 人、占 ${(gc.ratio * 100).toFixed(1)}% —— 以日本经典电影圈为核心（核心圈成员见下方 K-core，度数榜还出现斯里兰卡影人）。<br>` +
    `<b>小世界判定（巨片内）</b>：全局聚类系数 <b>${m.globalClustering}</b>，为同规模随机图（${m.randomBaselineClustering}）的约 <b>${fmtNum(Math.round(ratio))} 倍</b>；` +
    `平均路径长度 <b>${m.avgPathLength}</b>，与随机图基线 ${m.randomBaselineAvgPath} 同数量级（约 ${pathRatio.toFixed(1)} 倍），仍远小于网络规模 ${fmtNum(m.nodes)} —— ` +
    `「高聚类 + 相对短路径」的小世界特性成立（聚类优势达 3 个数量级，路径仅高出同数量级）。<br>` +
    `度分布幂律斜率 <b>${m.degree.powerLawSlope ?? "-"}</b>（Top 1% 演员占据 ${(m.degree.top1pctShare * 100).toFixed(1)}% 的合作关系），呈无标度特性：少数高产演员是网络的枢纽。` +
    `注：度数 9 处的尖峰是数据规则产物（每部电影取前 10 位主演，单片演员恰好有 9 位同片合作者），长尾部分才反映真实的度异质性。`;
}

function renderDegreeChart(dist, m) {
  const data = dist.map((d) => [d.degree, d.count]);
  const slope = m.degree?.powerLawSlope;
  // 幂律参考线：log-log 空间过均值点的直线
  let refLine = [];
  if (slope != null) {
    const pts = dist.filter((d) => d.degree >= 2 && d.count > 0);
    if (pts.length >= 3) {
      const mx = pts.reduce((s, p) => s + Math.log10(p.degree), 0) / pts.length;
      const my = pts.reduce((s, p) => s + Math.log10(p.count), 0) / pts.length;
      const b = my - slope * mx;
      const [k0, k1] = [pts[0].degree, pts[pts.length - 1].degree];
      refLine = [
        [k0, Math.pow(10, b + slope * Math.log10(k0))],
        [k1, Math.pow(10, b + slope * Math.log10(k1))],
      ];
    }
  }
  const axis = {
    type: "log",
    axisLine: { lineStyle: { color: "#263248" } },
    axisLabel: { color: "#7d8ba3", fontSize: 10 },
    splitLine: { lineStyle: { color: "#1b2537" } },
  };
  const series = [{
    type: "scatter", data, symbolSize: 7,
    itemStyle: { color: "#4f8cff", opacity: 0.85 },
    name: "度分布",
  }];
  if (refLine.length === 2) {
    series.push({
      type: "line", data: refLine, symbol: "none", smooth: false,
      lineStyle: { color: "#f5a623", width: 2, type: "dashed" }, name: "幂律参考线",
    });
  }
  getSnaChart("snaDegreeChart").setOption({
    backgroundColor: "transparent",
    title: {
      text: "合作网络度分布（log-log）",
      subtext: `幂律拟合斜率 ≈ ${slope ?? "-"}，Top 1% 演员占 ${(m.degree.top1pctShare * 100).toFixed(1)}% 合作度 —— 无标度（Scale-Free）`,
      left: 12, top: 10,
      textStyle: { color: "#dbe4f0", fontSize: 13 },
      subtextStyle: { color: "#7d8ba3", fontSize: 11 },
    },
    grid: { left: 58, right: 30, top: 72, bottom: 46 },
    tooltip: { trigger: "item", formatter: (p) => (p.seriesType === "scatter" ? `度数 ${p.data[0]} → ${p.data[1]} 人` : "幂律参考线") },
    legend: { top: 12, right: 16, textStyle: { color: "#7d8ba3", fontSize: 11 } },
    xAxis: { ...axis, name: "度数（log）", nameLocation: "middle", nameGap: 26, nameTextStyle: { color: "#7d8ba3" } },
    yAxis: { ...axis, name: "人数（log）", nameTextStyle: { color: "#7d8ba3" } },
    series,
  });
}

function renderKcore(kc) {
  const shells = kc.shells || [];
  const maxK = kc.maxKshell;
  getSnaChart("snaKcoreChart").setOption({
    backgroundColor: "transparent",
    title: {
      text: `K-core 壳层分布（最大 K 壳 = ${maxK}）`,
      subtext: "逐层剥壳：K 壳越大越处核心，核心圈成员是最紧密合作的演员群体",
      left: 12, top: 10,
      textStyle: { color: "#dbe4f0", fontSize: 13 },
      subtextStyle: { color: "#7d8ba3", fontSize: 11 },
    },
    grid: { left: 56, right: 30, top: 72, bottom: 42 },
    tooltip: { trigger: "axis", formatter: (ps) => `${ps[0].name} 壳层 → ${ps[0].value} 人` },
    xAxis: {
      type: "category", data: shells.map((s) => s.kshell),
      axisLine: { lineStyle: { color: "#263248" } },
      axisLabel: { color: "#7d8ba3", fontSize: 10 },
      name: "K 壳值", nameLocation: "middle", nameGap: 26, nameTextStyle: { color: "#7d8ba3" },
    },
    yAxis: {
      type: "log", name: "人数（log）", nameTextStyle: { color: "#7d8ba3" },
      axisLine: { lineStyle: { color: "#263248" } },
      axisLabel: { color: "#7d8ba3", fontSize: 10 },
      splitLine: { lineStyle: { color: "#1b2537" } },
    },
    series: [{
      type: "bar",
      data: shells.map((s) => ({
        value: s.size,
        itemStyle: { color: s.kshell === maxK ? "#36cfc9" : "#4f8cff", borderRadius: [4, 4, 0, 0] },
      })),
      barMaxWidth: 34,
    }],
  });
  $("#snaKcoreMembers").innerHTML = (kc.coreMembers || []).map((c) =>
    `<span class="chip" data-actor="${c.id}">${c.name}<small style="color:#7d8ba3"> · ${c.degree ?? "-"} 合作者</small></span>`
  ).join("") || "无数据";
  $("#snaKcoreMembers").querySelectorAll("[data-actor]").forEach((el) => {
    el.onclick = () => openActor(el.dataset.actor);
  });
}

function renderEvolution(evo) {
  const years = evo.map((r) => r.year);
  const interval = Math.max(0, Math.ceil(years.length / 18) - 1);
  const base = {
    axisLine: { lineStyle: { color: "#263248" } },
    axisLabel: { color: "#7d8ba3", fontSize: 10 },
    splitLine: { lineStyle: { color: "#1b2537" } },
  };
  getSnaChart("snaEvoChart").setOption({
    backgroundColor: "transparent",
    title: {
      text: "合作网络年代演化",
      subtext: "柱状=当年新增（合作关系 / 演员），折线=累计规模 —— 网络随年代加速扩张",
      left: 12, top: 10,
      textStyle: { color: "#dbe4f0", fontSize: 13 },
      subtextStyle: { color: "#7d8ba3", fontSize: 11 },
    },
    grid: { left: 64, right: 64, top: 76, bottom: 46 },
    tooltip: { trigger: "axis" },
    legend: { top: 14, right: 16, textStyle: { color: "#7d8ba3", fontSize: 11 } },
    xAxis: { ...base, type: "category", data: years, name: "年份", nameLocation: "middle", nameGap: 26, nameTextStyle: { color: "#7d8ba3" }, axisLabel: { ...base.axisLabel, interval } },
    yAxis: [
      { ...base, type: "log", name: "合作关系", nameTextStyle: { color: "#7d8ba3" } },
      { ...base, type: "log", name: "演员数", nameTextStyle: { color: "#7d8ba3" } },
    ],
    series: [
      { name: "新增合作关系", type: "bar", data: evo.map((r) => r.newEdges), itemStyle: { color: "rgba(79,140,255,.55)" }, barMaxWidth: 14 },
      { name: "新增演员", type: "bar", yAxisIndex: 1, data: evo.map((r) => r.newActors), itemStyle: { color: "rgba(245,166,35,.5)" }, barMaxWidth: 14 },
      { name: "累计合作关系", type: "line", data: evo.map((r) => r.cumulativeEdges), smooth: true, symbol: "none", lineStyle: { color: "#36cfc9", width: 2.5 } },
      { name: "累计演员", type: "line", yAxisIndex: 1, data: evo.map((r) => r.cumulativeActors), smooth: true, symbol: "none", lineStyle: { color: "#b37feb", width: 2.5 } },
    ],
  });
}

function renderBetweenness(rows) {
  $("#snaBetweenness").innerHTML = rows.map((r, i) =>
    `<li data-actor="${r.id}">
      <span class="idx">${i + 1}</span>
      <span class="nm" title="${r.name} · ${r.degree ?? "-"} 位合作者 · K壳 ${r.kshell ?? "-"}">${r.name}</span>
      <span class="sc">${(+r.score).toFixed(4)}</span>
    </li>`
  ).join("");
  $("#snaBetweenness").querySelectorAll("li").forEach((el) => {
    el.onclick = () => openActor(el.dataset.actor);
  });
}

/* ---------------- 深度挖掘：链接预测 / 嵌入 / 类型（FR-M11~M13） ---------------- */
function renderPrediction(pe) {
  const algs = pe.algorithms || [];
  const best = pe.bestAlgorithm;
  getSnaChart("snaPredChart").setOption({
    backgroundColor: "transparent",
    title: {
      text: "链接预测 AUC 对比（隐藏 20% 合作边）",
      subtext: `候选对 ${fmtNum(pe.protocol.candidatePairs)} · 正例 ${fmtNum(pe.protocol.positivePairs)} · 边可恢复率 ${(pe.protocol.recoverableRatio * 100).toFixed(2)}%`,
      left: 12, top: 10,
      textStyle: { color: "#dbe4f0", fontSize: 13 },
      subtextStyle: { color: "#7d8ba3", fontSize: 11 },
    },
    grid: { left: 56, right: 30, top: 74, bottom: 42 },
    tooltip: {
      trigger: "axis",
      formatter: (ps) => `${ps[0].name}：AUC ${ps[0].value}<br>P@100 ${(pe.algorithms.find((a) => a.name === ps[0].name).precisionAt["100"] * 100).toFixed(1)}%`,
    },
    xAxis: {
      type: "category", data: algs.map((a) => a.name),
      axisLine: { lineStyle: { color: "#263248" } },
      axisLabel: { color: "#7d8ba3", fontSize: 10 },
    },
    yAxis: {
      type: "value", min: 0, max: 1, name: "AUC", nameTextStyle: { color: "#7d8ba3" },
      axisLine: { lineStyle: { color: "#263248" } },
      axisLabel: { color: "#7d8ba3", fontSize: 10 },
      splitLine: { lineStyle: { color: "#1b2537" } },
    },
    series: [{
      type: "bar", barMaxWidth: 44,
      data: algs.map((a) => ({
        value: a.auc,
        itemStyle: { color: a.name === best ? "#36cfc9" : "#4f8cff", borderRadius: [4, 4, 0, 0] },
      })),
    }],
  });

  $("#snaPredBest").textContent = best;
  $("#snaPredTable").innerHTML = `
    <table class="sna-table">
      <thead><tr><th>算法</th><th>打分公式</th><th>AUC</th><th>Precision@100</th><th>Precision@500</th></tr></thead>
      <tbody>${algs.map((a) => `
        <tr${a.name === best ? ' class="best"' : ""}>
          <td>${a.name}${a.name === best ? " ★" : ""}</td>
          <td><span class="formula">${a.formula}</span></td>
          <td>${a.auc}</td>
          <td>${(a.precisionAt["100"] * 100).toFixed(1)}%</td>
          <td>${(a.precisionAt["500"] * 100).toFixed(1)}%</td>
        </tr>`).join("")}
      </tbody>
    </table>`;
  $("#snaPredPairs").innerHTML = (pe.topPredictions || []).slice(0, 6).map((p) =>
    `<div class="pred-pair-item">
      <div class="pair">${p.a.name} × ${p.b.name}</div>
      <div class="ev">得分 ${(+p.score).toFixed(3)} · ${p.commonCount} 位共同合作者</div>
      <div class="ev">如：${(p.commonNeighbors || []).slice(0, 3).join("、")}</div>
    </div>`
  ).join("");
}

function renderEmbeddings(emb) {
  const pts = emb.points || [];
  const s = emb.summary || {};
  const lp = s.linkPrediction || {};
  const c = getSnaChart("snaEmbChart");
  c.setOption({
    backgroundColor: "transparent",
    title: {
      text: "演员嵌入空间（64 维 → PCA 平面投影）",
      subtext: `度数 Top-${s.topN} 核心子图 · 每节点 ${s.walksPerNode} 条 × ${s.walkLength} 步加权游走 · 窗口 ${s.window} · PPMI ${fmtNum(s.ppmiEntries)} 项 · 截断 SVD`,
      left: 12, top: 10,
      textStyle: { color: "#dbe4f0", fontSize: 13 },
      subtextStyle: { color: "#7d8ba3", fontSize: 11 },
    },
    grid: { left: 46, right: 26, top: 74, bottom: 40 },
    xAxis: { type: "value", show: false, min: (v) => v.min - 0.01, max: (v) => v.max + 0.01 },
    yAxis: { type: "value", show: false },
    tooltip: {
      trigger: "item",
      formatter: (p) => `${p.data.name}<br>社区 #${p.data.community} · ${p.data.degree} 位合作者`,
    },
    series: [{
      type: "scatter",
      data: pts.map((p) => ({
        value: [p.x, p.y],
        name: p.name, actorId: p.actorId,
        community: p.community, degree: p.degree,
      })),
      symbolSize: (val, p) => 2.5 + Math.sqrt(p.data.degree) * 0.9,
      itemStyle: { color: (p) => colorOf(p.data.community), opacity: 0.8 },
    }],
  });
  c.off("click");
  c.on("click", (p) => { if (p.data && p.data.actorId) openActor(p.data.actorId); });

  $("#snaEmbNote").innerHTML =
    `同色聚簇 = LPA 社区 —— 嵌入在无人监督下恢复了社区结构。向量维度 <b>${s.dim}</b>，` +
    `PCA 平面方差解释率 <b>${(s.pcaExplainedVariance * 100).toFixed(1)}%</b>（64 维空间远比平面丰富）。` +
    `链接预测对比（核心子图内）：Adamic-Adar AUC <b>${lp.aaAucInCore}</b> vs 嵌入余弦 AUC <b>${lp.embeddingAucInCore}</b> —— ` +
    `高聚类网络下显式邻域信号极强，嵌入法虽逊于 AA 但仍显著高于随机基线 0.5，且以向量形式支持相似演员检索（点击散点查看）。`;
}

function renderGenres(g) {
  const sum = g.summary || {};
  const cooc = g.cooccurrence || [];
  const nodeW = {};
  cooc.forEach((e) => {
    nodeW[e.genreA] = (nodeW[e.genreA] || 0) + e.count;
    nodeW[e.genreB] = (nodeW[e.genreB] || 0) + e.count;
  });
  const maxW = Math.max(1, ...Object.values(nodeW));
  const nodes = Object.entries(nodeW).map(([name, w]) => ({
    name,
    symbolSize: 10 + 30 * Math.sqrt(w / maxW),
    value: w,
    itemStyle: { color: "#4f8cff" },
    label: { show: true, color: "#dbe4f0", fontSize: 10 },
  }));
  const links = cooc.map((e) => ({
    source: e.genreA, target: e.genreB, value: e.count,
    lineStyle: { width: Math.max(0.6, 5.5 * Math.sqrt(e.count / maxW)), opacity: 0.45, color: "#8fa3c2" },
  }));
  getSnaChart("snaGenreCoocChart").setOption({
    backgroundColor: "transparent",
    title: {
      text: `类型共现网络（${sum.nGenres} 种类型 / ${sum.nCooccurrencePairs} 个共现对）`,
      subtext: "节点大小 / 边粗 = 共现强度，同一部电影的多类型连边 —— 类型宇宙的亲和结构",
      left: 12, top: 10,
      textStyle: { color: "#dbe4f0", fontSize: 13 },
      subtextStyle: { color: "#7d8ba3", fontSize: 11 },
    },
    tooltip: { formatter: (p) => (p.dataType === "edge" ? `${p.data.source} × ${p.data.target}：${p.data.value} 部` : `${p.name}：累计共现 ${p.value}`) },
    series: [{
      type: "graph", layout: "force", roam: true,
      force: { repulsion: 240, edgeLength: [40, 110], gravity: 0.08 },
      nodes, links,
      emphasis: { focus: "adjacency", lineStyle: { opacity: 0.9 } },
    }],
  });

  const seriesMap = g.evolutionSeries || {};
  const decades = [...new Set(Object.values(seriesMap).flat().map((d) => d[0]))].sort((a, b) => a - b);
  const colors = ["#4f8cff", "#36cfc9", "#f5a623", "#b37feb", "#ff85c0", "#59d16f", "#ff7a45", "#e8c268", "#7d8ba3"];
  getSnaChart("snaGenreEvoChart").setOption({
    backgroundColor: "transparent",
    title: {
      text: "类型构成的年代演化（各年代份额）",
      subtext: `${sum.eraRange ? sum.eraRange[0] + "–" + sum.eraRange[1] : ""} 按十年分桶，份额 = 年代内该类型占比 —— 观察类型兴衰`,
      left: 12, top: 10,
      textStyle: { color: "#dbe4f0", fontSize: 13 },
      subtextStyle: { color: "#7d8ba3", fontSize: 11 },
    },
    grid: { left: 52, right: 26, top: 76, bottom: 42 },
    tooltip: { trigger: "axis", valueFormatter: (v) => ((v ?? 0) * 100).toFixed(1) + "%" },
    legend: { top: 14, right: 16, textStyle: { color: "#7d8ba3", fontSize: 11 } },
    xAxis: {
      type: "category", data: decades.map((d) => d + "s"),
      axisLine: { lineStyle: { color: "#263248" } },
      axisLabel: { color: "#7d8ba3", fontSize: 10 },
    },
    yAxis: {
      type: "value", max: 1, axisLabel: { color: "#7d8ba3", fontSize: 10, formatter: (v) => (v * 100).toFixed(0) + "%" },
      axisLine: { lineStyle: { color: "#263248" } },
      splitLine: { lineStyle: { color: "#1b2537" } },
    },
    series: Object.entries(seriesMap).map(([genre, data], i) => ({
      name: genre, type: "line", stack: "share", symbol: "none", smooth: true,
      lineStyle: { width: 0 },
      areaStyle: { opacity: 0.78 },
      itemStyle: { color: colors[i % colors.length] },
      data: decades.map((d) => {
        const hit = data.find((x) => x[0] === d);
        return hit ? hit[1] : 0;
      }),
    })),
  });

  const item = (r) => `
    <div class="spectrum-item" data-actor="${r.actorId}" title="H=${r.shannonDiversity} · ${r.movieCount} 部 · ${r.genreRichness} 种类型">
      <span>${r.name}<span class="g-val"> · ${r.topGenres.replace(/;/g, " / ")}</span></span>
      <span class="h-val">H ${(+r.shannonDiversity).toFixed(2)}</span>
    </div>`;
  $("#snaGenreSpectrum").innerHTML = `
    <div class="spectrum-col"><h5>多面手（高 Shannon 多样性 · ≥${sum.minMoviesForProfile} 部）</h5>${(sum.generalists || []).map(item).join("")}</div>
    <div class="spectrum-col"><h5>专一型（低 Shannon 多样性 · ≥${sum.minMoviesForProfile} 部）</h5>${(sum.specialists || []).map(item).join("")}</div>`;
  $("#snaGenreSpectrum").querySelectorAll("[data-actor]").forEach((el) => {
    el.onclick = () => openActor(el.dataset.actor);
  });
}

async function loadSna() {
  const [m, dist, evo, kc, bc, pe, emb, gn] = await Promise.all([
    fetchJSON(`${API}/sna/metrics`),
    fetchJSON(`${API}/sna/degree-distribution`),
    fetchJSON(`${API}/sna/evolution`),
    fetchJSON(`${API}/sna/kcore`),
    fetchJSON(`${API}/rank/betweenness?limit=15`),
    fetchJSON(`${API}/sna/prediction-eval`),
    fetchJSON(`${API}/sna/embeddings?limit=2000`),
    fetchJSON(`${API}/sna/genres`),
  ]);
  renderSnaCards(m);
  renderDegreeChart(dist, m);
  renderKcore(kc);
  renderEvolution(evo);
  renderBetweenness(bc);
  renderPrediction(pe);
  renderEmbeddings(emb);
  renderGenres(gn);
  Object.values(snaCharts).forEach((c) => c.resize());
}

function closeSna() {
  $("#snaPanel").hidden = true;
  $("#btnSna").classList.remove("active");
  $("#btnOverview").classList.add("active");
}

$("#snaClose").onclick = closeSna;
$("#btnSna").onclick = async () => {
  const panel = $("#snaPanel");
  if (!panel.hidden) { closeSna(); return; }
  panel.hidden = false;
  $("#btnSna").classList.add("active");
  $("#btnOverview").classList.remove("active");
  if (!snaLoaded) {
    setLoading(true);
    try {
      await loadSna();
      snaLoaded = true;
    } catch (e) {
      toast("加载网络分析失败: " + e.message);
      closeSna();
    } finally {
      setLoading(false);
    }
  }
};

/* ---------------- 启动 ---------------- */
(async function init() {
  try {
    await loadStats();
    await Promise.all([loadOverview(), loadRanks(), loadCommunities()]);
  } catch (e) {
    toast("初始化失败: " + e.message);
    setLoading(false);
  }
})();
