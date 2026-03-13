let datasetId = "";
let datasetModule = "public";
let historyRows = [];
let historyPage = 1;
const historyPageSize = 5;
let historyTotalPages = 1;
let datasetsById = {};
let allDatasets = [];

const datasetFilterInput = document.getElementById("datasetFilterInput");
const datasetModuleSelect = document.getElementById("datasetModuleSelect");
const datasetSelect = document.getElementById("datasetSelect");
const refreshDatasetsBtn = document.getElementById("refreshDatasetsBtn");
const layoutModeBtn = document.getElementById("layoutModeBtn");
const datasetInfo = document.getElementById("datasetInfo");
const datasetMetaSummary = document.getElementById("datasetMetaSummary");
const datasetPreviewWrap = document.getElementById("datasetPreviewWrap");
const questionInput = document.getElementById("questionInput");
const maxRowsInput = document.getElementById("maxRowsInput");
const askBtn = document.getElementById("askBtn");
const smartAskBtn = document.getElementById("smartAskBtn");
const toolAskBtn = document.getElementById("toolAskBtn");
const executionGraphWrap = document.getElementById("executionGraphWrap");
const answerText = document.getElementById("answerText");
const errorText = document.getElementById("errorText");
const errorHintText = document.getElementById("errorHintText");
const suggestionWrap = document.getElementById("suggestionWrap");
const statusTag = document.getElementById("statusTag");
const codeText = document.getElementById("codeText");
const stdoutText = document.getElementById("stdoutText");
const tableWrap = document.getElementById("tableWrap");
const plotsWrap = document.getElementById("plotsWrap");
const subResultsWrap = document.getElementById("subResultsWrap");
const questionTemplates = document.getElementById("questionTemplates");
const historyKeywordInput = document.getElementById("historyKeywordInput");
const refreshHistoryBtn = document.getElementById("refreshHistoryBtn");
const historyList = document.getElementById("historyList");
const historyPrevBtn = document.getElementById("historyPrevBtn");
const historyNextBtn = document.getElementById("historyNextBtn");
const historyPageInfo = document.getElementById("historyPageInfo");
const plotModal = document.getElementById("plotModal");
const closeModalBtn = document.getElementById("closeModalBtn");
const modalImage = document.getElementById("modalImage");

function setBusy(btn, busy, text) {
  btn.disabled = busy;
  if (busy) btn.dataset.old = btn.textContent;
  btn.textContent = busy ? text : (btn.dataset.old || btn.textContent);
}

function applyLayoutMode(mode) {
  const m = mode === "standard" ? "standard" : "workbench";
  document.body.setAttribute("data-layout-mode", m);
  if (layoutModeBtn) {
    layoutModeBtn.textContent = m === "workbench" ? "切到标准布局" : "切到工作台布局";
  }
  localStorage.setItem("layout_mode", m);
}

function setStatus(status, category) {
  const s = String(status || "ok").toLowerCase();
  const c = String(category || "none").toLowerCase();
  statusTag.className = "status-tag";

  if (s === "ok") {
    statusTag.textContent = "正常";
    statusTag.classList.add("ok");
    return;
  }

  if (c === "field_missing") {
    statusTag.textContent = "降级: 字段缺失";
    statusTag.classList.add("warn");
    return;
  }

  if (c === "semantic_unsupported") {
    statusTag.textContent = "降级: 语义不支持";
    statusTag.classList.add("warn");
    return;
  }

  statusTag.textContent = "降级: 执行异常";
  statusTag.classList.add("error");
}

function parseErrorPayload(payload) {
  const detail = payload && payload.detail;
  if (detail && typeof detail === "object") {
    return {
      category: detail.error_category || "execution_error",
      message: detail.error_message || "分析失败",
      hint: detail.error_hint || ""
    };
  }
  return {
    category: "execution_error",
    message: (typeof detail === "string" && detail) || "分析失败",
    hint: ""
  };
}

function clearResultArea() {
  answerText.textContent = "执行中...";
  errorText.textContent = "";
  errorHintText.textContent = "";
  suggestionWrap.innerHTML = "";
  codeText.textContent = "";
  stdoutText.textContent = "";
  tableWrap.innerHTML = "";
  plotsWrap.innerHTML = "";
  subResultsWrap.innerHTML = "";
  if (executionGraphWrap) {
    executionGraphWrap.innerHTML = "<p class='muted'>发起分析后展示本次分层执行链路</p>";
  }
  setStatus("ok", "none");
}

async function loadDatasets(preferredId) {
  const query = new URLSearchParams({ module: datasetModule });
  const resp = await fetch(`/datasets?${query.toString()}`);
  const payload = await resp.json();
  allDatasets = (payload && payload.data) || [];
  renderDatasetOptions(preferredId);
  updateDatasetInfo();
  if (datasetId) {
    await loadDatasetSummary();
    await loadDatasetPreview();
  } else {
    datasetMetaSummary.innerHTML = "<p class='muted'>未选择数据集</p>";
    datasetPreviewWrap.innerHTML = "<p class='muted'>未选择数据集</p>";
  }
}

function getFilteredDatasets() {
  const kw = (datasetFilterInput.value || "").trim().toLowerCase();
  if (!kw) return allDatasets.slice();
  return allDatasets.filter((r) => {
    const text = `${r.alias || ""}\n${r.original_filename || ""}\n${r.filename || ""}\n${r.dataset_id || ""}`.toLowerCase();
    return text.includes(kw);
  });
}

function renderDatasetOptions(preferredId) {
  const rows = getFilteredDatasets();
  datasetsById = {};

  datasetSelect.innerHTML = "";
  const empty = document.createElement("option");
  empty.value = "";
  empty.textContent = rows.length ? "请选择已有数据集" : "暂无已有数据集";
  datasetSelect.appendChild(empty);

  rows.forEach((r) => {
    datasetsById[r.dataset_id] = r;
    const op = document.createElement("option");
    op.value = r.dataset_id;
    const aliasText = r.alias ? `【${r.alias}】` : "";
    op.textContent = `${aliasText}${r.original_filename || r.filename} (${r.dataset_id})`;
    datasetSelect.appendChild(op);
  });

  if (preferredId && rows.some((r) => r.dataset_id === preferredId)) {
    datasetSelect.value = preferredId;
    datasetId = preferredId;
  } else if (datasetId && rows.some((r) => r.dataset_id === datasetId)) {
    datasetSelect.value = datasetId;
  } else if (rows.length) {
    datasetSelect.value = rows[0].dataset_id;
    datasetId = rows[0].dataset_id;
  } else {
    datasetId = "";
  }
}

async function loadDatasetPreview() {
  if (!datasetId) {
    datasetPreviewWrap.innerHTML = "<p class='muted'>未选择数据集</p>";
    return;
  }
  const resp = await fetch(`/datasets/${encodeURIComponent(datasetId)}/preview?rows=8`);
  const payload = await resp.json();
  const data = payload && payload.data;
  if (!resp.ok || !data) {
    datasetPreviewWrap.innerHTML = "<p class='muted'>数据集预览加载失败</p>";
    return;
  }

  const columnsHtml = (data.columns || [])
    .map((c) => `<span class='field-chip'>${escapeHtml(c)}</span>`)
    .join("");
  const rows = data.sample_rows || [];
  datasetPreviewWrap.innerHTML = `
    <p class="muted">字段预览（${data.rows}行 ${data.cols}列）</p>
    <div class="field-chips">${columnsHtml || "<span class='muted'>无字段</span>"}</div>
    <div class="table-wrap">${renderTableHtml(rows)}</div>
  `;
}

async function loadDatasetSummary() {
  if (!datasetId) {
    datasetMetaSummary.innerHTML = "<p class='muted'>未选择数据集</p>";
    return;
  }
  const resp = await fetch(`/datasets/${encodeURIComponent(datasetId)}/summary`);
  const payload = await resp.json();
  const data = payload && payload.data;
  if (!resp.ok || !data) {
    datasetMetaSummary.innerHTML = "<p class='muted'>数据集总结加载失败</p>";
    return;
  }
  const tags = Array.isArray(data.tags) ? data.tags : [];
  const analyses = Array.isArray(data.recommended_analyses) ? data.recommended_analyses : [];
  datasetMetaSummary.innerHTML = `
    <p class="muted"><strong>数据集元数据总结</strong></p>
    <p class="muted">分类: ${escapeHtml(data.category || "未分类")} | 来源: ${escapeHtml(data.module === "public" ? "公开数据集" : "私人数据集")}</p>
    <p>${escapeHtml(data.biz_description || data.source_description || "暂无描述")}</p>
    <p class="muted">${escapeHtml(data.analysis_notes || "")}</p>
    <p class="muted">标签: ${tags.length ? tags.map((t) => escapeHtml(t)).join(" / ") : "无"}</p>
    <p class="muted">推荐分析: ${analyses.length ? analyses.map((a) => escapeHtml(a)).join("；") : "通用分析"}</p>
  `;
}

async function loadHistory() {
  const query = new URLSearchParams({
    page: String(historyPage),
    page_size: String(historyPageSize),
    keyword: (historyKeywordInput.value || "").trim()
  });
  if (datasetId) query.set("dataset_id", datasetId);
  const resp = await fetch(`/records?${query.toString()}`);
  const payload = await resp.json();
  historyRows = (payload && payload.data) || [];
  historyTotalPages = Math.max(1, Number(payload.total_pages || 1));
  historyPage = Math.min(Math.max(1, Number(payload.page || historyPage)), historyTotalPages);
  updateHistoryPager();
  renderHistory();
}

function renderHistory() {
  const rows = historyRows;

  if (!rows.length) {
    historyList.innerHTML = "<p class='muted'>暂无匹配历史</p>";
    return;
  }

  historyList.innerHTML = rows
    .map((r) => {
      const status = r.status || "ok";
      const category = r.error_category || "none";
      return `
        <article class="history-item" data-q="${escapeAttr(r.question || "")}" data-session="${escapeAttr(r.session_id || "")}">
          <header>
            <span class="time">${escapeHtml(r.time || "")}</span>
            <span class="mini-tag ${escapeHtml(status)}">${escapeHtml(category)}</span>
          </header>
          <p class="q"><strong>问：</strong>${escapeHtml(r.question || "")}</p>
          <p class="a"><strong>答：</strong>${escapeHtml(r.answer || "")}</p>
          <div class="row">
            <button class="reuse-btn" type="button">填入问题</button>
            <button class="view-btn" type="button">查看结果</button>
          </div>
        </article>
      `;
    })
    .join("");
}

function updateHistoryPager() {
  historyPageInfo.textContent = `第 ${historyPage} / ${historyTotalPages} 页`;
  historyPrevBtn.disabled = historyPage <= 1;
  historyNextBtn.disabled = historyPage >= historyTotalPages;
}

function updateDatasetInfo() {
  if (!datasetSelect.value) {
    datasetInfo.textContent = "未选中数据集";
    return;
  }
  const selected = datasetsById[datasetSelect.value] || {};
  const alias = selected.alias ? ` | 别名: ${selected.alias}` : "";
  const source = datasetModule === "public" ? "公开数据集" : "私人数据集";
  datasetInfo.textContent = `当前数据集: ${selected.original_filename || selected.filename || datasetSelect.value}${alias} | ${source}`;
}

refreshDatasetsBtn.addEventListener("click", async () => {
  setBusy(refreshDatasetsBtn, true, "刷新中...");
  try {
    await loadDatasets(datasetId);
    await loadHistory();
  } finally {
    setBusy(refreshDatasetsBtn, false, "刷新列表");
  }
});

datasetModuleSelect.addEventListener("change", async () => {
  datasetModule = datasetModuleSelect.value || "public";
  datasetId = "";
  historyPage = 1;
  await loadDatasets("");
  await loadHistory();
});

datasetSelect.addEventListener("change", async () => {
  datasetId = datasetSelect.value;
  updateDatasetInfo();
  await loadDatasetSummary();
  await loadDatasetPreview();
  await loadHistory();
});

datasetFilterInput.addEventListener("input", async () => {
  const prev = datasetId;
  renderDatasetOptions(prev);
  updateDatasetInfo();
  if (datasetId) {
    await loadDatasetSummary();
    await loadDatasetPreview();
  } else {
    datasetMetaSummary.innerHTML = "<p class='muted'>筛选后无可用数据集</p>";
    datasetPreviewWrap.innerHTML = "<p class='muted'>筛选后无可用数据集</p>";
  }
  await loadHistory();
});

questionTemplates.addEventListener("click", (event) => {
  const btn = event.target.closest("button[data-q]");
  if (!btn) return;
  questionInput.value = btn.dataset.q || "";
  questionInput.focus();
});

refreshHistoryBtn.addEventListener("click", async () => {
  setBusy(refreshHistoryBtn, true, "刷新中...");
  try {
    historyPage = 1;
    await loadHistory();
  } finally {
    setBusy(refreshHistoryBtn, false, "刷新历史");
  }
});

historyKeywordInput.addEventListener("input", async () => {
  historyPage = 1;
  await loadHistory();
});

historyPrevBtn.addEventListener("click", async () => {
  if (historyPage <= 1) return;
  historyPage -= 1;
  await loadHistory();
});

historyNextBtn.addEventListener("click", async () => {
  if (historyPage >= historyTotalPages) return;
  historyPage += 1;
  await loadHistory();
});

if (layoutModeBtn) {
  layoutModeBtn.addEventListener("click", () => {
    const current = document.body.getAttribute("data-layout-mode") || "workbench";
    applyLayoutMode(current === "workbench" ? "standard" : "workbench");
  });
}

historyList.addEventListener("click", (event) => {
  const btn = event.target.closest(".reuse-btn");
  const viewBtn = event.target.closest(".view-btn");
  const item = event.target.closest(".history-item");
  if (!item) return;

  if (btn) {
    questionInput.value = item.dataset.q || "";
    questionInput.focus();
    return;
  }

  if (viewBtn) {
    const sessionId = item.dataset.session || "";
    if (!sessionId) return;
    fetch(`/records/${encodeURIComponent(sessionId)}`)
      .then((resp) => resp.json())
      .then((payload) => {
        if (!payload || !payload.data) throw new Error("未找到历史详情");
        renderAskResult(payload.data);
      })
      .catch((err) => {
        answerText.textContent = "加载历史详情失败";
        errorText.textContent = err.message || "未知错误";
      });
  }
});

async function runAsk(mode, triggerBtn) {
  if (!datasetId) {
    answerText.textContent = "请先选择数据集";
    return;
  }

  const question = questionInput.value.trim();
  if (!question) {
    answerText.textContent = "请输入问题";
    return;
  }

  const allBtns = [askBtn, smartAskBtn, toolAskBtn];
  allBtns.forEach((b) => {
    if (b && b !== triggerBtn) b.disabled = true;
  });
  setBusy(triggerBtn, true, "分析中...");
  clearResultArea();

  try {
    const resp = await fetch("/ask", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dataset_id: datasetId,
        question,
        max_rows: Number(maxRowsInput.value || 20),
        mode
      })
    });

    const data = await resp.json();
    if (!resp.ok) {
      const e = parseErrorPayload(data);
      throw new Error(`${e.category}: ${e.message}${e.hint ? ` | ${e.hint}` : ""}`);
    }
    renderAskResult(data);
    await loadHistory();
  } catch (err) {
    setStatus("degraded", "execution_error");
    answerText.textContent = "分析失败";
    errorText.textContent = err.message || "未知错误";
  } finally {
    setBusy(triggerBtn, false, triggerBtn === askBtn ? "自动分析" : (triggerBtn === smartAskBtn ? "智能分析" : "通用模式"));
    allBtns.forEach((b) => {
      if (b && b !== triggerBtn) b.disabled = false;
    });
  }
}

function renderAskResult(data) {
  answerText.textContent = data.answer || "-";
  setStatus(data.status || (data.used_fallback ? "degraded" : "ok"), data.error_category || "none");
  errorText.textContent = data.error_message || "";
  const hint = data.error_hint || "";
  const modeHint = data.mode_degraded_reason || "";
  const quotaHint = (data.llm_quota && data.llm_quota.message) || "";
  errorHintText.textContent = [hint, modeHint, quotaHint].filter(Boolean).join(" | ");
  renderSuggestion(data.suggested_question || "", data.suggestion_reason || "");
  codeText.textContent = data.generated_code || "";
  stdoutText.textContent = data.stdout || "";
  renderTable(data.table || []);
  const graphUrl = data.execution_graph_url || "";
  const normalPlots = (data.plots || []).filter((p) => p !== graphUrl);
  renderExecutionGraph(graphUrl, data.execution_layers || []);
  renderPlots(normalPlots);
  renderSubResults(data.sub_results || []);
}

askBtn.addEventListener("click", () => runAsk("auto", askBtn));
smartAskBtn.addEventListener("click", () => runAsk("smart", smartAskBtn));
toolAskBtn.addEventListener("click", () => runAsk("tool", toolAskBtn));

function renderSuggestion(question, reason) {
  if (!question) {
    suggestionWrap.innerHTML = "";
    return;
  }
  const safeQuestion = escapeHtml(question);
  suggestionWrap.innerHTML = `
    <div class="suggestion-card">
      <p><strong>建议改写问句：</strong>${safeQuestion}</p>
      <p class="muted">${escapeHtml(reason || "")}</p>
      <button id="useSuggestionBtn" type="button" data-q="${escapeAttr(question)}">使用该问句</button>
    </div>
  `;
}

suggestionWrap.addEventListener("click", (event) => {
  const btn = event.target.closest("#useSuggestionBtn");
  if (!btn) return;
  const q = btn.dataset.q || "";
  if (!q) return;
  questionInput.value = q;
  questionInput.focus();
});

function renderTable(rows) {
  if (!rows.length) {
    tableWrap.innerHTML = "<p class='muted'>无表格结果</p>";
    return;
  }
  tableWrap.innerHTML = renderTableHtml(rows);
}

function renderTableHtml(rows) {
  if (!rows.length) {
    return "<p class='muted'>无表格结果</p>";
  }

  const cols = Object.keys(rows[0]);
  const thead = `<thead><tr>${cols.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}</tr></thead>`;
  const tbody = `<tbody>${rows.map((r) => `<tr>${cols.map((c) => `<td>${escapeHtml(String(r[c] ?? ""))}</td>`).join("")}</tr>`).join("")}</tbody>`;
  return `<table>${thead}${tbody}</table>`;
}

function renderPlots(plots) {
  if (!plots.length) {
    plotsWrap.innerHTML = "<p class='muted'>无图表结果</p>";
    return;
  }

  plotsWrap.innerHTML = renderPlotsHtml(plots);
}

function renderExecutionGraph(graphUrl, layers) {
  if (!executionGraphWrap) return;
  const safeUrl = graphUrl ? encodeURI(graphUrl) : "";
  const layerRows = Array.isArray(layers) ? layers : [];
  const layersHtml = layerRows.length
    ? `
      <div class="table-wrap">
        <table>
          <thead><tr><th>层级</th><th>名称</th><th>状态</th><th>执行细节</th></tr></thead>
          <tbody>
            ${layerRows
              .map((l) => {
                const details = Array.isArray(l.details) ? l.details.map((d) => escapeHtml(String(d))).join("<br/>") : "";
                return `<tr>
                  <td>${escapeHtml(String(l.layer ?? ""))}</td>
                  <td>${escapeHtml(String(l.name || ""))}</td>
                  <td>${escapeHtml(String(l.state || ""))}</td>
                  <td>${details || "-"}</td>
                </tr>`;
              })
              .join("")}
          </tbody>
        </table>
      </div>
    `
    : "<p class='muted'>暂无执行层细节</p>";
  if (!safeUrl) {
    executionGraphWrap.innerHTML = layersHtml;
    return;
  }
  executionGraphWrap.innerHTML = `
    <figure class="plot-card execution-graph-card">
      <img class="plot-img" src="${safeUrl}" alt="execution-graph" loading="lazy" data-src="${safeUrl}" />
      <figcaption>本次执行分层图</figcaption>
    </figure>
    ${layersHtml}
  `;
}

function renderPlotsHtml(plots) {
  return plots
    .map((p, idx) => {
      const src = encodeURI(p);
      return `
        <figure class="plot-card">
          <img class="plot-img" src="${src}" alt="plot-${idx + 1}" loading="lazy" data-src="${src}" />
          <figcaption>${escapeHtml(src)}</figcaption>
        </figure>
      `;
    })
    .join("");
}

function renderSubResults(subResults) {
  if (!Array.isArray(subResults) || !subResults.length) {
    subResultsWrap.innerHTML = "<p class='muted'>无子问题结果</p>";
    return;
  }

  subResultsWrap.innerHTML = subResults
    .map((item, idx) => {
      const st = escapeHtml(item.status || "ok");
      const cat = escapeHtml(item.error_category || "none");
      const q = escapeHtml(item.question || "");
      const a = escapeHtml(item.answer || "");
      const err = escapeHtml(item.error_message || "");
      const hint = escapeHtml(item.error_hint || "");
      const tableHtml = renderTableHtml(item.table || []);
      const plotsHtml = (item.plots && item.plots.length)
        ? renderPlotsHtml(item.plots)
        : "<p class='muted'>无图表结果</p>";
      return `
        <article class="sub-card">
          <header class="sub-head">
            <span class="sub-index">子问题 ${idx + 1}</span>
            <span class="mini-tag ${st}">${cat}</span>
          </header>
          <p><strong>问：</strong>${q}</p>
          <p><strong>答：</strong>${a}</p>
          ${err ? `<p class="error-text">${err}</p>` : ""}
          ${hint ? `<p class="muted">${hint}</p>` : ""}
          <details>
            <summary>子问题结果表</summary>
            <div class="table-wrap">${tableHtml}</div>
          </details>
          <details>
            <summary>子问题图表</summary>
            <div class="plots-wrap">${plotsHtml}</div>
          </details>
        </article>
      `;
    })
    .join("");
}

document.addEventListener("click", (event) => {
  const img = event.target.closest(".plot-img");
  if (!img) return;
  modalImage.src = img.dataset.src || img.src;
  plotModal.classList.remove("hidden");
});

closeModalBtn.addEventListener("click", () => {
  plotModal.classList.add("hidden");
  modalImage.src = "";
});

plotModal.addEventListener("click", (event) => {
  if (event.target === plotModal) {
    plotModal.classList.add("hidden");
    modalImage.src = "";
  }
});

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeAttr(s) {
  return escapeHtml(s).replace(/`/g, "");
}

(async function bootstrap() {
  try {
    datasetModule = datasetModuleSelect.value || "public";
    const savedMode = localStorage.getItem("layout_mode");
    if (savedMode === "workbench" || savedMode === "standard") {
      applyLayoutMode(savedMode);
    } else {
      applyLayoutMode(window.innerWidth > 1080 ? "workbench" : "standard");
    }
    await loadDatasets("");
    await loadHistory();
    setStatus("ok", "none");
  } catch (err) {
    datasetInfo.textContent = `初始化失败: ${err.message}`;
  }
})();
