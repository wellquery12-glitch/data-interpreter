let datasetId = "";
let datasetsById = {};
let currentReport = null;

const datasetSelect = document.getElementById("datasetSelect");
const topicSelect = document.getElementById("topicSelect");
const requirementInput = document.getElementById("requirementInput");
const refreshBtn = document.getElementById("refreshBtn");
const datasetInfo = document.getElementById("datasetInfo");
const maxRowsInput = document.getElementById("maxRowsInput");
const analyzeBtn = document.getElementById("analyzeBtn");
const exportMdBtn = document.getElementById("exportMdBtn");
const exportPdfBtn = document.getElementById("exportPdfBtn");
const hintText = document.getElementById("hintText");
const summaryText = document.getElementById("summaryText");
const confidenceTag = document.getElementById("confidenceTag");
const insightsWrap = document.getElementById("insightsWrap");
const plotsWrap = document.getElementById("plotsWrap");
const tablesWrap = document.getElementById("tablesWrap");

function setBusy(btn, busy, text) {
  btn.disabled = busy;
  if (busy) btn.dataset.old = btn.textContent;
  btn.textContent = busy ? text : (btn.dataset.old || btn.textContent);
}

function escapeHtml(v) {
  return String(v || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderTable(rows) {
  if (!Array.isArray(rows) || !rows.length) return "<p class='muted'>无表格数据</p>";
  const keys = Object.keys(rows[0] || {});
  const thead = `<tr>${keys.map((k) => `<th>${escapeHtml(k)}</th>`).join("")}</tr>`;
  const body = rows
    .map((r) => `<tr>${keys.map((k) => `<td>${escapeHtml(r[k])}</td>`).join("")}</tr>`)
    .join("");
  return `<table><thead>${thead}</thead><tbody>${body}</tbody></table>`;
}

function renderReport(report) {
  currentReport = report;
  summaryText.textContent = report.summary || "-";
  const conf = String(report.confidence || "medium");
  confidenceTag.textContent = conf;
  confidenceTag.className = "status-tag " + (conf === "high" ? "ok" : (conf === "low" ? "warn" : ""));

  const insights = Array.isArray(report.insights) ? report.insights : [];
  if (!insights.length) {
    insightsWrap.innerHTML = "<p class='muted'>暂无建议</p>";
  } else {
    insightsWrap.innerHTML = insights
      .map((x, i) => {
        return `
          <article class="suggestion-card" style="margin:8px 0;">
            <p><strong>${i + 1}. 发现：</strong>${escapeHtml(x.finding)}</p>
            <p><strong>证据：</strong>${escapeHtml(x.evidence)}</p>
            <p><strong>建议：</strong>${escapeHtml(x.suggestion)}</p>
            <p><strong>优先级：</strong>${escapeHtml(x.priority)}</p>
            <p><strong>影响预估：</strong>${escapeHtml(x.impact_estimation)}</p>
          </article>
        `;
      })
      .join("");
  }

  const tables = Array.isArray(report.tables) ? report.tables : [];
  if (!tables.length) {
    tablesWrap.innerHTML = "<p class='muted'>暂无关键表格</p>";
  } else {
    tablesWrap.innerHTML = tables
      .map((t) => {
        return `<h4>${escapeHtml(t.name || "table")}</h4>${renderTable(t.rows || [])}`;
      })
      .join("");
  }

  const plots = Array.isArray(report.plots) ? report.plots : [];
  if (!plots.length) {
    plotsWrap.innerHTML = "<p class='muted'>暂无图表</p>";
  } else {
    plotsWrap.innerHTML = plots
      .map((p, i) => `<figure style="margin:8px 0;"><img src="${escapeHtml(p)}" alt="plot-${i}" style="max-width:100%; border:1px solid #e5e7eb; border-radius:8px;" /></figure>`)
      .join("");
  }
}

function updateDatasetInfo() {
  if (!datasetId) {
    datasetInfo.textContent = "未选择数据集";
    return;
  }
  const row = datasetsById[datasetId] || {};
  const alias = row.alias ? `【${row.alias}】` : "";
  datasetInfo.textContent = `当前数据集: ${alias}${row.original_filename || row.filename || datasetId} (${datasetId})`;
}

async function loadDatasets() {
  const resp = await fetch("/datasets");
  const payload = await resp.json();
  const rows = (payload && payload.data) || [];
  datasetsById = {};
  datasetSelect.innerHTML = "";

  const empty = document.createElement("option");
  empty.value = "";
  empty.textContent = rows.length ? "请选择数据集" : "暂无数据集";
  datasetSelect.appendChild(empty);

  rows.forEach((r) => {
    datasetsById[r.dataset_id] = r;
    const op = document.createElement("option");
    op.value = r.dataset_id;
    op.textContent = `${r.alias ? `【${r.alias}】` : ""}${r.original_filename || r.filename} (${r.dataset_id})`;
    datasetSelect.appendChild(op);
  });

  if (rows.length) {
    datasetId = rows[0].dataset_id;
    datasetSelect.value = datasetId;
  } else {
    datasetId = "";
  }
  updateDatasetInfo();
}

async function analyze() {
  if (!datasetId) {
    hintText.textContent = "请先选择数据集";
    return;
  }
  setBusy(analyzeBtn, true, "分析中...");
  hintText.textContent = "正在生成商业建议...";
  try {
    const resp = await fetch("/insights/auto", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dataset_id: datasetId,
        topic: topicSelect.value || "sales",
        requirement: (requirementInput.value || "").trim(),
        max_rows: Math.max(3, Math.min(50, Number(maxRowsInput.value || 10)))
      })
    });
    const data = await resp.json();
    if (!resp.ok) {
      throw new Error((data && data.detail) || "分析失败");
    }
    renderReport(data);
    hintText.textContent = "分析完成";
  } catch (err) {
    hintText.textContent = err.message || "分析失败";
  } finally {
    setBusy(analyzeBtn, false, "分析中...");
  }
}

async function exportReport(format) {
  if (!datasetId) {
    hintText.textContent = "请先选择数据集";
    return;
  }
  const btn = format === "pdf" ? exportPdfBtn : exportMdBtn;
  setBusy(btn, true, "导出中...");
  try {
    const resp = await fetch("/insights/export", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        dataset_id: datasetId,
        export_format: format,
        topic: topicSelect.value || "sales",
        requirement: (requirementInput.value || "").trim(),
        max_rows: Math.max(3, Math.min(50, Number(maxRowsInput.value || 10))),
        report: currentReport || {}
      })
    });
    const data = await resp.json();
    if (!resp.ok) {
      throw new Error((data && data.detail) || "导出失败");
    }
    hintText.textContent = `导出成功: ${data.filename}`;
    if (data.download_url) {
      window.open(data.download_url, "_blank");
    }
  } catch (err) {
    hintText.textContent = err.message || "导出失败";
  } finally {
    setBusy(btn, false, "导出中...");
  }
}

refreshBtn.addEventListener("click", async () => {
  hintText.textContent = "刷新中...";
  await loadDatasets();
  hintText.textContent = "数据集已刷新";
});

datasetSelect.addEventListener("change", () => {
  datasetId = datasetSelect.value;
  updateDatasetInfo();
});

analyzeBtn.addEventListener("click", analyze);
exportMdBtn.addEventListener("click", () => exportReport("markdown"));
exportPdfBtn.addEventListener("click", () => exportReport("pdf"));

loadDatasets().catch((err) => {
  hintText.textContent = err.message || "初始化失败";
});
