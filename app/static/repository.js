let activeTab = "public";
let publicPage = 1;
const publicPageSize = 20;
let publicTotalPages = 1;
let publicRows = [];
let privateRows = [];
let currentPublicSource = "uci";

const tabPublicBtn = document.getElementById("tabPublicBtn");
const tabPrivateBtn = document.getElementById("tabPrivateBtn");
const publicPanel = document.getElementById("publicPanel");
const privatePanel = document.getElementById("privatePanel");
const repoHint = document.getElementById("repoHint");

const publicSourceSelect = document.getElementById("publicSourceSelect");
const keywordInput = document.getElementById("uciKeywordInput");
const searchBtn = document.getElementById("uciSearchBtn");
const refreshBtn = document.getElementById("uciRefreshBtn");
const uciListWrap = document.getElementById("uciListWrap");
const prevBtn = document.getElementById("uciPrevBtn");
const nextBtn = document.getElementById("uciNextBtn");
const pageInfo = document.getElementById("uciPageInfo");

const privateCategoryFilter = document.getElementById("privateCategoryFilter");
const privateKeywordInput = document.getElementById("privateKeywordInput");
const privateSearchBtn = document.getElementById("privateSearchBtn");
const privateRefreshBtn = document.getElementById("privateRefreshBtn");
const privateFileInput = document.getElementById("privateFileInput");
const privateUploadBtn = document.getElementById("privateUploadBtn");
const privateUploadHint = document.getElementById("privateUploadHint");
const privateListWrap = document.getElementById("privateListWrap");

const previewModal = document.getElementById("previewModal");
const closePreviewModalBtn = document.getElementById("closePreviewModalBtn");
const previewModalContent = document.getElementById("previewModalContent");

function setBusy(btn, busy, text) {
  btn.disabled = busy;
  if (busy) btn.dataset.old = btn.textContent;
  btn.textContent = busy ? text : (btn.dataset.old || btn.textContent);
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttr(value) {
  return escapeHtml(value).replaceAll("`", "");
}

function switchTab(tab) {
  activeTab = tab === "private" ? "private" : "public";
  publicPanel.style.display = activeTab === "public" ? "" : "none";
  privatePanel.style.display = activeTab === "private" ? "" : "none";
  tabPublicBtn.style.opacity = activeTab === "public" ? "1" : "0.7";
  tabPrivateBtn.style.opacity = activeTab === "private" ? "1" : "0.7";
}

async function loadPublicSources() {
  const resp = await fetch("/datasets/repository/public/sources");
  const payload = await resp.json();
  const rows = Array.isArray(payload.data) ? payload.data : [];
  publicSourceSelect.innerHTML = "";
  rows.forEach((r) => {
    const op = document.createElement("option");
    op.value = r.source_id || "";
    op.textContent = r.name || r.source_id || "-";
    publicSourceSelect.appendChild(op);
  });
  if (!rows.length) {
    const op = document.createElement("option");
    op.value = "uci";
    op.textContent = "UCI";
    publicSourceSelect.appendChild(op);
  }
}

async function loadPublicList() {
  const source = publicSourceSelect.value || "uci";
  currentPublicSource = source;
  const query = new URLSearchParams({
    keyword: (keywordInput.value || "").trim(),
    page: String(publicPage),
    page_size: String(publicPageSize)
  });
  let url = `/datasets/repository/uci?${query.toString()}`;
  if (source === "kaggle") {
    query.set("topic", "businessDataset");
    url = `/datasets/repository/kaggle?${query.toString()}`;
  }
  const resp = await fetch(url);
  const payload = await resp.json();
  if (!resp.ok) {
    throw new Error(payload.detail || "公开数据集加载失败");
  }
  publicRows = Array.isArray(payload.data) ? payload.data : [];
  publicTotalPages = Math.max(1, Number(payload.total_pages || 1));
  publicPage = Math.min(Math.max(1, Number(payload.page || publicPage)), publicTotalPages);
  renderPublicRows();
  updatePublicPager();
  repoHint.textContent = `公开数据集 ${payload.total || 0} 条，当前第 ${publicPage} 页`;
}

function renderPublicRows() {
  if (!publicRows.length) {
    uciListWrap.innerHTML = "<p class='muted'>当前条件下无公开数据集</p>";
    return;
  }
  uciListWrap.innerHTML = publicRows.map((r) => {
    const downloaded = r.downloaded ? "<span class='mini-tag ok'>已下载</span>" : "<span class='mini-tag'>未下载</span>";
    const sourceLine = currentPublicSource === "kaggle"
      ? `Kaggle: ${escapeHtml(r.kaggle_ref || "-")}`
      : `UCI ID: ${escapeHtml(r.uci_id || "")}`;
    const taskLine = currentPublicSource === "kaggle"
      ? `来源: Kaggle businessDataset`
      : `任务: ${escapeHtml(r.task || "-")}`;
    const importBtn = currentPublicSource === "kaggle"
      ? `<button class="public-kaggle-import-btn" data-kaggle-ref="${escapeAttr(r.kaggle_ref || "")}" ${r.downloaded ? "disabled" : ""}>${r.downloaded ? "已下载" : "导入到工作台"}</button>`
      : `<button class="public-import-btn" data-uci-id="${escapeAttr(r.uci_id || "")}" ${r.downloaded ? "disabled" : ""}>${r.downloaded ? "已下载" : "下载到工作台"}</button>`;
    return `
      <article class="repo-item">
        <header>
          <strong>${escapeHtml(r.name || "-")}</strong>
          ${downloaded}
        </header>
        <p class="muted">${sourceLine} | ${taskLine}</p>
        <p>${escapeHtml(r.description || "无描述")}</p>
        <div class="row">
          ${importBtn}
          ${currentPublicSource === "kaggle" ? `<a class="btn-link" href="${escapeAttr(r.dataset_page_url || "#")}" target="_blank" rel="noopener noreferrer">打开 Kaggle 页面</a>` : ""}
          <button class="preview-btn public-preview-btn" data-module="public" data-source="${escapeAttr(currentPublicSource)}" data-uci-id="${escapeAttr(r.uci_id || "")}" data-kaggle-ref="${escapeAttr(r.kaggle_ref || "")}" data-downloaded="${r.downloaded ? "1" : "0"}" data-dataset-id="${escapeAttr(r.dataset_id || "")}" data-name="${escapeAttr(r.name || "")}" data-desc="${escapeAttr(r.description || "")}" data-page-url="${escapeAttr(r.dataset_page_url || "")}">字段预览</button>
        </div>
      </article>
    `;
  }).join("");
}

function updatePublicPager() {
  pageInfo.textContent = `第 ${publicPage} / ${publicTotalPages} 页`;
  prevBtn.disabled = publicPage <= 1;
  nextBtn.disabled = publicPage >= publicTotalPages;
}

async function loadPrivateList() {
  const query = new URLSearchParams({
    module: "private",
    category: (privateCategoryFilter.value || "").trim(),
    keyword: (privateKeywordInput.value || "").trim()
  });
  const resp = await fetch(`/datasets?${query.toString()}`);
  const payload = await resp.json();
  const rows = (payload && payload.data) || [];
  privateRows = Array.isArray(rows) ? rows : [];
  renderPrivateRows();
  repoHint.textContent = `私人数据集 ${privateRows.length} 条`;
}

function renderPrivateRows() {
  if (!privateRows.length) {
    privateListWrap.innerHTML = "<p class='muted'>暂无私人数据集，请先上传</p>";
    return;
  }
  privateListWrap.innerHTML = privateRows.map((r) => {
    const tagsText = Array.isArray(r.tags) ? r.tags.join(",") : "";
    return `
      <article class="repo-item">
        <header>
          <strong>${escapeHtml(r.alias || r.original_filename || r.filename || r.dataset_id)}</strong>
          <span class="mini-tag">${escapeHtml(r.category || "未分类")}</span>
        </header>
        <p class="muted">dataset_id: ${escapeHtml(r.dataset_id || "")}</p>
        <p>${escapeHtml(r.biz_description || "暂无业务描述")}</p>
        <div class="row">
          <input class="meta-alias" data-dataset-id="${escapeAttr(r.dataset_id)}" type="text" value="${escapeAttr(r.alias || "")}" placeholder="别名" />
          <input class="meta-category" data-dataset-id="${escapeAttr(r.dataset_id)}" type="text" value="${escapeAttr(r.category || "")}" placeholder="分类" />
        </div>
        <div class="row">
          <input class="meta-tags" data-dataset-id="${escapeAttr(r.dataset_id)}" type="text" value="${escapeAttr(tagsText)}" placeholder="标签，逗号分隔" />
        </div>
        <div class="row">
          <input class="meta-biz-description" data-dataset-id="${escapeAttr(r.dataset_id)}" type="text" value="${escapeAttr(r.biz_description || "")}" placeholder="业务描述" />
        </div>
        <div class="row">
          <input class="meta-analysis-notes" data-dataset-id="${escapeAttr(r.dataset_id)}" type="text" value="${escapeAttr(r.analysis_notes || "")}" placeholder="分析说明" />
        </div>
        <div class="row">
          <button class="private-save-meta-btn" data-dataset-id="${escapeAttr(r.dataset_id)}">保存基础信息</button>
          <button class="preview-btn private-preview-btn" data-module="private" data-dataset-id="${escapeAttr(r.dataset_id)}">字段预览</button>
          <button class="private-delete-btn" data-dataset-id="${escapeAttr(r.dataset_id)}">删除数据集</button>
        </div>
      </article>
    `;
  }).join("");
}

async function importPublicDataset(uciId, btn) {
  setBusy(btn, true, "下载中...");
  try {
    const resp = await fetch("/datasets/repository/uci/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ uci_id: Number(uciId) })
    });
    const payload = await resp.json();
    if (!resp.ok) {
      throw new Error(payload.detail || "下载失败");
    }
    repoHint.textContent = payload.skipped ? `已存在 dataset_id=${payload.dataset_id}` : `下载完成 dataset_id=${payload.dataset_id}`;
    await loadPublicList();
  } catch (err) {
    repoHint.textContent = `下载失败: ${err.message || err}`;
  } finally {
    setBusy(btn, false, "下载到工作台");
  }
}

async function importKaggleDataset(kaggleRef, btn) {
  setBusy(btn, true, "导入中...");
  try {
    const resp = await fetch("/datasets/repository/kaggle/import", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ kaggle_ref: kaggleRef })
    });
    const payload = await resp.json();
    if (!resp.ok) {
      throw new Error(payload.detail || "导入失败");
    }
    repoHint.textContent = payload.skipped
      ? `Kaggle 数据集已存在，dataset_id=${payload.dataset_id}`
      : `Kaggle 导入完成，dataset_id=${payload.dataset_id}`;
    await loadPublicList();
  } catch (err) {
    repoHint.textContent = `导入失败: ${err.message || err}（如未配置凭证，请到 LLM 管理页配置 Kaggle）`;
  } finally {
    setBusy(btn, false, "导入到工作台");
  }
}

async function uploadPrivateDataset() {
  if (!privateFileInput.files || !privateFileInput.files.length) {
    privateUploadHint.textContent = "请先选择文件";
    return;
  }
  const fd = new FormData();
  fd.append("file", privateFileInput.files[0]);
  setBusy(privateUploadBtn, true, "上传中...");
  try {
    const resp = await fetch("/upload", { method: "POST", body: fd });
    const payload = await resp.json();
    if (!resp.ok) {
      throw new Error(payload.detail || "上传失败");
    }
    privateUploadHint.textContent = `上传成功，dataset_id=${payload.dataset_id}`;
    privateFileInput.value = "";
    await loadPrivateList();
  } catch (err) {
    privateUploadHint.textContent = `上传失败: ${err.message || err}`;
  } finally {
    setBusy(privateUploadBtn, false, "上传");
  }
}

async function savePrivateMetadata(datasetId, btn) {
  const aliasEl = privateListWrap.querySelector(`.meta-alias[data-dataset-id="${datasetId}"]`);
  const categoryEl = privateListWrap.querySelector(`.meta-category[data-dataset-id="${datasetId}"]`);
  const tagsEl = privateListWrap.querySelector(`.meta-tags[data-dataset-id="${datasetId}"]`);
  const bizDescriptionEl = privateListWrap.querySelector(`.meta-biz-description[data-dataset-id="${datasetId}"]`);
  const analysisNotesEl = privateListWrap.querySelector(`.meta-analysis-notes[data-dataset-id="${datasetId}"]`);
  const tags = String(tagsEl && tagsEl.value ? tagsEl.value : "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  setBusy(btn, true, "保存中...");
  try {
    const resp = await fetch(`/datasets/${encodeURIComponent(datasetId)}/metadata`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        alias: aliasEl ? aliasEl.value : "",
        category: categoryEl ? categoryEl.value : "",
        biz_description: bizDescriptionEl ? bizDescriptionEl.value : "",
        analysis_notes: analysisNotesEl ? analysisNotesEl.value : "",
        tags
      })
    });
    const payload = await resp.json();
    if (!resp.ok) {
      throw new Error(payload.detail || "保存失败");
    }
    repoHint.textContent = `已保存 ${payload.dataset_id} 的基础信息`;
    await loadPrivateList();
  } catch (err) {
    repoHint.textContent = `保存失败: ${err.message || err}`;
  } finally {
    setBusy(btn, false, "保存基础信息");
  }
}

async function deletePrivateDataset(datasetId, btn) {
  if (!window.confirm(`确认删除数据集 ${datasetId} ?`)) return;
  setBusy(btn, true, "删除中...");
  try {
    const resp = await fetch(`/datasets/${encodeURIComponent(datasetId)}`, { method: "DELETE" });
    const payload = await resp.json();
    if (!resp.ok) {
      throw new Error(payload.detail || "删除失败");
    }
    repoHint.textContent = `删除成功，dataset_id=${payload.dataset_id}`;
    await loadPrivateList();
  } catch (err) {
    repoHint.textContent = `删除失败: ${err.message || err}`;
  } finally {
    setBusy(btn, false, "删除数据集");
  }
}

function renderTableHtml(rows) {
  if (!Array.isArray(rows) || !rows.length) {
    return "<p class='muted'>无预览数据</p>";
  }
  const cols = Object.keys(rows[0] || {});
  const thead = `<thead><tr>${cols.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}</tr></thead>`;
  const tbody = `<tbody>${rows.map((r) => `<tr>${cols.map((c) => `<td>${escapeHtml(String(r[c] ?? ""))}</td>`).join("")}</tr>`).join("")}</tbody>`;
  return `<div class="table-wrap"><table>${thead}${tbody}</table></div>`;
}

async function openPreviewForDatasetId(datasetId, title) {
  const resp = await fetch(`/datasets/${encodeURIComponent(datasetId)}/preview?rows=10`);
  const payload = await resp.json();
  const data = payload && payload.data;
  if (!resp.ok || !data) {
    throw new Error((payload && payload.detail) || "预览失败");
  }
  const columns = Array.isArray(data.columns) ? data.columns : [];
  previewModalContent.innerHTML = `
    <h3>${escapeHtml(title || datasetId)}</h3>
    <p class="muted">${escapeHtml(datasetId)} | ${data.rows} 行 ${data.cols} 列</p>
    <div class="field-chips">${columns.map((c) => `<span class='field-chip'>${escapeHtml(c)}</span>`).join("")}</div>
    ${renderTableHtml(data.sample_rows || [])}
  `;
  previewModal.classList.remove("hidden");
}

async function openPreviewForPublic(btn) {
  const source = btn.dataset.source || "uci";
  const downloaded = btn.dataset.downloaded === "1";
  const datasetId = btn.dataset.datasetId || "";
  const name = btn.dataset.name || "";
  const desc = btn.dataset.desc || "";
  const uciId = btn.dataset.uciId || "";
  const kaggleRef = btn.dataset.kaggleRef || "";
  const pageUrl = btn.dataset.pageUrl || "";
  if (source === "kaggle") {
    if (downloaded && datasetId) {
      await openPreviewForDatasetId(datasetId, name || datasetId);
      return;
    }
    previewModalContent.innerHTML = `
      <h3>${escapeHtml(name || "Kaggle 数据集")}</h3>
      <p class="muted">Kaggle Ref: ${escapeHtml(kaggleRef || "-")}</p>
      <p>${escapeHtml(desc || "暂无描述")}</p>
      <p class="muted">该来源当前接入为目录浏览，请点击“打开 Kaggle 页面”查看与下载。</p>
      ${pageUrl ? `<p><a class="btn-link" href="${escapeAttr(pageUrl)}" target="_blank" rel="noopener noreferrer">打开 Kaggle 页面</a></p>` : ""}
    `;
    previewModal.classList.remove("hidden");
    return;
  }
  if (!downloaded || !datasetId) {
    previewModalContent.innerHTML = `
      <h3>${escapeHtml(name || "UCI 数据集")}</h3>
      <p class="muted">UCI ID: ${escapeHtml(uciId)}</p>
      <p>${escapeHtml(desc || "暂无描述")}</p>
      <p class="muted">该公开数据集尚未下载，先点击“下载到工作台”后可查看字段预览。</p>
    `;
    previewModal.classList.remove("hidden");
    return;
  }
  await openPreviewForDatasetId(datasetId, name || datasetId);
}

tabPublicBtn.addEventListener("click", async () => {
  switchTab("public");
  await loadPublicList();
});

tabPrivateBtn.addEventListener("click", async () => {
  switchTab("private");
  await loadPrivateList();
});

searchBtn.addEventListener("click", async () => {
  publicPage = 1;
  setBusy(searchBtn, true, "搜索中...");
  try {
    await loadPublicList();
  } finally {
    setBusy(searchBtn, false, "搜索");
  }
});

refreshBtn.addEventListener("click", async () => {
  setBusy(refreshBtn, true, "刷新中...");
  try {
    await loadPublicList();
  } finally {
    setBusy(refreshBtn, false, "刷新");
  }
});

publicSourceSelect.addEventListener("change", async () => {
  publicPage = 1;
  await loadPublicList();
});

keywordInput.addEventListener("keydown", async (evt) => {
  if (evt.key !== "Enter") return;
  publicPage = 1;
  await loadPublicList();
});

prevBtn.addEventListener("click", async () => {
  if (publicPage <= 1) return;
  publicPage -= 1;
  await loadPublicList();
});

nextBtn.addEventListener("click", async () => {
  if (publicPage >= publicTotalPages) return;
  publicPage += 1;
  await loadPublicList();
});

uciListWrap.addEventListener("click", async (evt) => {
  const target = evt.target;
  if (!(target instanceof HTMLElement)) return;
  if (target.classList.contains("public-import-btn")) {
    if (currentPublicSource !== "uci") return;
    const uciId = target.dataset.uciId || "";
    if (!uciId) return;
    await importPublicDataset(uciId, target);
    return;
  }
  if (target.classList.contains("public-kaggle-import-btn")) {
    if (currentPublicSource !== "kaggle") return;
    const kaggleRef = target.dataset.kaggleRef || "";
    if (!kaggleRef) return;
    await importKaggleDataset(kaggleRef, target);
    return;
  }
  if (target.classList.contains("public-preview-btn")) {
    await openPreviewForPublic(target);
  }
});

privateSearchBtn.addEventListener("click", async () => {
  setBusy(privateSearchBtn, true, "搜索中...");
  try {
    await loadPrivateList();
  } finally {
    setBusy(privateSearchBtn, false, "搜索");
  }
});

privateRefreshBtn.addEventListener("click", async () => {
  setBusy(privateRefreshBtn, true, "刷新中...");
  try {
    await loadPrivateList();
  } finally {
    setBusy(privateRefreshBtn, false, "刷新");
  }
});

privateUploadBtn.addEventListener("click", uploadPrivateDataset);

privateListWrap.addEventListener("click", async (evt) => {
  const target = evt.target;
  if (!(target instanceof HTMLElement)) return;
  const datasetId = target.dataset.datasetId || "";
  if (!datasetId) return;
  if (target.classList.contains("private-save-meta-btn")) {
    await savePrivateMetadata(datasetId, target);
    return;
  }
  if (target.classList.contains("private-delete-btn")) {
    await deletePrivateDataset(datasetId, target);
    return;
  }
  if (target.classList.contains("private-preview-btn")) {
    await openPreviewForDatasetId(datasetId, datasetId);
  }
});

closePreviewModalBtn.addEventListener("click", () => {
  previewModal.classList.add("hidden");
  previewModalContent.innerHTML = "";
});

previewModal.addEventListener("click", (evt) => {
  if (evt.target === previewModal) {
    previewModal.classList.add("hidden");
    previewModalContent.innerHTML = "";
  }
});

(async function bootstrap() {
  try {
    await loadPublicSources();
    switchTab("public");
    await loadPublicList();
    await loadPrivateList();
  } catch (err) {
    repoHint.textContent = `初始化失败: ${err.message || err}`;
  }
})();
