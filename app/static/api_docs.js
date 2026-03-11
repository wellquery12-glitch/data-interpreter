const docsHint = document.getElementById("docsHint");
const docsTableBody = document.getElementById("docsTableBody");
const previewHint = document.getElementById("previewHint");
const docsPreviewFrame = document.getElementById("docsPreviewFrame");
const refreshDocsBtn = document.getElementById("refreshDocsBtn");

function statusTag(status) {
  const v = String(status || "").toLowerCase();
  if (v === "ok") return '<span class="status-tag ok">在线</span>';
  if (v === "down") return '<span class="status-tag error">离线</span>';
  if (v === "disabled") return '<span class="status-tag warn">未启用</span>';
  return `<span class="status-tag warn">${v || "未知"}</span>`;
}

function openPreview(url, name) {
  docsPreviewFrame.src = url;
  previewHint.textContent = `当前预览：${name}`;
}

async function loadDocs() {
  docsHint.textContent = "加载中...";
  let services = [];
  let topo = [];
  try {
    const resp = await fetch("/api/docs/services");
    const data = await resp.json();
    services = Array.isArray(data.services) ? data.services : [];
  } catch (err) {
    docsHint.textContent = err.message || "加载文档列表失败";
    return;
  }

  try {
    const resp = await fetch("/architecture/services");
    const data = await resp.json();
    topo = Array.isArray(data.services) ? data.services : [];
  } catch (_) {
    topo = [];
  }

  const statusMap = {};
  for (const s of topo) {
    statusMap[String(s.name || "")] = String(s.status || "unknown");
  }

  if (!services.length) {
    docsHint.textContent = "暂无可用服务";
    docsTableBody.innerHTML = '<tr><td colspan="4" class="muted">暂无数据</td></tr>';
    return;
  }

  docsTableBody.innerHTML = services
    .map((s) => {
      const status = statusMap[s.name] || "unknown";
      const docsUrl = s.docs_url || "";
      const openCell = docsUrl
        ? `<a class="btn-link" href="${docsUrl}" target="_blank" rel="noreferrer">打开</a>
            <button type="button" class="preview-btn" data-url="${docsUrl.replace(/"/g, "&quot;")}" data-name="${(s.name || "").replace(/"/g, "&quot;")}">预览</button>`
        : '<span class="muted">未配置</span>';
      return `
        <tr>
          <td>${s.name || ""}</td>
          <td>${statusTag(status)}</td>
          <td>${s.base_url || ""}</td>
          <td>${openCell}</td>
        </tr>
      `;
    })
    .join("");
  docsHint.textContent = "已加载";

  const btns = docsTableBody.querySelectorAll(".preview-btn");
  btns.forEach((btn) => {
    btn.addEventListener("click", () => {
      const url = btn.getAttribute("data-url") || "";
      const name = btn.getAttribute("data-name") || "";
      openPreview(url, name);
    });
  });
}

refreshDocsBtn.addEventListener("click", () => {
  loadDocs().catch((err) => {
    docsHint.textContent = err.message || "刷新失败";
  });
});

loadDocs().catch((err) => {
  docsHint.textContent = err.message || "初始化失败";
});
