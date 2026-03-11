const sourceUrlInput = document.getElementById("sourceUrlInput");
const packageIdInput = document.getElementById("packageIdInput");
const installPkgBtn = document.getElementById("installPkgBtn");
const refreshCatalogBtn = document.getElementById("refreshCatalogBtn");
const pkgHint = document.getElementById("pkgHint");
const catalogBody = document.getElementById("catalogBody");
const packageSelectInput = document.getElementById("packageSelectInput");
const toolIdInput = document.getElementById("toolIdInput");
const toolNameInput = document.getElementById("toolNameInput");
const toolDescInput = document.getElementById("toolDescInput");
const toolEndpointInput = document.getElementById("toolEndpointInput");
const toolKeywordsInput = document.getElementById("toolKeywordsInput");
const addToolBtn = document.getElementById("addToolBtn");
const installedToolsBody = document.getElementById("installedToolsBody");
const saveSelectionBtn = document.getElementById("saveSelectionBtn");
const refreshStateBtn = document.getElementById("refreshStateBtn");
const toolsHint = document.getElementById("toolsHint");

let currentState = { tools: [], selected_tool_ids: [] };

function safeJson(v) {
  try {
    return JSON.stringify(v || {});
  } catch {
    return "{}";
  }
}

function renderCatalog(packages) {
  if (!packages.length) {
    catalogBody.innerHTML = `<tr><td colspan="4" class="muted">暂无可用工具包</td></tr>`;
    return;
  }
  catalogBody.innerHTML = packages
    .map(
      (p) => `
      <tr>
        <td>${p.package_id || ""}</td>
        <td>${p.name || ""}</td>
        <td>${p.description || ""}</td>
        <td>${p.version || ""}</td>
      </tr>
    `
    )
    .join("");
}

function renderInstalled(state) {
  currentState = state || { tools: [], selected_tool_ids: [] };
  const selected = new Set(currentState.selected_tool_ids || []);
  const tools = currentState.tools || [];
  if (!tools.length) {
    installedToolsBody.innerHTML = `<tr><td colspan="7" class="muted">暂无工具，请先安装工具包或手动添加</td></tr>`;
    return;
  }
  installedToolsBody.innerHTML = tools
    .map((t) => {
      return `
        <tr>
          <td><input type="checkbox" class="tool-select" data-id="${t.tool_id}" ${selected.has(t.tool_id) ? "checked" : ""} /></td>
          <td><input type="checkbox" class="tool-enable" data-id="${t.tool_id}" ${t.enabled ? "checked" : ""} /></td>
          <td>${t.tool_id || ""}</td>
          <td>${t.name || ""}</td>
          <td>${t.description || ""}</td>
          <td><pre>${safeJson(t.input_schema)}</pre></td>
          <td><pre>${safeJson(t.output_schema)}</pre></td>
        </tr>
      `;
    })
    .join("");
  renderPackageOptions();
}

function renderPackageOptions() {
  const pkgs = currentState.packages || [];
  if (!pkgs.length) {
    packageSelectInput.innerHTML = `<option value="manual">manual</option>`;
    return;
  }
  packageSelectInput.innerHTML = pkgs
    .map((p) => `<option value="${p.package_id || "manual"}">${p.name || p.package_id || "manual"} (${p.package_id || "manual"})</option>`)
    .join("");
}

async function loadState() {
  const resp = await fetch("/tools/state");
  const data = await resp.json();
  if (!resp.ok) throw new Error("加载工具状态失败");
  renderInstalled(data);
}

async function loadCatalog() {
  const sourceUrl = sourceUrlInput.value.trim();
  const url = sourceUrl ? `/tools/catalog?source_url=${encodeURIComponent(sourceUrl)}` : "/tools/catalog";
  const resp = await fetch(url);
  const data = await resp.json();
  if (!resp.ok) throw new Error((data && data.detail) || "拉取工具包失败");
  renderCatalog(data.packages || []);
}

async function installPackage() {
  const source_url = sourceUrlInput.value.trim();
  const package_id = packageIdInput.value.trim();
  if (!source_url || !package_id) throw new Error("工具平台地址和工具包ID都必填");
  const resp = await fetch("/tools/packages/install", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ source_url, package_id })
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error((data && data.detail) || "安装失败");
  renderInstalled(data);
}

async function addTool() {
  const tool_id = toolIdInput.value.trim();
  const name = toolNameInput.value.trim();
  const endpoint = toolEndpointInput.value.trim();
  if (!tool_id || !name || !endpoint) throw new Error("tool_id / name / endpoint 必填");
  const keywords = toolKeywordsInput.value
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const payload = {
    tool_id,
    name,
    package_id: packageSelectInput.value || "manual",
    description: toolDescInput.value.trim(),
    endpoint,
    method: "POST",
    keywords,
    input_schema: { question: "string", data: "object" },
    output_schema: { answer: "string", table: "array" }
  };
  const resp = await fetch("/tools/add", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error((data && data.detail) || "添加工具失败");
  renderInstalled(data);
}

async function saveSelection() {
  const checked = Array.from(document.querySelectorAll(".tool-select:checked")).map((el) => el.dataset.id || "");
  const resp = await fetch("/tools/select", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ selected_tool_ids: checked })
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error("保存选择失败");
  renderInstalled(data);
}

installedToolsBody.addEventListener("change", async (event) => {
  const target = event.target;
  if (!target.classList.contains("tool-enable")) return;
  const toolId = target.dataset.id || "";
  const enabled = !!target.checked;
  try {
    const resp = await fetch(`/tools/${encodeURIComponent(toolId)}/enabled`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled })
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error("更新启用状态失败");
    renderInstalled(data);
    toolsHint.textContent = "启用状态已更新";
  } catch (err) {
    toolsHint.textContent = err.message || "更新失败";
  }
});

installPkgBtn.addEventListener("click", async () => {
  pkgHint.textContent = "安装中...";
  try {
    await installPackage();
    pkgHint.textContent = "工具包安装成功";
  } catch (err) {
    pkgHint.textContent = err.message || "安装失败";
  }
});

refreshCatalogBtn.addEventListener("click", async () => {
  pkgHint.textContent = "拉取中...";
  try {
    await loadCatalog();
    pkgHint.textContent = "拉取成功";
  } catch (err) {
    pkgHint.textContent = err.message || "拉取失败";
  }
});

addToolBtn.addEventListener("click", async () => {
  toolsHint.textContent = "添加中...";
  try {
    await addTool();
    toolsHint.textContent = "工具添加成功";
  } catch (err) {
    toolsHint.textContent = err.message || "添加失败";
  }
});

saveSelectionBtn.addEventListener("click", async () => {
  toolsHint.textContent = "保存中...";
  try {
    await saveSelection();
    toolsHint.textContent = "工具选择已保存";
  } catch (err) {
    toolsHint.textContent = err.message || "保存失败";
  }
});

refreshStateBtn.addEventListener("click", async () => {
  toolsHint.textContent = "刷新中...";
  try {
    await loadState();
    toolsHint.textContent = "已刷新";
  } catch (err) {
    toolsHint.textContent = err.message || "刷新失败";
  }
});

loadState().catch((err) => {
  toolsHint.textContent = err.message || "初始化失败";
});

loadCatalog().catch(() => {});
