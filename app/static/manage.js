const enabledInput = document.getElementById("enabledInput");
const providerInput = document.getElementById("providerInput");
const modelInput = document.getElementById("modelInput");
const baseUrlInput = document.getElementById("baseUrlInput");
const apiKeyInput = document.getElementById("apiKeyInput");
const maxTokensInput = document.getElementById("maxTokensInput");
const warnRatioInput = document.getElementById("warnRatioInput");
const inPriceInput = document.getElementById("inPriceInput");
const outPriceInput = document.getElementById("outPriceInput");
const currencyInput = document.getElementById("currencyInput");
const saveBtn = document.getElementById("saveBtn");
const resetBtn = document.getElementById("resetBtn");
const saveHint = document.getElementById("saveHint");
const quotaStatus = document.getElementById("quotaStatus");
const quotaMessage = document.getElementById("quotaMessage");
const quotaNumbers = document.getElementById("quotaNumbers");
const usageSummary = document.getElementById("usageSummary");
const eventsBody = document.getElementById("eventsBody");
const refreshServicesBtn = document.getElementById("refreshServicesBtn");
const servicesHint = document.getElementById("servicesHint");
const servicesBody = document.getElementById("servicesBody");
const refreshBugsBtn = document.getElementById("refreshBugsBtn");
const bugsHint = document.getElementById("bugsHint");
const bugsBody = document.getElementById("bugsBody");

function num(v, fallback) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function money(v) {
  return Number(v || 0).toFixed(6);
}

function setQuotaTag(quota) {
  quotaStatus.className = "status-tag";
  if (quota.exhausted) {
    quotaStatus.classList.add("error");
    quotaStatus.textContent = "已耗尽";
    return;
  }
  if (quota.warning) {
    quotaStatus.classList.add("warn");
    quotaStatus.textContent = "接近阈值";
    return;
  }
  if (!quota.enabled) {
    quotaStatus.classList.add("warn");
    quotaStatus.textContent = "已禁用";
    return;
  }
  quotaStatus.classList.add("ok");
  quotaStatus.textContent = "正常";
}

function render(data) {
  const cfg = data.config || {};
  const usage = data.usage || {};
  const quota = data.quota || {};

  enabledInput.checked = !!cfg.enabled;
  providerInput.value = cfg.provider || "openai_compatible";
  modelInput.value = cfg.model || "";
  baseUrlInput.value = cfg.base_url || "";
  apiKeyInput.value = cfg.api_key || "";
  maxTokensInput.value = cfg.max_total_tokens || 500000;
  warnRatioInput.value = cfg.warn_ratio || 0.8;
  inPriceInput.value = cfg.input_cost_per_million || 0;
  outPriceInput.value = cfg.output_cost_per_million || 0;
  currencyInput.value = cfg.currency || "USD";

  setQuotaTag(quota);
  quotaMessage.textContent = quota.message || "";
  quotaNumbers.textContent = `已使用 ${quota.used_tokens || 0} / ${quota.max_total_tokens || 0} tokens（${((quota.used_ratio || 0) * 100).toFixed(2)}%）`;

  usageSummary.innerHTML = `
    <tr><td>Prompt Tokens</td><td>${usage.total_prompt_tokens || 0}</td></tr>
    <tr><td>Completion Tokens</td><td>${usage.total_completion_tokens || 0}</td></tr>
    <tr><td>Total Tokens</td><td>${usage.total_tokens || 0}</td></tr>
    <tr><td>Total Cost</td><td>${money(usage.total_cost)} ${cfg.currency || "USD"}</td></tr>
    <tr><td>最近更新时间</td><td>${usage.updated_at || "-"}</td></tr>
  `;

  const rows = (usage.events || []).slice(-20).reverse();
  if (!rows.length) {
    eventsBody.innerHTML = `<tr><td colspan="8" class="muted">暂无链路消耗记录</td></tr>`;
    return;
  }
  eventsBody.innerHTML = rows
    .map((r) => {
      return `
        <tr>
          <td>${r.time || ""}</td>
          <td>${r.mode || ""}</td>
          <td>${r.stage || ""}</td>
          <td>${r.model || ""}</td>
          <td>${r.prompt_tokens || 0}</td>
          <td>${r.completion_tokens || 0}</td>
          <td>${r.total_tokens || 0}</td>
          <td>${money(r.cost)} ${cfg.currency || "USD"}</td>
        </tr>
      `;
    })
    .join("");
}

function statusTag(status) {
  const v = String(status || "").toLowerCase();
  if (v === "ok") return '<span class="status-tag ok">在线</span>';
  if (v === "down") return '<span class="status-tag error">离线</span>';
  if (v === "disabled") return '<span class="status-tag warn">未启用</span>';
  return `<span class="status-tag warn">${v || "-"}</span>`;
}

function renderServices(gateway, topology) {
  const rows = [];
  rows.push({
    name: "frontend-gateway",
    role: "前端页面/API 入口",
    status: gateway && gateway.status ? gateway.status : "down",
    url: window.location.origin,
    message: gateway && gateway.orchestrator ? `orchestrator=${gateway.orchestrator}` : ""
  });

  const services = topology && Array.isArray(topology.services) ? topology.services : [];
  for (const s of services) {
    rows.push({
      name: s.name || "-",
      role: s.role || "",
      status: s.status || "unknown",
      url: s.url || "",
      message: s.message || ""
    });
  }

  if (!rows.length) {
    servicesBody.innerHTML = '<tr><td colspan="5" class="muted">暂无服务信息</td></tr>';
    return;
  }

  servicesBody.innerHTML = rows
    .map((r) => {
      return `
        <tr>
          <td>${r.name}</td>
          <td>${r.role}</td>
          <td>${statusTag(r.status)}</td>
          <td>${r.url || "-"}</td>
          <td>${r.message || ""}</td>
        </tr>
      `;
    })
    .join("");
}

function renderBugs(data) {
  const rows = data && Array.isArray(data.items) ? data.items : [];
  if (!rows.length) {
    bugsBody.innerHTML = '<tr><td colspan="6" class="muted">暂无错误记录</td></tr>';
    bugsHint.textContent = "暂无错误记录";
    return;
  }
  bugsBody.innerHTML = rows
    .map((r) => {
      const fixed = !!r.fixed;
      const tag = fixed ? '<span class="status-tag ok">已修复</span>' : '<span class="status-tag error">未修复</span>';
      return `
        <tr>
          <td>${tag}</td>
          <td>${r.intent_key || ""}</td>
          <td>${r.error_sample || ""}</td>
          <td>${r.count || 0}</td>
          <td>${r.last_time || ""}</td>
          <td>${r.fixed_time || "-"}</td>
        </tr>
      `;
    })
    .join("");
  bugsHint.textContent = `已加载 ${rows.length} 条聚合错误`;
}

async function loadConfig() {
  const resp = await fetch("/llm/config");
  const data = await resp.json();
  if (!resp.ok) {
    throw new Error("加载配置失败");
  }
  render(data);
}

async function loadServices() {
  let gateway = null;
  let topology = null;
  let errMsg = "";
  try {
    const r1 = await fetch("/health");
    gateway = await r1.json();
    if (!r1.ok) throw new Error("gateway health failed");
  } catch (err) {
    errMsg = err.message || "gateway unavailable";
  }
  try {
    const r2 = await fetch("/architecture/services");
    topology = await r2.json();
    if (!r2.ok) throw new Error("topology failed");
  } catch (err) {
    errMsg = err.message || errMsg || "topology unavailable";
  }
  renderServices(gateway, topology);
  servicesHint.textContent = errMsg ? `部分服务状态获取失败: ${errMsg}` : "状态已更新";
}

async function loadBugs() {
  const resp = await fetch("/bugs/summary?limit=60");
  const data = await resp.json();
  if (!resp.ok) {
    throw new Error("加载错误统计失败");
  }
  renderBugs(data);
}

async function saveConfig() {
  const payload = {
    enabled: !!enabledInput.checked,
    provider: providerInput.value.trim() || "openai_compatible",
    model: modelInput.value.trim(),
    base_url: baseUrlInput.value.trim(),
    api_key: apiKeyInput.value.trim(),
    max_total_tokens: Math.max(1000, Math.floor(num(maxTokensInput.value, 500000))),
    warn_ratio: Math.min(0.99, Math.max(0.1, num(warnRatioInput.value, 0.8))),
    input_cost_per_million: Math.max(0, num(inPriceInput.value, 0)),
    output_cost_per_million: Math.max(0, num(outPriceInput.value, 0)),
    currency: currencyInput.value.trim() || "USD"
  };
  const resp = await fetch("/llm/config", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const data = await resp.json();
  if (!resp.ok) {
    throw new Error("保存失败");
  }
  render(data);
  saveHint.textContent = "配置已保存";
}

async function resetUsage() {
  const resp = await fetch("/llm/reset", { method: "POST" });
  const data = await resp.json();
  if (!resp.ok) {
    throw new Error("重置失败");
  }
  render(data);
  saveHint.textContent = "资源统计已重置";
}

saveBtn.addEventListener("click", async () => {
  saveHint.textContent = "保存中...";
  try {
    await saveConfig();
  } catch (err) {
    saveHint.textContent = err.message || "保存失败";
  }
});

resetBtn.addEventListener("click", async () => {
  saveHint.textContent = "重置中...";
  try {
    await resetUsage();
  } catch (err) {
    saveHint.textContent = err.message || "重置失败";
  }
});

if (refreshServicesBtn) {
  refreshServicesBtn.addEventListener("click", async () => {
    servicesHint.textContent = "刷新中...";
    await loadServices();
  });
}

if (refreshBugsBtn) {
  refreshBugsBtn.addEventListener("click", async () => {
    bugsHint.textContent = "刷新中...";
    try {
      await loadBugs();
    } catch (err) {
      bugsHint.textContent = err.message || "刷新失败";
    }
  });
}

loadConfig().catch((err) => {
  saveHint.textContent = err.message || "初始化失败";
});
loadServices().catch((err) => {
  servicesHint.textContent = err.message || "服务状态初始化失败";
});
loadBugs().catch((err) => {
  bugsHint.textContent = err.message || "错误统计初始化失败";
});
