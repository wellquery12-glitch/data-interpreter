const includePytestInput = document.getElementById("includePytestInput");
const runTestsBtn = document.getElementById("runTestsBtn");
const refreshRunsBtn = document.getElementById("refreshRunsBtn");
const testHint = document.getElementById("testHint");
const runsBody = document.getElementById("runsBody");
const detailHint = document.getElementById("detailHint");
const detailWrap = document.getElementById("detailWrap");

function setBusy(btn, busy, text) {
  btn.disabled = busy;
  if (busy) btn.dataset.old = btn.textContent;
  btn.textContent = busy ? text : (btn.dataset.old || btn.textContent);
}

function statusTag(status) {
  const s = String(status || "").toLowerCase();
  if (s === "pass") return '<span class="status-tag ok">通过</span>';
  if (s === "skipped") return '<span class="status-tag warn">跳过</span>';
  return '<span class="status-tag error">失败</span>';
}

function escapeHtml(v) {
  return String(v || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

async function runTests() {
  const resp = await fetch("/tests/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ include_pytest: !!includePytestInput.checked }),
  });
  const data = await resp.json();
  if (!resp.ok) {
    throw new Error((data && data.detail) || "执行失败");
  }
  return data;
}

async function loadRuns() {
  const resp = await fetch("/tests/runs?limit=30");
  const data = await resp.json();
  if (!resp.ok) {
    throw new Error((data && data.detail) || "加载历史失败");
  }
  const rows = Array.isArray(data.items) ? data.items : [];
  if (!rows.length) {
    runsBody.innerHTML = "<tr><td colspan='7' class='muted'>暂无测试记录</td></tr>";
    return;
  }
  runsBody.innerHTML = rows
    .map((r) => {
      return `
        <tr>
          <td>${escapeHtml(r.time || "")}</td>
          <td>${statusTag(r.status)}</td>
          <td>${Number(r.duration_ms || 0)}</td>
          <td>${Number(r.pass_count || 0)}</td>
          <td>${Number(r.fail_count || 0)}</td>
          <td>${Number(r.skip_count || 0)}</td>
          <td><button class="view-run-btn" data-run-id="${escapeHtml(r.run_id || "")}" type="button">查看详情</button></td>
        </tr>
      `;
    })
    .join("");
}

function renderRunDetail(data) {
  const items = Array.isArray(data.items) ? data.items : [];
  detailHint.textContent = `run_id=${data.run_id || ""} | 状态=${data.status || ""} | 耗时=${Number(data.duration_ms || 0)}ms`;
  if (!items.length) {
    detailWrap.innerHTML = "<p class='muted'>无明细</p>";
    return;
  }
  detailWrap.innerHTML = items
    .map((it) => {
      return `
        <details open>
          <summary>${escapeHtml(it.name || "-")} | ${statusTag(it.status)} | ${escapeHtml(it.cmd || "")}</summary>
          <p class="muted">返回码: ${Number(it.returncode || 0)} | 耗时: ${Number(it.duration_ms || 0)}ms</p>
          <pre>${escapeHtml(it.output || "")}</pre>
        </details>
      `;
    })
    .join("");
}

async function loadRunDetail(runId) {
  const resp = await fetch(`/tests/runs/${encodeURIComponent(runId)}`);
  const payload = await resp.json();
  if (!resp.ok) {
    throw new Error((payload && payload.detail) || "加载详情失败");
  }
  renderRunDetail(payload.data || {});
}

runTestsBtn.addEventListener("click", async () => {
  setBusy(runTestsBtn, true, "执行中...");
  testHint.textContent = "自动测试执行中...";
  try {
    const out = await runTests();
    testHint.textContent = `测试完成：${out.status}（通过 ${out.pass_count} / 失败 ${out.fail_count} / 跳过 ${out.skip_count}）`;
    await loadRuns();
    if (out.run_id) {
      await loadRunDetail(out.run_id);
    }
  } catch (err) {
    testHint.textContent = err.message || "执行失败";
  } finally {
    setBusy(runTestsBtn, false, "执行自动测试");
  }
});

refreshRunsBtn.addEventListener("click", async () => {
  setBusy(refreshRunsBtn, true, "刷新中...");
  try {
    await loadRuns();
    testHint.textContent = "历史已刷新";
  } catch (err) {
    testHint.textContent = err.message || "刷新失败";
  } finally {
    setBusy(refreshRunsBtn, false, "刷新历史");
  }
});

runsBody.addEventListener("click", async (evt) => {
  const target = evt.target;
  if (!(target instanceof HTMLElement)) return;
  if (!target.classList.contains("view-run-btn")) return;
  const runId = target.dataset.runId || "";
  if (!runId) return;
  detailHint.textContent = "详情加载中...";
  try {
    await loadRunDetail(runId);
  } catch (err) {
    detailHint.textContent = err.message || "详情加载失败";
  }
});

loadRuns().catch((err) => {
  testHint.textContent = err.message || "初始化失败";
});
