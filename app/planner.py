from __future__ import annotations

import datetime as dt
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class DatasetMeta:
    columns: List[str]
    dtypes: Dict[str, str]


class CodePlanner:
    """Generate executable pandas code from natural language questions.

    This MVP uses deterministic rules so it works offline.
    """

    def __init__(self) -> None:
        self.use_llm = os.getenv("USE_LLM_PLANNER", "1") != "0"
        self.llm_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self._llm_runtime: Dict[str, Any] = {}
        self._llm_trace: List[Dict[str, Any]] = []
        self._tool_context: List[Dict[str, Any]] = []

    def begin_trace(self) -> None:
        self._llm_trace = []

    def consume_trace(self) -> List[Dict[str, Any]]:
        out = [dict(v) for v in self._llm_trace]
        self._llm_trace = []
        return out

    def set_llm_runtime(self, config: Optional[Dict[str, Any]] = None, allow_llm: bool = True, deny_reason: str = "") -> None:
        cfg = config or {}
        self._llm_runtime = {
            "allow_llm": bool(allow_llm),
            "deny_reason": str(deny_reason or ""),
            "enabled": bool(cfg.get("enabled", True)),
            "api_key": str(cfg.get("api_key", "")).strip(),
            "base_url": str(cfg.get("base_url", "")).strip(),
            "model": str(cfg.get("model", "")).strip() or self.llm_model,
            "input_cost_per_million": float(cfg.get("input_cost_per_million", 0.0) or 0.0),
            "output_cost_per_million": float(cfg.get("output_cost_per_million", 0.0) or 0.0),
        }

    def set_tool_context(self, tools: Optional[List[Dict[str, Any]]] = None) -> None:
        rows = tools or []
        cleaned: List[Dict[str, Any]] = []
        for item in rows[:20]:
            if not isinstance(item, dict):
                continue
            cleaned.append(
                {
                    "tool_id": str(item.get("tool_id", "")),
                    "name": str(item.get("name", "")),
                    "description": str(item.get("description", "")),
                    "input_schema": item.get("input_schema", {}) if isinstance(item.get("input_schema"), dict) else {},
                    "output_schema": item.get("output_schema", {}) if isinstance(item.get("output_schema"), dict) else {},
                }
            )
        self._tool_context = cleaned

    def _tool_context_prompt(self) -> str:
        if not self._tool_context:
            return ""
        lines: List[str] = ["可用远程工具（用户已选择，可在生成方案时优先考虑）："]
        for t in self._tool_context:
            lines.append(
                f"- {t.get('tool_id')} | {t.get('name')} | {t.get('description')} | "
                f"input={json.dumps(t.get('input_schema', {}), ensure_ascii=False)} | "
                f"output={json.dumps(t.get('output_schema', {}), ensure_ascii=False)}"
            )
        return "\n".join(lines) + "\n"

    def generate(self, question: str, meta: DatasetMeta) -> str:
        llm = self._generate_llm_code(question=question, meta=meta, error="")
        if llm:
            return llm
        return self._generate_rule_based(question=question, meta=meta)

    def generate_llm_only(self, question: str, meta: DatasetMeta, error: str = "") -> Optional[str]:
        return self._generate_llm_code(question=question, meta=meta, error=error)

    def llm_available(self) -> bool:
        if self._llm_runtime and not bool(self._llm_runtime.get("allow_llm", True)):
            return False
        if self._llm_runtime and not bool(self._llm_runtime.get("enabled", True)):
            return False
        if not self.use_llm:
            return False
        api_key = str(self._llm_runtime.get("api_key", "")).strip() or os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            return False
        try:
            import openai  # type: ignore  # noqa: F401
        except Exception:  # noqa: BLE001
            return False
        return True

    def _generate_rule_based(self, question: str, meta: DatasetMeta) -> str:
        q = question.strip().lower()
        cols = meta.columns
        chart_requested = any(k in question for k in ["图", "图表", "可视化", "柱状", "饼图", "条形"])

        # Common query: top N frequency by a column (e.g. "哪个司机运单最多")
        if ("最多" in question or "top" in q) and any(k in question for k in ["司机", "driver", "shipper", "发货"]):
            target = self._pick_column(cols, ["driver", "司机", "shipper", "发货方", "承运"])
            if target:
                return self._code_top_frequency(target)

        group_col = self._pick_column(cols, ["交易对象", "交易对方", "对象", "对方", "客户", "商户"])
        amount_col = self._pick_column(cols, ["金额", "amount", "金额(元)", "金额（元）"]) or self._pick_numeric_like(cols)

        # Group-by amount question (e.g. "哪个交易对象的金额最多")
        if ("最多" in question or "top" in q) and any(k in question for k in ["金额", "amount"]) and group_col and amount_col:
            return self._code_group_sum_top(group_col, amount_col, with_plot=chart_requested)

        # Numeric aggregate queries
        if any(k in question for k in ["总", "合计", "sum", "累计"]):
            num_col = self._pick_numeric_like(cols)
            if num_col:
                return self._code_sum(num_col)

        if any(k in question for k in ["平均", "avg", "mean"]):
            num_col = self._pick_numeric_like(cols)
            if num_col:
                return self._code_mean(num_col)

        # Trend plot request
        if any(k in question for k in ["趋势", "折线", "line", "曲线"]):
            date_col = self._pick_column(cols, ["date", "time", "日期", "时间", "created"])
            num_col = self._pick_numeric_like(cols)
            if date_col and num_col:
                return self._code_trend(date_col, num_col)
            if num_col:
                return self._code_trend_by_index(num_col)

        # Fallback: descriptive summary
        return self.fallback_code()

    def fallback_code(self) -> str:
        return self._code_profile()

    def is_fallback_code(self, code: str) -> bool:
        return code.strip() == self.fallback_code().strip()

    def intent_key(self, question: str) -> str:
        q = question.strip().lower()
        chart_requested = any(k in question for k in ["图", "图表", "可视化", "柱状", "饼图", "条形"])
        if any(k in q for k in ["司机", "driver"]) and any(k in q for k in ["最多", "top"]):
            return "driver_top_frequency"
        if any(k in q for k in ["交易对象", "交易对方", "amount", "金额"]) and any(k in q for k in ["最多", "top"]):
            suffix = ""
            if chart_requested:
                suffix += "_plot"
            return "counterparty_amount_top" + suffix
        if any(k in q for k in ["趋势", "折线", "line", "曲线"]):
            return "trend_plot"
        if any(k in q for k in ["平均", "avg", "mean"]):
            return "mean_aggregate"
        if any(k in q for k in ["总", "合计", "sum", "累计"]):
            return "sum_aggregate"
        normalized = re.sub(r"\d+", "<num>", q)
        normalized = re.sub(r"\s+", "", normalized)
        normalized = re.sub(r"[^\w\u4e00-\u9fff]+", "_", normalized).strip("_")
        return "q_" + (normalized[:48] or "unknown")

    def generate_auto_tool(self, question: str, meta: DatasetMeta) -> Optional[str]:
        cols = meta.columns
        q = question.lower()
        if any(k in question for k in ["最大", "max", "最高"]):
            col = self._pick_numeric_like(cols)
            if col:
                return self._code_max(col)
        if any(k in question for k in ["最小", "min", "最低"]):
            col = self._pick_numeric_like(cols)
            if col:
                return self._code_min(col)
        if any(k in question for k in ["去重", "distinct", "唯一", "几种", "多少种"]):
            col = self._pick_column(cols, self._query_keywords(question))
            if not col and cols:
                col = cols[0]
            if col:
                return self._code_distinct_count(col)
        if any(k in q for k in ["前", "top", "最多"]) and cols:
            col = self._pick_column(cols, self._query_keywords(question)) or cols[0]
            return self._code_top_frequency(col)
        return None

    def repair_code(self, question: str, meta: DatasetMeta, error: str) -> str:
        llm = self._generate_llm_code(question=question, meta=meta, error=error or "")
        if llm:
            return llm
        e = (error or "").lower()
        if "keyerror" in e or "not found in axis" in e:
            auto = self.generate_auto_tool(question, meta)
            if auto:
                return auto
        if "could not convert" in e or "no numeric types" in e or "unsupported operand" in e:
            num_col = self._pick_numeric_like(meta.columns)
            if num_col:
                if any(k in question for k in ["平均", "avg", "mean"]):
                    return self._code_mean(num_col)
                return self._code_sum(num_col)
        auto = self.generate_auto_tool(question, meta)
        return auto or self.fallback_code()

    def plan_tool(self, question: str, meta: DatasetMeta) -> Dict[str, Any]:
        llm_plan = self._plan_tool_llm(question=question, meta=meta)
        if llm_plan:
            return llm_plan
        return self._plan_tool_rule(question=question, meta=meta)

    def build_tool_code(self, plan: Dict[str, Any], meta: DatasetMeta) -> str:
        tool = str(plan.get("tool", "profile"))
        args = plan.get("args", {}) if isinstance(plan.get("args"), dict) else {}
        cols = meta.columns

        if tool == "top_frequency":
            col = str(args.get("col", "")).strip() or (cols[0] if cols else "")
            return self._code_top_frequency(col) if col else self.fallback_code()

        if tool == "group_sum_top":
            g = str(args.get("group_col", "")).strip()
            a = str(args.get("amount_col", "")).strip()
            w = bool(args.get("with_plot", False))
            if g and a:
                return self._code_group_sum_top(g, a, with_plot=w)
            return self.fallback_code()

        if tool == "aggregate":
            op = str(args.get("op", "sum")).lower()
            col = str(args.get("col", "")).strip() or self._pick_numeric_like(cols) or ""
            if not col:
                return self.fallback_code()
            if op == "max":
                return self._code_max(col)
            if op == "min":
                return self._code_min(col)
            if op == "mean":
                return self._code_mean(col)
            return self._code_sum(col)

        if tool == "trend_plot":
            d = str(args.get("date_col", "")).strip()
            n = str(args.get("num_col", "")).strip()
            if d and n:
                return self._code_trend(d, n)
            if n:
                return self._code_trend_by_index(n)
            return self.fallback_code()

        if tool == "distinct_count":
            col = str(args.get("col", "")).strip() or (cols[0] if cols else "")
            return self._code_distinct_count(col) if col else self.fallback_code()

        return self.fallback_code()

    def _plan_tool_rule(self, question: str, meta: DatasetMeta) -> Dict[str, Any]:
        q = question.strip().lower()
        cols = meta.columns
        chart_requested = any(k in question for k in ["图", "图表", "可视化", "柱状", "饼图", "条形"])
        group_col = self._pick_column(cols, ["交易对象", "交易对方", "对象", "对方", "客户", "商户"])
        amount_col = self._pick_column(cols, ["金额", "amount", "金额(元)", "金额（元）"]) or self._pick_numeric_like(cols)

        if ("最多" in question or "top" in q) and any(k in question for k in ["司机", "driver", "shipper", "发货"]):
            target = self._pick_column(cols, ["driver", "司机", "shipper", "发货方", "承运"])
            if target:
                return {"tool": "top_frequency", "args": {"col": target}}

        if ("最多" in question or "top" in q) and any(k in question for k in ["金额", "amount"]) and group_col and amount_col:
            return {"tool": "group_sum_top", "args": {"group_col": group_col, "amount_col": amount_col, "with_plot": chart_requested}}

        if any(k in question for k in ["趋势", "折线", "line", "曲线"]):
            date_col = self._pick_column(cols, ["date", "time", "日期", "时间", "created"])
            num_col = self._pick_numeric_like(cols)
            if date_col and num_col:
                return {"tool": "trend_plot", "args": {"date_col": date_col, "num_col": num_col}}
            if num_col:
                return {"tool": "trend_plot", "args": {"date_col": "", "num_col": num_col, "use_index": True}}

        if any(k in question for k in ["最大", "max", "最高"]):
            col = self._pick_numeric_like(cols)
            if col:
                return {"tool": "aggregate", "args": {"op": "max", "col": col}}
        if any(k in question for k in ["最小", "min", "最低"]):
            col = self._pick_numeric_like(cols)
            if col:
                return {"tool": "aggregate", "args": {"op": "min", "col": col}}
        if any(k in question for k in ["平均", "avg", "mean"]):
            col = self._pick_numeric_like(cols)
            if col:
                return {"tool": "aggregate", "args": {"op": "mean", "col": col}}
        if any(k in question for k in ["总", "合计", "sum", "累计"]):
            col = self._pick_numeric_like(cols)
            if col:
                return {"tool": "aggregate", "args": {"op": "sum", "col": col}}

        if any(k in question for k in ["去重", "distinct", "唯一", "几种", "多少种"]):
            col = self._pick_column(cols, self._query_keywords(question)) or (cols[0] if cols else "")
            if col:
                return {"tool": "distinct_count", "args": {"col": col}}

        return {"tool": "profile", "args": {}}

    def _plan_tool_llm(self, question: str, meta: DatasetMeta) -> Optional[Dict[str, Any]]:
        if not self.llm_available():
            return None

        schema = ", ".join([f"{c}({meta.dtypes.get(c, 'unknown')})" for c in meta.columns[:60]])
        prompt = (
            "将用户问题解析为 JSON，不要解释。\n"
            "可选 tool: top_frequency, group_sum_top, aggregate, trend_plot, distinct_count, profile。\n"
            "aggregate 的 op 只能是 sum/mean/max/min。\n"
            f"列信息: {schema}\n"
            f"问题: {question}\n"
            "输出格式: {\"tool\":\"...\",\"args\":{...}}"
        )
        prompt += self._tool_context_prompt()
        text = self._chat_completion(
            system_prompt="你是数据分析工具参数解析器，只返回 JSON。",
            user_prompt=prompt,
            temperature=0,
            stage="tool_plan",
        )
        if not text:
            return None

        raw = self._extract_json_text(text)
        if not raw:
            return None
        try:
            plan = json.loads(raw)
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(plan, dict):
            return None
        tool = str(plan.get("tool", ""))
        if tool not in {"top_frequency", "group_sum_top", "aggregate", "trend_plot", "distinct_count", "profile"}:
            return None
        return {"tool": tool, "args": plan.get("args", {}) if isinstance(plan.get("args"), dict) else {}}

    @staticmethod
    def _extract_json_text(text: str) -> str:
        if not text:
            return ""
        m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.S | re.I)
        if m:
            return m.group(1).strip()
        m2 = re.search(r"(\{.*\})", text, flags=re.S)
        return m2.group(1).strip() if m2 else ""

    def _generate_llm_code(self, question: str, meta: DatasetMeta, error: str = "") -> Optional[str]:
        if not self.llm_available():
            return None

        schema = ", ".join([f"{c}({meta.dtypes.get(c, 'unknown')})" for c in meta.columns[:60]])
        prompt = (
            "你是 Data Interpreter 代码规划器。只输出可执行 Python 代码，不要解释。\n"
            "约束:\n"
            "1) 只能使用已有变量: df, pd, plt, plots, save_plot。\n"
            "2) 不允许 import / open / eval / exec。\n"
            "3) 必须设置 result_df (DataFrame) 和 answer (str)。\n"
            "4) 如需画图，使用 save_plot('xxx.png') 并 append 到 plots。\n"
            f"列信息: {schema}\n"
            f"用户问题: {question}\n"
        )
        prompt += self._tool_context_prompt()
        if error:
            prompt += f"上次执行错误: {error}\n请修复后重新生成。"

        text = self._chat_completion(
            system_prompt="你是严谨的 pandas 分析代码生成器。",
            user_prompt=prompt,
            temperature=0,
            stage="generate_code",
        )
        if not text:
            return None

        code = self._extract_python_code(text)
        if not code:
            return None
        low = code.lower()
        if "import " in low:
            return None
        if "result_df" not in code or "answer" not in code:
            return None
        return code

    def _chat_completion(self, system_prompt: str, user_prompt: str, temperature: float = 0, stage: str = "chat") -> Optional[str]:
        api_key = str(self._llm_runtime.get("api_key", "")).strip() or os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            return None
        model = str(self._llm_runtime.get("model", "")).strip() or self.llm_model
        base_url = str(self._llm_runtime.get("base_url", "")).strip()
        try:
            import openai  # type: ignore
        except Exception:  # noqa: BLE001
            return None

        # Prefer the new OpenAI SDK client. Fallback to legacy ChatCompletion API for older envs.
        try:
            if hasattr(openai, "OpenAI"):
                kwargs: Dict[str, Any] = {"api_key": api_key}
                if base_url:
                    kwargs["base_url"] = base_url
                client = openai.OpenAI(**kwargs)
                rsp = client.chat.completions.create(
                    model=model,
                    temperature=temperature,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                )
                self._record_usage(stage=stage, model=model, usage=getattr(rsp, "usage", None))
                return str(rsp.choices[0].message.content or "").strip()
        except Exception:  # noqa: BLE001
            pass

        try:
            openai.api_key = api_key
            if base_url:
                openai.api_base = base_url
            rsp = openai.ChatCompletion.create(
                model=model,
                temperature=temperature,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            self._record_usage(stage=stage, model=model, usage=rsp.get("usage") if isinstance(rsp, dict) else None)
            msg = rsp["choices"][0]["message"]["content"]
            return str(msg or "").strip()
        except Exception:  # noqa: BLE001
            return None

    def _record_usage(self, stage: str, model: str, usage: Any) -> None:
        prompt_tokens = 0
        completion_tokens = 0
        total_tokens = 0
        if usage is not None:
            if isinstance(usage, dict):
                prompt_tokens = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
                completion_tokens = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
                total_tokens = int(usage.get("total_tokens") or (prompt_tokens + completion_tokens))
            else:
                prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or getattr(usage, "input_tokens", 0) or 0)
                completion_tokens = int(getattr(usage, "completion_tokens", 0) or getattr(usage, "output_tokens", 0) or 0)
                total_tokens = int(getattr(usage, "total_tokens", 0) or (prompt_tokens + completion_tokens))
        if total_tokens <= 0 and (prompt_tokens > 0 or completion_tokens > 0):
            total_tokens = prompt_tokens + completion_tokens

        in_cost = float(self._llm_runtime.get("input_cost_per_million", 0.0) or 0.0)
        out_cost = float(self._llm_runtime.get("output_cost_per_million", 0.0) or 0.0)
        cost = (prompt_tokens / 1_000_000.0) * in_cost + (completion_tokens / 1_000_000.0) * out_cost
        self._llm_trace.append(
            {
                "time": dt.datetime.utcnow().isoformat(),
                "stage": stage,
                "model": model,
                "prompt_tokens": int(prompt_tokens),
                "completion_tokens": int(completion_tokens),
                "total_tokens": int(total_tokens),
                "cost": float(round(cost, 8)),
            }
        )

    @staticmethod
    def _extract_python_code(text: str) -> str:
        if not text:
            return ""
        m = re.search(r"```(?:python)?\s*(.*?)```", text, flags=re.S | re.I)
        if m:
            return m.group(1).strip()
        return text.strip()

    def failure_solution(self, question: str, meta: DatasetMeta, error: str) -> str:
        cols = ", ".join(meta.columns[:8]) if meta.columns else "无可用字段"
        return (
            "建议检查问题与字段是否对齐，必要时明确列名后重试。"
            f"当前可参考字段: {cols}。最近错误: {error or '未知错误'}"
        )

    def _pick_column(self, cols: List[str], keywords: List[str]) -> Optional[str]:
        low_map = {c.lower(): c for c in cols}
        for k in keywords:
            for lc, orig in low_map.items():
                if k in lc:
                    return orig
        return None

    @staticmethod
    def _query_keywords(question: str) -> List[str]:
        parts = re.split(r"[\s,，。？?！!、]+", question)
        return [p for p in parts if len(p) >= 2]

    def _pick_numeric_like(self, cols: List[str]) -> Optional[str]:
        preferred = ["weight", "amount", "price", "qty", "count", "total", "重量", "金额", "数量"]
        low_map = {c.lower(): c for c in cols}
        for k in preferred:
            for lc, orig in low_map.items():
                if k in lc:
                    return orig
        return cols[0] if cols else None

    def _code_top_frequency(self, col: str) -> str:
        col_lit = repr(col)
        return f"""
result_df = df[{col_lit}].astype(str).value_counts().reset_index()
result_df.columns = [{col_lit}, 'count']
answer = f\"{col} 运单量 Top 10\"
""".strip()

    def _code_sum(self, col: str) -> str:
        col_lit = repr(col)
        return f"""
_tmp = pd.to_numeric(df[{col_lit}], errors='coerce')
result_df = pd.DataFrame([{{'{col}_sum': float(_tmp.sum())}}])
answer = f\"字段 {col} 的合计已计算\"
""".strip()

    def _code_mean(self, col: str) -> str:
        col_lit = repr(col)
        return f"""
_tmp = pd.to_numeric(df[{col_lit}], errors='coerce')
result_df = pd.DataFrame([{{'{col}_mean': float(_tmp.mean())}}])
answer = f\"字段 {col} 的平均值已计算\"
""".strip()

    def _code_max(self, col: str) -> str:
        col_lit = repr(col)
        return f"""
_tmp = pd.to_numeric(df[{col_lit}], errors='coerce')
result_df = pd.DataFrame([{{'{col}_max': float(_tmp.max())}}])
answer = f\"字段 {col} 的最大值已计算\"
""".strip()

    def _code_min(self, col: str) -> str:
        col_lit = repr(col)
        return f"""
_tmp = pd.to_numeric(df[{col_lit}], errors='coerce')
result_df = pd.DataFrame([{{'{col}_min': float(_tmp.min())}}])
answer = f\"字段 {col} 的最小值已计算\"
""".strip()

    def _code_distinct_count(self, col: str) -> str:
        col_lit = repr(col)
        return f"""
_tmp = df[{col_lit}].astype(str)
result_df = pd.DataFrame([{{'{col}_distinct_count': int(_tmp.nunique())}}])
answer = f\"字段 {col} 的去重计数已计算\"
""".strip()

    def _code_trend(self, date_col: str, num_col: str) -> str:
        d_lit = repr(date_col)
        n_lit = repr(num_col)
        return f"""
_tmp = df.copy()
_tmp[{d_lit}] = pd.to_datetime(_tmp[{d_lit}], errors='coerce')
_tmp[{n_lit}] = pd.to_numeric(_tmp[{n_lit}], errors='coerce')
_tmp = _tmp.dropna(subset=[{d_lit}, {n_lit}])
_tmp = _tmp.sort_values({d_lit})
result_df = _tmp[[{d_lit}, {n_lit}]].head(200)

plt.figure(figsize=(10, 4))
plt.plot(_tmp[{d_lit}], _tmp[{n_lit}])
plt.title('{num_col} trend by {date_col}')
plt.tight_layout()
plot_path = save_plot('trend.png')
plots.append(plot_path)
answer = '已生成趋势图'
""".strip()

    def _code_trend_by_index(self, num_col: str) -> str:
        n_lit = repr(num_col)
        return f"""
_tmp = df.copy()
_tmp[{n_lit}] = pd.to_numeric(_tmp[{n_lit}], errors='coerce')
_tmp = _tmp.dropna(subset=[{n_lit}]).reset_index(drop=True)
_tmp['_row_index'] = _tmp.index + 1
result_df = _tmp[['_row_index', {n_lit}]].head(200)

plt.figure(figsize=(10, 4))
plt.plot(_tmp['_row_index'], _tmp[{n_lit}])
plt.title('{num_col} trend by row index')
plt.xlabel('row_index')
plt.tight_layout()
plot_path = save_plot('trend.png')
plots.append(plot_path)
answer = '未识别到时间字段，已按行序号生成趋势图'
""".strip()

    def _code_profile(self) -> str:
        return """
result_df = pd.DataFrame({
    'column': list(df.columns),
    'dtype': [str(t) for t in df.dtypes],
    'non_null_count': [int(df[c].notna().sum()) for c in df.columns],
    'null_count': [int(df[c].isna().sum()) for c in df.columns],
})
answer = '已返回数据集字段概览'
""".strip()

    def _code_group_sum_top(self, group_col: str, amount_col: str, with_plot: bool) -> str:
        g_lit = repr(group_col)
        a_lit = repr(amount_col)
        plot_block = ""
        if with_plot:
            plot_block = """
plt.figure(figsize=(10, 5))
_x = list(range(len(result_df)))
plt.bar(_x, result_df[_amount_col])
plt.xticks(_x, [str(i + 1) for i in _x])
plt.xlabel('Rank')
plt.ylabel(str(_amount_col))
plt.title('Top 10 by amount')
plt.tight_layout()
plot_path = save_plot('counterparty_top10_bar.png')
plots.append(plot_path)
"""

        return f"""
_tmp = df.copy()
_group_col = {g_lit}
_amount_col = {a_lit}
_tmp[{a_lit}] = pd.to_numeric(
    _tmp[{a_lit}].astype(str).str.replace('¥', '', regex=False).str.replace(',', '', regex=False),
    errors='coerce'
)
_tmp = _tmp.dropna(subset=[_group_col, _amount_col])
result_df = _tmp.groupby(_group_col, as_index=False)[_amount_col].sum().sort_values(_amount_col, ascending=False).head(10)
result_df.columns = [_group_col, _amount_col]
{plot_block}
answer = "按 {group_col} 汇总后，金额 Top 10 已计算"
""".strip()
