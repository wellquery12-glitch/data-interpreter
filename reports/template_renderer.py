from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


class ReportRenderer:
    def render(
        self,
        output_dir: Path,
        dataset_name: str,
        profile: Dict[str, Any],
        plan: List[str],
        facts: List[str],
        inferences: List[str],
        model_conclusions: List[str],
        risks: List[str],
        validation: Dict[str, Any],
        chart_index: List[Dict[str, str]],
        template_names: List[str],
    ) -> Dict[str, str]:
        output_dir.mkdir(parents=True, exist_ok=True)

        executive = self._build_executive_summary(
            dataset_name=dataset_name,
            facts=facts,
            inferences=inferences,
            model_conclusions=model_conclusions,
            risks=risks,
            validation=validation,
        )
        full = self._build_full_report(
            dataset_name=dataset_name,
            profile=profile,
            plan=plan,
            facts=facts,
            inferences=inferences,
            model_conclusions=model_conclusions,
            risks=risks,
            validation=validation,
            chart_index=chart_index,
            template_names=template_names,
        )

        machine = {
            "dataset_name": dataset_name,
            "profile": profile,
            "analysis_plan": plan,
            "facts": facts,
            "statistical_inferences": inferences,
            "model_conclusions": model_conclusions,
            "risks_and_limits": risks,
            "validation": validation,
            "charts": chart_index,
        }

        executive_path = output_dir / "executive_summary.md"
        full_path = output_dir / "full_report.md"
        json_path = output_dir / "machine_readable_summary.json"
        html_path = output_dir / "full_report.html"

        executive_path.write_text(executive, encoding="utf-8")
        full_path.write_text(full, encoding="utf-8")
        json_path.write_text(json.dumps(machine, ensure_ascii=False, indent=2), encoding="utf-8")
        html_path.write_text(self._markdown_to_html(full), encoding="utf-8")

        return {
            "executive_summary": str(executive_path),
            "full_report": str(full_path),
            "machine_readable_summary": str(json_path),
            "full_report_html": str(html_path),
        }

    def _build_executive_summary(
        self,
        dataset_name: str,
        facts: List[str],
        inferences: List[str],
        model_conclusions: List[str],
        risks: List[str],
        validation: Dict[str, Any],
    ) -> str:
        conf = validation.get("confidence", "medium")
        return "\n".join(
            [
                f"# Executive Summary - {dataset_name}",
                "",
                "## 数据事实",
                *[f"- {x}" for x in facts],
                "",
                "## 统计推断",
                *[f"- {x}" for x in inferences],
                "",
                "## 模型结论",
                *[f"- {x}" for x in model_conclusions],
                "",
                "## 风险与限制",
                *[f"- {x}" for x in risks],
                "",
                f"## 结论可信度\n- {conf}",
            ]
        )

    def _build_full_report(
        self,
        dataset_name: str,
        profile: Dict[str, Any],
        plan: List[str],
        facts: List[str],
        inferences: List[str],
        model_conclusions: List[str],
        risks: List[str],
        validation: Dict[str, Any],
        chart_index: List[Dict[str, str]],
        template_names: List[str],
    ) -> str:
        lines = [
            f"# Full Report - {dataset_name}",
            "",
            "## 1. 数据集概览",
            f"- 行数: {profile.get('rows', 0)}",
            f"- 列数: {profile.get('cols', 0)}",
            f"- 数值列: {', '.join(profile.get('numeric_cols', [])) or '-'}",
            f"- 类别列: {', '.join(profile.get('categorical_cols', [])) or '-'}",
            f"- 时间列: {', '.join(profile.get('datetime_cols', [])) or '-'}",
            "",
            "## 2. 分析规划",
            *[f"- {x}" for x in plan],
            "",
            "## 3. 数据事实",
            *[f"- {x}" for x in facts],
            "",
            "## 4. 统计推断",
            *[f"- {x}" for x in inferences],
            "",
            "## 5. 模型结论",
            *[f"- {x}" for x in model_conclusions],
            "",
            "## 6. 风险与限制",
            *[f"- {x}" for x in risks],
            "",
            "## 7. 校验步骤与可信度",
            *[f"- [{c.get('status')}] {c.get('rule')}: {c.get('message')}" for c in validation.get("checks", [])],
            f"- 可信度: {validation.get('confidence', 'medium')}",
            "",
            "## 8. 图表索引",
            *[f"- {c.get('name')}: {c.get('path')}" for c in chart_index],
            "",
            "## 9. 报表模板覆盖",
            *[f"- {name}" for name in template_names],
        ]
        return "\n".join(lines)

    @staticmethod
    def _markdown_to_html(markdown: str) -> str:
        html_lines = ["<html><body>"]
        for line in markdown.splitlines():
            if line.startswith("# "):
                html_lines.append(f"<h1>{line[2:]}</h1>")
            elif line.startswith("## "):
                html_lines.append(f"<h2>{line[3:]}</h2>")
            elif line.startswith("- "):
                html_lines.append(f"<li>{line[2:]}</li>")
            elif line.strip() == "":
                html_lines.append("<br/>")
            else:
                html_lines.append(f"<p>{line}</p>")
        html_lines.append("</body></html>")
        return "\n".join(html_lines)
