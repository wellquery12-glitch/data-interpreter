#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8010}"
SKIP_PUBLIC="${SKIP_PUBLIC:-0}"
TMP_CSV="${TMP_CSV:-/tmp/repository_private_demo.csv}"

echo "[INFO] BASE_URL=${BASE_URL}"
echo "[INFO] SKIP_PUBLIC=${SKIP_PUBLIC}"

command -v curl >/dev/null 2>&1 || { echo "[ERROR] curl 未安装"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "[ERROR] python3 未安装"; exit 1; }

json_read() {
  local expr="$1"
  python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(${expr})"
}

require_non_empty() {
  local value="$1"
  local name="$2"
  if [[ -z "${value}" ]]; then
    echo "[ERROR] ${name} 为空"
    exit 1
  fi
}

echo "[STEP] 健康检查"
curl -fsS "${BASE_URL}/health" >/dev/null

echo "[STEP] 构造私人数据集样例文件"
cat >"${TMP_CSV}" <<'CSV'
date,city,amount,orders
2026-01-01,SH,100,2
2026-01-02,BJ,150,3
2026-01-03,SH,120,2
CSV

echo "[STEP] 上传私人数据集"
UPLOAD_JSON="$(curl -fsS -F "file=@${TMP_CSV}" "${BASE_URL}/upload")"
PRIVATE_DATASET_ID="$(printf '%s' "${UPLOAD_JSON}" | json_read "d.get('dataset_id','')")"
require_non_empty "${PRIVATE_DATASET_ID}" "PRIVATE_DATASET_ID"
echo "[INFO] PRIVATE_DATASET_ID=${PRIVATE_DATASET_ID}"

echo "[STEP] 保存私人数据集基础信息（别名/分类/描述/标签）"
curl -fsS -X PUT "${BASE_URL}/datasets/${PRIVATE_DATASET_ID}/metadata" \
  -H "Content-Type: application/json" \
  -d '{"alias":"私有销售样例","category":"sales","biz_description":"区域销售日流水","analysis_notes":"建议做城市对比与时间趋势","tags":["private","sales","daily"]}' \
  >/dev/null

echo "[STEP] 校验私人数据集筛选与元数据"
PRIVATE_LIST_JSON="$(curl -fsS "${BASE_URL}/datasets?module=private&category=sales&keyword=%E5%8C%BA%E5%9F%9F")"
PRIVATE_HIT="$(printf '%s' "${PRIVATE_LIST_JSON}" | json_read "next((x.get('dataset_id','') for x in d.get('data',[]) if x.get('dataset_id')=='${PRIVATE_DATASET_ID}'), '')")"
require_non_empty "${PRIVATE_HIT}" "PRIVATE_HIT"

echo "[STEP] 校验工作台数据集总结接口"
SUMMARY_JSON="$(curl -fsS "${BASE_URL}/datasets/${PRIVATE_DATASET_ID}/summary")"
SUMMARY_CATEGORY="$(printf '%s' "${SUMMARY_JSON}" | json_read "d.get('data',{}).get('category','')")"
if [[ "${SUMMARY_CATEGORY}" != "sales" ]]; then
  echo "[ERROR] summary.category 非预期: ${SUMMARY_CATEGORY}"
  exit 1
fi

echo "[STEP] 校验字段预览接口"
curl -fsS "${BASE_URL}/datasets/${PRIVATE_DATASET_ID}/preview?rows=5" >/dev/null

if [[ "${SKIP_PUBLIC}" != "1" ]]; then
  echo "[STEP] 校验公开数据源列表"
  SOURCES_JSON="$(curl -fsS "${BASE_URL}/datasets/repository/public/sources")"
  SOURCE_ID="$(printf '%s' "${SOURCES_JSON}" | json_read "next((x.get('source_id','') for x in d.get('data',[]) if x.get('enabled')), '')")"
  require_non_empty "${SOURCE_ID}" "SOURCE_ID"

  echo "[STEP] 拉取公开数据集（UCI）"
  UCI_LIST_JSON="$(curl -fsS "${BASE_URL}/datasets/repository/uci?keyword=iris&page=1&page_size=5")"
  UCI_ID="$(printf '%s' "${UCI_LIST_JSON}" | json_read "str((d.get('data') or [{}])[0].get('uci_id',''))")"
  require_non_empty "${UCI_ID}" "UCI_ID"
  echo "[INFO] UCI_ID=${UCI_ID}"

  echo "[STEP] 导入公开数据集到工作台"
  IMPORT_JSON="$(curl -fsS -X POST "${BASE_URL}/datasets/repository/uci/import" -H "Content-Type: application/json" -d "{\"uci_id\": ${UCI_ID}}")"
  PUBLIC_DATASET_ID="$(printf '%s' "${IMPORT_JSON}" | json_read "d.get('dataset_id','')")"
  require_non_empty "${PUBLIC_DATASET_ID}" "PUBLIC_DATASET_ID"
  echo "[INFO] PUBLIC_DATASET_ID=${PUBLIC_DATASET_ID}"

  echo "[STEP] 校验公开数据集列表与预览"
  PUBLIC_LIST_JSON="$(curl -fsS "${BASE_URL}/datasets?module=public&keyword=uci")"
  PUBLIC_HIT="$(printf '%s' "${PUBLIC_LIST_JSON}" | json_read "next((x.get('dataset_id','') for x in d.get('data',[]) if x.get('dataset_id')=='${PUBLIC_DATASET_ID}'), '')")"
  require_non_empty "${PUBLIC_HIT}" "PUBLIC_HIT"
  curl -fsS "${BASE_URL}/datasets/${PUBLIC_DATASET_ID}/preview?rows=5" >/dev/null
fi

echo "[PASS] 数据仓库端到端验收通过"
