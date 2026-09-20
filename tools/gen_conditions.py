# -*- coding: utf-8 -*-
"""从 qdm-metric-cli 的 registry release 生成 H5 用的 conditions.json。

设计原则：**只读别人的东西，一行都不改。**
输入是 ``qdm-metric-cli`` 的 registry release JSON（机器可读的权威元数据），
既不 import 它的 Python，也不调它的 CLI（那会依赖 DataQL 凭据且慢）。

为什么不用 ``harness-data-wikis/scripts/generate-indicators-wikis.py``：
它靠 subprocess 起 426 次 CLI，内部还有 fallback 兜底逻辑会污染语义，输出
是 markdown 还得反解析。registry JSON 一次 ``json.load`` 就够，且它是
``analysis validate`` 真正校验的那份数据（``validator.go:403-419``）——
wikis 的 spec.md 只是它的下游渲染快照，必然滞后。

用法：
    E:/py/python.exe tools/gen_conditions.py [--source <path>] [--out <path>]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent

DEFAULT_SOURCE = (
    Path("E:/harness/harness_all/qdm-metric-cli/internal/registry")
    / "default_registry_release.json"
)
DEFAULT_OUT = REPO / "plugin" / "conditions.json"


def _pick(item: dict[str, Any], *keys: str, default: Any = "") -> Any:
    for k in keys:
        v = item.get(k)
        if v not in (None, "", []):
            return v
    return default


def build(release: dict[str, Any]) -> dict[str, Any]:
    metrics_raw: dict[str, Any] = release.get("metrics") or {}
    dims_raw: dict[str, Any] = release.get("dimensions") or {}
    policies_raw: dict[str, Any] = release.get("statisticPolicies") or {}
    compiled: dict[str, Any] = release.get("compiled") or {}
    runtime: dict[str, Any] = compiled.get("runtime") or {}
    caps_raw: dict[str, Any] = runtime.get("metricCapabilities") or {}
    group_index: dict[str, Any] = compiled.get("dimensionGroupIndex") or {}
    time_profiles: dict[str, Any] = release.get("timeProfiles") or {}

    # ---- 维度 -------------------------------------------------------------
    dimensions: list[dict[str, Any]] = []
    for code, d in sorted(dims_raw.items()):
        dimensions.append(
            {
                "code": code,
                "name": d.get("name") or code,
                "group": d.get("group") or "",
                "valueType": d.get("valueType") or "",
                # 有 valueSource 说明枚举值要实时查 DataQL，H5 得走异步搜索
                "dynamic": bool(d.get("valueSource")),
            }
        )

    groups: dict[str, dict[str, Any]] = {}
    for gname, members in sorted(group_index.items()):
        groups[gname] = {
            "code": gname,
            "dimensions": list(members),
        }

    # ---- 时间粒度（来自 timeProfiles，不硬编码）---------------------------
    grains: list[dict[str, Any]] = []
    time_dims: set[str] = set()
    for _code, prof in time_profiles.items():
        for d in prof.get("dimensions") or []:
            dc = d.get("code")
            if not dc:
                continue
            time_dims.add(dc)
            grains.append(
                {
                    "code": dc,
                    "name": (dims_raw.get(dc) or {}).get("name") or dc,
                }
            )

    # ---- 统计口径 ---------------------------------------------------------
    policies: list[dict[str, Any]] = [
        {"code": code, "name": p.get("name") or code}
        for code, p in sorted(policies_raw.items())
    ]

    # ---- 指标（含按口径裁剪后的可用维度）---------------------------------
    metrics: list[dict[str, Any]] = []
    skipped_internal = 0
    skipped_status = 0
    for code, m in sorted(metrics_raw.items()):
        if m.get("internal"):
            skipped_internal += 1
            continue
        if (m.get("status") or "") != "published":
            skipped_status += 1
            continue

        supported = list(m.get("supportedStatisticPolicies") or [])
        caps_src = caps_raw.get(code) or {}
        caps: dict[str, list[str]] = {}
        for pol in supported:
            entry = caps_src.get(pol) or {}
            # 权威映射：某指标在某口径下真正可用的维度。
            # 缺失时回落到 release true 层的 supportedDimensions / effectiveDimensions。
            dims = entry.get("dimensions")
            if not dims:
                ed = (compiled.get("effectiveDimensions") or {}).get(code)
                dims = ed or list(m.get("supportedDimensions") or [])
            # 时间维度由"时间范围"模块控制，不进维度勾选区
            caps[pol] = [d for d in dims if d not in time_dims]

        metrics.append(
            {
                "code": code,
                "name": m.get("name") or code,
                "unit": m.get("unit") or "",
                "valueType": m.get("valueType") or "",
                "defaultPolicy": m.get("defaultStatisticPolicy") or "",
                "policies": supported,
                "caps": caps,
            }
        )

    return {
        "schemaVersion": 1,
        "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "generator": "qdm-query-card/tools/gen_conditions.py",
        "release": {
            "releaseId": release.get("releaseId") or "",
            "builtAt": release.get("builtAt") or "",
            "contentHash": release.get("contentHash") or "",
            "schemaVersion": release.get("schemaVersion") or "",
        },
        "stats": {
            "metrics": len(metrics),
            "dimensions": len(dimensions),
            "groups": len(groups),
            "policies": len(policies),
            "grains": len(grains),
            "skippedInternal": skipped_internal,
            "skippedUnpublished": skipped_status,
        },
        "policies": policies,
        "grains": grains,
        "groups": groups,
        "dimensions": dimensions,
        "metrics": metrics,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="生成 H5 查询条件元数据")
    ap.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args(argv)

    if not args.source.exists():
        print(f"ERROR: registry not found: {args.source}", file=sys.stderr)
        return 2

    release = json.loads(args.source.read_text(encoding="utf-8"))
    doc = build(release)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(doc, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )

    s = doc["stats"]
    print(f"OK  -> {args.out}")
    print(f"size     : {args.out.stat().st_size / 1024:.0f} KB")
    print(f"release  : {doc['release']['releaseId']} ({doc['release']['builtAt']})")
    print(
        f"metrics  : {s['metrics']}  (drop internal={s['skippedInternal']}, "
        f"unpublished={s['skippedUnpublished']})"
    )
    print(f"dims     : {s['dimensions']} in {s['groups']} groups")
    print(f"policies : {[p['code'] for p in doc['policies']]}")
    print(f"grains   : {[g['code'] for g in doc['grains']]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
