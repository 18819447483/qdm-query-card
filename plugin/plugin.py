# -*- coding: utf-8 -*-
"""企微「查数卡片」插件 —— P0-b 可行性验证版.

本阶段只回答一个问题：**在不改 qwenpaw 宿主代码的前提下，插件能否借本轮
入站 frame 发出一张可跳转的卡片。**

为此做三件事：
  1. PRE_DISPATCH 阶段命中触发词；
  2. 从 ``request.channel_meta["wecom_frame"]`` 取 frame（``base.py:1512``
     由 channel 侧 ``setattr`` 动态附加，AgentRequest 本身无此字段）；
  3. 复用宿主自己的写法 ``channel._client.reply_template_card(frame, card)``
     （``wecom/cards/tool_guard.py:276``）发出 ``text_notice`` 整卡跳转卡片。

设计取舍：
  * **默认不短路**（``delivery.short_circuit=false``）。即便卡片发送失败，
    Agent 仍会正常作答，用户不会感觉"机器人没反应"。验证通过后再打开。
  * **任何异常都不冒泡**。插件永远不能拖垮宿主请求。

硬约束：不修改 qwenpaw 宿主代码。
"""

from __future__ import annotations

import asyncio
import base64
import calendar
import glob
import hashlib
import hmac
import inspect
import json
import logging
import os
import platform
import re
import secrets
import shutil
import subprocess
import sys
import time
import urllib.parse
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from qwenpaw.plugins.api import PluginApi
from qwenpaw.runtime.hooks import HookAction, HookBase, HookResult
from qwenpaw.runtime.phases import Phase

# **坑**：FastAPI 用 ``get_type_hints`` 解析路由函数签名，注解里的名字必须能
# 从**模块 globals** 找到。若在 ``build_router()`` 内部局部 import，注解解析
# 会失败，``request: Request`` 会被当成 query 参数，实际请求直接 422。
try:
    from fastapi import APIRouter, Request  # noqa: F401
    from fastapi.responses import JSONResponse

    _HAS_FASTAPI = True
except Exception:  # noqa: BLE001 - 宿主缺 fastapi 时插件其余能力仍可用
    JSONResponse = None  # type: ignore[assignment,misc]
    _HAS_FASTAPI = False

logger = logging.getLogger("qwenpaw.plugins.qdm_query_card")
LOG_PREFIX = "[qqc]"

PLUGIN_DIR = Path(__file__).resolve().parent
TRIGGER_FILE = PLUGIN_DIR / "trigger.json"

# 一次性 token 的签名密钥。首次运行自动生成，重启后复用（否则旧卡片全部失效）。
SECRET_FILE = PLUGIN_DIR / ".hmac_secret"
TOKEN_TTL_SEC = 15 * 60

# HTTP 路由没有 HookContext，拿不到 channel；只能靠 hook 触发时把实例缓存下来。
_channel_cache: list[Any] = []
# 触发词那条消息的 frame（session_id -> (ts, frame)）。submit 注入查询时透传
# 回 payload meta，让注入轮复用正常提问的流式回复链路（见 inject_into_session）。
# frame 回复只用到 headers.req_id（aibot client.py:125-127），同一 req_id 在
# 同一轮里本就被复用多次（占位/卡片/提示语），所以几分钟内复用是安全的。
_trigger_frames: dict[str, tuple[float, Any]] = {}
_FRAME_TTL_SEC = 30 * 60
# 上次查询结果（session_id -> {body, text, payload, ts, uid}）。
# direct 模式把结果直接推给用户、不进会话上下文，追问时靠这里补回 Agent。
_last_queries: dict[str, dict[str, Any]] = {}
# 已核销的 token nonce（内存态，重启即清空——最坏结果是旧链接可再用一次）
_used_nonces: dict[str, float] = {}
# 占位哨兵任务持有引用，防止被 GC 掉（asyncio 的保留教训）
_watchdogs: set[Any] = set()

# 企微 text_notice 卡片字段长度约束，对齐宿主
# ``wecom/cards/tool_guard.py:172-173`` 的 _truncate(title, 36) / desc 44。
TITLE_MAX = 36
DESC_MAX = 44

DEFAULT_CONFIG: dict[str, Any] = {
    "enabled": True,
    "trigger": {"phrases": ["手动查询数据", "手动取数"], "match": "equals_then_contains"},
    "delivery": {
        "short_circuit": False,
        "fallback_text": "",
        # 卡片发送失败时，改发一条带链接的 markdown（退化路径，默认关）
        "fallback_markdown": False,
        # 占位流（"🤔 Thinking..."）由谁收尾：
        #   host —— 推荐。插件**完全不碰**占位，把 closing 文案放进 SHORT_CIRCUIT
        #           payload，由宿主 ``on_event_message_completed`` →
        #           ``send_content_parts`` 顺手顶掉占位（``channel.py:1359-1402``，
        #           宿主自己平时就是这么干的）。
        #   self —— 我们自己拿 reply_stream(stream_id, finish=True) 定稿。
        #   none —— 谁都不管（对照用，会留下空气泡）。
        "placeholder_mode": "host",
        "absorb_placeholder": False,
        "placeholder_closing_text": "已为你打开查数面板，请点击上方卡片选择条件。",
    },
    "card": {"title": "🔍 手动查数", "desc": "点击卡片，选择查询条件后提交"},
    "publicBaseUrl": "http://127.0.0.1:8088",
    "h5Path": "/api/frontend_plugin/qdm-query-card/files/h5/index.html",
    # auth-center（权限维度只读展示用；查询时的权限求交仍由 CLI/服务端做）
    "authCenter": {
        "enabled": True,
        "baseUrl": "http://127.0.0.1:4008",
        "systemCode": "BI",
        "timeoutSec": 4.0,
        "cacheSec": 300,
    },
    # 查询执行模式：
    #   agent  —— 现有逻辑：注入会话，由 Agent（召回+CLI）查数并叙述；
    #   direct —— 插件拿参数直调 qdm-metric-cli（含数据权限），零 LLM 秒级回。
    #             失败自动回退 agent；结果不进会话上下文。
    "queryMode": "agent",
    # 追问增强 —— 补救 direct 模式「结果不进会话上下文」的短板。
    #   contextInject —— 用户下一句话时，把上次查询的参数 + 结果摘要通过宿主
    #      官方 ``ctx.inject_context``（``runtime/hooks.py:113``）插进本轮
    #      Agent 输入最前面。**不额外发消息、不额外触发 Agent 轮次** —— 那一轮
    #      本来就要跑，只是让 Agent 多看见一段上下文。
    #   autoTimeShift —— 识别「上月/上周/昨天」这类时间追问，插件直接改时间重查
    #      并推送，Agent 完全不跑（秒回）。有误判风险（陈述句也会命中），默认关。
    # 群聊面板防护。
    #
    # 背景：卡片是「整卡跳转 URL」，企微智能机器人**不会在跳转时带上点击者
    # 身份**（``card_action.type=1`` 只是 url，无回调、无 userid；可用的网页
    # 授权需要 corpid + agent secret，本形态没有）。所以"判断点击者是不是 A"
    # 在平台层做不到，只能在应用层做弱约束：
    #   none   —— 谁都能打开并提交（群里：B 会用 A 的数据权限查数）
    #   claim  —— 首开认领：首个打开面板的浏览器（opener 存在 localStorage）
    #             锁定该链接；换人/换设备再打开 → 友好提示，引导自行触发
    #   strict —— 身份确认：打开者必须输入自己的企微账号(loginid)，与签发者
    #             一致才放行（弱校验，能挡误用，挡不住蓄意冒充）
    "groupGuard": {
        "mode": "claim",
        # 群聊「按钮交付」：群里只发带按钮的卡，链接只给点按钮的本人。
        # 真机实测有效（2026-09-18）；关掉则退回原来的整卡跳转卡。
        "buttonDelivery": True,
        # 「一步跳转」：给按钮补 type=1 + url，点一下直接打开页面，不用等换卡
        # 再点第二次。企微未文档化能力（官方 Button 结构体只有 text/style/key）。
        # ⚠️ 真机实测结论（2026-09-18 18:31）：带 type=1+url 的**跳转型按钮不推
        # template_card_event 回调**，服务端拿不到点击者身份 → 页面只能一直
        # pending「无法确认身份」。也就是「一步打开」与「身份隔离」不可兼得。
        # 因此默认关闭；保持 False 时走两步（点按钮 → 卡片只给本人换成带链接卡）。
        "buttonJump": False,
        # ⚠️ 实验项「一步交付」：卡发出去后立刻只给发起人换一张带链接卡，
        # 让他点 1 次就进 H5（否则要「点按钮 → 再点卡片」两步）。
        # 身份不用再靠按钮回调取 —— 群里 @机器人那帧本身就带 from.userid。
        # ⚠️ **已证伪（2026-09-21 真机）**，保留只为留档，别打开：
        # 企微 errcode=846606 "request already responded, cannot respond again"
        # —— 同一个 req_id 只能响应一次，发卡已经用掉了，没有第二次机会。
        "instantDelivery": False,
        "instantClosingText": "查数面板已就绪，请点击上方卡片打开（链接只有你能看到）。",
        # 「定向可见」：回复消息时带 visible_to_user=[发起人]，只有他看得到这条。
        # 企微**应用消息** API 的字段；智能机器人 WS 协议未承诺支持（aibot SDK
        # 里零处引用）。若成立，群聊就能一步打开，不必再「点按钮 → 再点卡片」。
        #   ❌ **2026-09-21 真机：无效。** 带字段的 reply 被服务端正常接受
        #   （errcode=0，没走回退），但群里其他人依然看得到、也点得到那张卡
        #   —— 字段被**静默忽略**，不是报错。所以这条路也关闭，保持 False。
        #   保留代码只为留档；真要再用，必须先确认机器人协议开始支持它。
        "visibleToUser": False,
        # 「一步打开」：直接发带链接的整卡跳转卡，点 1 次就进 H5。
        # 强制依赖 visibleToUser（否则链接全员可见）—— 既然上面那个字段无效，
        # 这个也就永远发不出去，保持 False。安全底线仍由 pick_group_card 守住。
        "oneStep": False,
        # 同一张面板最多换几次 token（换设备/刷新用）
        "jumpRedeemMax": 6,
        "buttonText": "打开查数面板",
        "buttonTtlSec": 3600,
        # 点按钮后只给本人换上的那张卡（text_notice 整卡跳转，点卡片任意处即开）
        "linkCardTitle": "✅ 面板已就绪",
        "linkCardDesc": "点击本卡片打开查数面板（链接只有你能看到）",
        "buttonClosingText": "查数面板已生成，请点击上方卡片打开（链接只有你能看到）。",
        "buttonFallbackText": "查数面板发送失败，请重新发送触发词。",
        "claimTtlSec": 3600,
        "applyToPrivate": False,
        "triggerHint": "手动查询数据",
    },
    "followUp": {
        "enabled": True,
        "ttlSec": 1800,
        "maxRows": 20,
        "contextInject": True,
        "autoTimeShift": False,
    },
    # 方案 B 探针：验证「模板卡片按钮回调能否取到点击者 userid」以及
    # 「update_template_card(userids=[...]) 能否只对点击者替换卡片」。
    # 默认关闭，打开后需重启宿主（plugin.py 无热加载）。
    "probe": {
        "enabled": False,
        # 探针触发词（剥掉群聊 @前缀后完全匹配才触发）
        "trigger": "卡片探针",
        # 命中正常触发词时，是否顺手也发一张探针卡（省得记新触发词）
        "alsoOnTrigger": False,
        # 是否给 wecom client 挂 template_card_event 监听（仅打日志用）
        "attachListener": True,
    },
    # direct 模式参数
    "direct": {
        # auth-center 的 runtime MCP 端点（与 Agent 用的是同一个，非新权限面）
        "runtimeMcpUrl": "http://127.0.0.1:8765/mcp",
        # runtime token 文件（Bearer 鉴权）。留空则取环境变量
        # QDM_AUTH_RUNTIME_TOKEN_FILE；两者都取不到时直连查询会明确报错。
        # 这里不写死平台路径 —— 配置里写死的路径在别的平台上多半不存在，
        # 会自动回退到环境变量（见 ``_auth_blob``）。
        "runtimeTokenFile": "",
        # 带鉴权查询必须用 instance 托管的 CLI（E:\harness 快照传 blob 会 rc=77）。
        # 留空则自动探测 instance runtimes 目录里 mtime 最新的一个
        "cliPath": "",
        "timeoutSec": 90.0,
        "blobTimeoutSec": 8.0,
        "maxRows": 20,
    },
    # qdm-metric-cli（dim values 实时搜索用）。path 为空则自动探测
    "cli": {"path": "", "timeoutSec": 20.0},
    # 限流：DataQL 的 QDM_METRIC_DATAQL_MAX_CONCURRENCY=4 是**进程内**信号量，
    # N 个提交 = N×4，必须在插件层做全局闸门
    "limits": {
        "dimValuesConcurrency": 4,
        "dimValuesLimitMax": 200,
        "dimValuesCacheSec": 60,
        # 提交冷却拆成两段，别再让用户干等固定 180s：
        #   submitInFlightSec —— 占坑时的兜底上限（只防异常泄漏，正常路径用不到）
        #   submitCooldownSec —— 任务**结束**后真正保留的冷却（防手抖连点）
        # 任务一结束就把截止时间收缩到 now+submitCooldownSec，失败则整个释放，
        # 于是用户感知到的等待 = 查询本身耗时 + 15s。
        "submitInFlightSec": 60,
        "submitCooldownSec": 15,
        "maxPendingSubmissions": 4,
    },
}

_PUNCT_RE = re.compile(
    r"[\s\u3000、，,。.；;：:！!？?“”\"'`~@#$%^&*()\[\]{}<>/\\|+=\-_]+"
)

# ---------------------------------------------------------------------------
# 配置：热加载（改 trigger.json 不用重启宿主）
# ---------------------------------------------------------------------------

_config_cache: dict[str, Any] = {}
_config_mtime: float = -1.0


def _deep_merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def load_config() -> dict[str, Any]:
    """读取 trigger.json，按 mtime 缓存以实现热加载。"""
    global _config_cache, _config_mtime
    try:
        mtime = TRIGGER_FILE.stat().st_mtime
    except OSError:
        return _config_cache or DEFAULT_CONFIG
    if mtime == _config_mtime and _config_cache:
        return _config_cache
    try:
        raw = json.loads(TRIGGER_FILE.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - 配置坏了也不能拖垮宿主
        logger.warning("%s config load failed: %s; using defaults", LOG_PREFIX, exc)
        return DEFAULT_CONFIG
    merged = _deep_merge(DEFAULT_CONFIG, raw)
    _config_cache, _config_mtime = merged, mtime
    logger.info(
        "%s config loaded phrases=%s short_circuit=%s",
        LOG_PREFIX,
        merged.get("trigger", {}).get("phrases"),
        merged.get("delivery", {}).get("short_circuit"),
    )
    return merged


# ---------------------------------------------------------------------------
# 触发词匹配
# ---------------------------------------------------------------------------


def normalize(text: str) -> str:
    """归一化：去首尾空白、去标点/空白、转小写。"""
    value = (text or "").strip().lower()
    return _PUNCT_RE.sub("", value)


def match_trigger(text: str, cfg: dict[str, Any]) -> tuple[bool, str]:
    """先判「归一化后完全相等」（零误触），再判「包含」（容忍口语表述）。"""
    trig = cfg.get("trigger") or {}
    phrases = [p for p in (trig.get("phrases") or []) if str(p).strip()]
    target = normalize(text)
    if not target or not phrases:
        return False, ""
    for phrase in phrases:
        norm = normalize(str(phrase))
        if norm and target == norm:
            return True, f"equals:{phrase}"
    if trig.get("match") == "equals_then_contains":
        for phrase in phrases:
            norm = normalize(str(phrase))
            if norm and norm in target:
                return True, f"contains:{phrase}"
    return False, ""


# ---------------------------------------------------------------------------
# 上下文探测
# ---------------------------------------------------------------------------


def input_text(ctx: Any) -> str:
    """取本轮入站文本。

    HookContext 构造时即已填充 input_msgs（``runtime.py:488``），
    因此 PRE_DISPATCH 阶段可用。
    """
    messages = getattr(ctx, "input_msgs", None)
    if not isinstance(messages, list) or not messages:
        return ""
    getter = getattr(messages[-1], "get_text_content", None)
    value = getter() if callable(getter) else ""
    return value.strip() if isinstance(value, str) else ""


def probe_meta(ctx: Any) -> dict[str, Any]:
    """尽可能多地取证：现阶段不知道 meta 里到底有什么，先全记下来。"""
    request = getattr(ctx, "request", None)
    meta = getattr(request, "channel_meta", None) or {}
    info: dict[str, Any] = {
        "has_request": request is not None,
        "meta_keys": sorted(meta.keys()) if isinstance(meta, dict) else [],
        "has_frame": False,
        "frame_keys": [],
        "chat_type": meta.get("wecom_chat_type"),
        "is_group": meta.get("is_group"),
        "has_sender_id": bool(meta.get("wecom_sender_id")),
        "has_chatid": bool(meta.get("wecom_chatid")),
        "session_id": getattr(ctx, "session_id", None),
        "agent_id": getattr(ctx, "agent_id", None),
    }
    frame = meta.get("wecom_frame") if isinstance(meta, dict) else None
    if frame:
        info["has_frame"] = True
        info["frame_keys"] = (
            sorted(frame.keys()) if isinstance(frame, dict) else [type(frame).__name__]
        )
    return info


async def resolve_channel(ctx: Any) -> Any:
    """取 wecom channel 实例（``app/workspace/workspace.py:114``）。

    **坑 1**：``ChannelManager.get_channel`` 是 async（``channels/manager.py:551``），
    漏掉 await 会拿到 coroutine 而不是 channel —— P0-b 首次实测就栽在这里，
    症状极具迷惑性：看起来像"channel 被禁用"，实际是类型不对。
    这里用 ``isawaitable`` 兼容同步/异步两种实现，避免再踩。
    """
    try:
        workspace = getattr(ctx, "workspace", None)
        manager = getattr(workspace, "channel_manager", None)
        if manager is None:
            return None
        result = manager.get_channel("wecom")
        if inspect.isawaitable(result):
            result = await result
        return result
    except Exception:  # noqa: BLE001
        logger.exception("%s resolve_channel failed", LOG_PREFIX)
        return None


def candidate_channels(ctx: Any) -> list[Any]:
    """枚举 manager 里所有 wecom channel 实例。

    **坑 2**：宿主可能同时挂着多个 wecom bot（本机实测 3 个：其中一个
    ``bot_id=aib2t7B9G785`` 认证失败）。``get_channel`` 只返回**第一个**
    匹配，恰好可能就是那个坏掉的。所以不能只取一个，要全量枚举后逐个试。
    """
    try:
        workspace = getattr(ctx, "workspace", None)
        manager = getattr(workspace, "channel_manager", None)
        if manager is None:
            return []
        channels = list(getattr(manager, "channels", None) or [])
    except Exception:  # noqa: BLE001
        logger.exception("%s candidate_channels failed", LOG_PREFIX)
        return []
    hits = [ch for ch in channels if getattr(ch, "channel", None) == "wecom"]
    # 启用的排前面；认证失败的实例即便 enabled=True 也会在发送时抛错，靠 try 兜住
    hits.sort(key=lambda ch: not getattr(ch, "enabled", False))
    return hits


def remember_channels(channels: list[Any]) -> None:
    """缓存 wecom 实例，供 HTTP 提交接口回推结果时使用。

    HTTP 请求不在 Agent 上下文里，拿不到 HookContext，也没有
    ``app.state`` 上的 channel_manager（``AppServiceManager`` 不暴露它），
    所以只能在 hook 命中时把引用留下来。
    """
    global _channel_cache
    usable = [ch for ch in channels if ch is not None]
    if usable:
        _channel_cache = usable


def cached_channels() -> list[Any]:
    return [ch for ch in _channel_cache if getattr(ch, "enabled", False)]


def _remember_frame(session_id: str, frame: Any) -> None:
    """缓存触发词消息的 frame，供 submit 注入时透传（超 TTL 清理）。"""
    if not session_id or frame is None:
        return
    now = time.time()
    # 顺手清理过期项，防止长期运行内存膨胀
    stale = [k for k, (ts, _) in _trigger_frames.items() if now - ts > _FRAME_TTL_SEC]
    for k in stale:
        _trigger_frames.pop(k, None)
    _trigger_frames[session_id] = (now, frame)


def _lookup_frame(session_id: str) -> Any:
    """取回该会话最近一次触发的 frame（过期返回 None）。"""
    entry = _trigger_frames.get(session_id)
    if not entry:
        return None
    ts, frame = entry
    if time.time() - ts > _FRAME_TTL_SEC:
        _trigger_frames.pop(session_id, None)
        return None
    return frame


# ---------------------------------------------------------------------------
# 一次性 token：让 H5 只能被「发起提问的那个人」使用
# ---------------------------------------------------------------------------
#
# 原生 metric-cli UI 是「谁拿到 URL 谁就能用」，且身份是进程级的 —— 多人同时
# 打开会互相污染。这里的做法是：卡片里嵌一个 HMAC 签名的短时 token，H5 的每
# 次请求都要带上；服务端验签 + 查过期 + 核销 nonce。前端不依赖任何服务端
# session，条件全在浏览器内存里、提交时整体带走，因此天然多人隔离。


def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64d(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


def _secret() -> bytes:
    """读取或生成签名密钥。

    放在插件目录内（``.hmac_secret``），已在 .gitignore 中排除。
    密钥轮换 = 删掉这个文件重启，代价是已发出的旧卡片链接全部失效。
    """
    if SECRET_FILE.exists():
        data = SECRET_FILE.read_bytes().strip()
        if data:
            return data
    key = secrets.token_urlsafe(32).encode("ascii")
    try:
        SECRET_FILE.write_bytes(key)
    except Exception:  # noqa: BLE001 - 只读目录时退回进程内随机密钥
        logger.warning("%s cannot persist hmac secret; tokens die on restart", LOG_PREFIX)
    return key


def issue_token(
    *,
    sender_id: str,
    chatid: str,
    chat_type: str,
    session_id: str,
    ttl: int = TOKEN_TTL_SEC,
    guard_free: bool = False,
) -> str:
    """签发 token：``<payload_b64>.<sig_b64>``。

    ``guard_free=True`` 会在 payload 里打上 ``gf=1``，表示**这条链接的归属
    已经在签发前校验过了**，H5 打开时不再走 claim/strict 的"谁先打开谁认领"。

    这是跨设备问题的根因修复：认领机制用的是浏览器 localStorage 句柄，
    同一用户的手机和电脑句柄必然不同，第二台设备会被自己挡住；而按钮交付
    模式下链接本来就只递给点按钮的本人（差异化卡片已保证），再叠一层认领
    纯属自伤。
    """
    payload = {
        "u": sender_id or "",
        "c": chatid or "",
        "g": 1 if chat_type == "group" else 0,
        "s": session_id or "",
        "e": int(time.time()) + int(ttl),
        "n": secrets.token_urlsafe(8),
    }
    if guard_free:
        payload["gf"] = 1
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    body = _b64e(raw)
    sig = _b64e(hmac.new(_secret(), body.encode("ascii"), hashlib.sha256).digest()[:16])
    return f"{body}.{sig}"


def verify_token(token: str, *, consume: bool = True) -> tuple[bool, dict[str, Any], str]:
    """校验 token。返回 (是否通过, payload, 原因)。

    ``consume=True`` 时核销 nonce —— 提交接口应当核销，bootstrap 不应核销
    （页面刷新会重新拉一次）。
    """
    if not token or "." not in token:
        return False, {}, "empty or malformed token"
    body, _, sig = token.rpartition(".")
    try:
        expect = _b64e(
            hmac.new(_secret(), body.encode("ascii"), hashlib.sha256).digest()[:16]
        )
    except Exception:  # noqa: BLE001
        return False, {}, "sign error"
    if not hmac.compare_digest(expect, sig):
        return False, {}, "bad signature"
    try:
        payload = json.loads(_b64d(body).decode("utf-8"))
    except Exception:  # noqa: BLE001
        return False, {}, "bad payload"
    if not isinstance(payload, dict):
        return False, {}, "bad payload type"
    if int(payload.get("e") or 0) < int(time.time()):
        return False, {}, "expired"
    nonce = str(payload.get("n") or "")
    if consume:
        if nonce in _used_nonces:
            return False, {}, "already used"
        _used_nonces[nonce] = time.time()
        # 顺手清理过期记录，避免无界增长
        if len(_used_nonces) > 500:
            cutoff = time.time() - TOKEN_TTL_SEC * 2
            for k, v in list(_used_nonces.items()):
                if v < cutoff:
                    _used_nonces.pop(k, None)
    return True, payload, "ok"


def nonce_used(payload: dict[str, Any]) -> bool:
    """只看不核销：这个 token 是否已经提交过。"""
    return str(payload.get("n") or "") in _used_nonces


def rollback_nonce(payload: dict[str, Any]) -> None:
    """把刚核销的 nonce 退回去（投递失败时用，让用户能原 token 重试）。"""
    _used_nonces.pop(str(payload.get("n") or ""), None)


def consume_nonce(payload: dict[str, Any]) -> bool:
    """核销 token 的 nonce（配合 ``verify_token(consume=False)`` 使用）。

    分两步的原因：提交接口要先过限流闸门再核销 —— 如果先核销，
    一个被 429 拦下来的用户连 token 都没了，只能重新发触发词。
    返回 False 表示 nonce 已被并发请求抢先核销。
    """
    nonce = str(payload.get("n") or "")
    if not nonce or nonce in _used_nonces:
        return False
    _used_nonces[nonce] = time.time()
    if len(_used_nonces) > 500:
        cutoff = time.time() - TOKEN_TTL_SEC * 2
        for k, v in list(_used_nonces.items()):
            if v < cutoff:
                _used_nonces.pop(k, None)
    return True


# ---------------------------------------------------------------------------
# 群聊面板防护（groupGuard）
# ---------------------------------------------------------------------------

# nonce -> (opener, expire_ts)；opener 是 H5 存在 localStorage 里的浏览器句柄
_panel_owners: dict[str, tuple[str, float]] = {}


def _guard_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    return cfg.get("groupGuard") or {}


def guard_applies(payload: dict[str, Any], cfg: dict[str, Any]) -> bool:
    """这条链接要不要做「打开者约束」。"""
    # gf=1：归属在签发前已校验（按钮交付 / redeem），不再叠认领
    if int(payload.get("gf") or 0):
        return False
    guard = _guard_cfg(cfg)
    mode = str(guard.get("mode") or "claim").lower()
    if mode in ("none", "off", "false"):
        return False
    is_group = bool(int(payload.get("g") or 0))
    if not is_group and not bool(guard.get("applyToPrivate", False)):
        return False
    return True


def guard_mode(cfg: dict[str, Any]) -> str:
    return str(_guard_cfg(cfg).get("mode") or "claim").lower()


def _clean_panel_owners(ttl: float) -> None:
    now = time.time()
    for nonce in [k for k, v in _panel_owners.items() if v[1] <= now]:
        _panel_owners.pop(nonce, None)


def release_panel_claim(payload: dict[str, Any]) -> None:
    """链接作废时顺手释放认领（提交完成/失败回滚都调一次）。"""
    _panel_owners.pop(str(payload.get("n") or ""), None)


def check_panel_guard(
    payload: dict[str, Any],
    cfg: dict[str, Any],
    opener: str = "",
    who: str = "",
    owner_name: str = "",
) -> tuple[bool, str, str]:
    """面板准入判定。返回 ``(allowed, code, message)``。

    code 取值：
      ok / who_required（strict 且还没自报身份）/ no_opener（拿不到浏览器句柄）
      / claimed（已被别人抢先打开）/ not_owner（自报身份与签发者不符）

    message 是**直接给用户看**的中文文案。
    """
    if not guard_applies(payload, cfg):
        return True, "ok", ""
    mode = guard_mode(cfg)
    hint = str(_guard_cfg(cfg).get("triggerHint") or "手动查询数据")
    owner = owner_name or str(payload.get("u") or "")
    tip_self = (
        f"如需查询，请在群里自己发送「{hint}」，机器人会给你只属于你的面板。"
    )

    if mode == "strict":
        # 自报家门：比对企微账号（loginid）或中文姓名
        claimed = str(who or "").strip()
        if not claimed:
            return False, "who_required", (
                f"这份查数面板由 {owner} 发起。"
                if owner else "这份查数面板有归属人。"
            )
        uid = str(payload.get("u") or "").strip().lower()
        given = claimed.lower()
        if given not in (uid, str(owner_name or "").strip().lower()):
            return False, "not_owner", (
                f"这份查数面板由 {owner} 发起，只有本人可以使用。{tip_self}"
            )
        return True, "ok", ""

    # mode == claim：首开认领 + 本人自证接管
    opener = str(opener or "").strip()
    ttl = float(_guard_cfg(cfg).get("claimTtlSec", 3600) or 0)
    now = time.time()
    _clean_panel_owners(ttl)
    nonce = str(payload.get("n") or "")
    cur = _panel_owners.get(nonce)

    # 自证是本人才允许接管（换手机/换浏览器时用得到）
    given = str(who or "").strip().lower()
    uid = str(payload.get("u") or "").strip().lower()
    self_proof = bool(given) and bool(uid) and (
        given == uid or (owner_name and given == str(owner_name).strip().lower())
    )

    if cur is None or (cur[0] != opener and self_proof):
        if not opener:
            return False, "no_opener", (
                "当前浏览器无法识别打开者（不支持本地存储），为避免被他人使用，"
                "已阻止打开。请换成普通浏览器/企微内置浏览器打开。" + tip_self
            )
        _panel_owners[nonce] = (opener, now + (ttl or 3600))
        if cur is not None:
            logger.info(
                "%s panel ownership transferred login=%s", LOG_PREFIX, payload.get("u") or ""
            )
        return True, "ok", ""
    if cur[0] != opener:
        return False, "claimed", (
            f"这份查数面板已由 {owner} 打开并使用，不能多人共用。{tip_self}"
            "如果你是本人换设备打开，请输入你的企微账号即可解锁。"
        )
    return True, "ok", ""


# ---------------------------------------------------------------------------
# 卡片
# ---------------------------------------------------------------------------


def _truncate(text: str, limit: int) -> str:
    value = str(text or "")
    return value if len(value) <= limit else value[: limit - 1] + "…"


def h5_base_url(cfg: dict[str, Any]) -> str:
    """H5 页面地址（不带任何参数）。"""
    base = str(cfg.get("publicBaseUrl") or "").rstrip("/")
    path = str(cfg.get("h5Path") or "")
    return f"{base}{path}" if base and path else base


def build_card(cfg: dict[str, Any], token: str = "") -> tuple[dict[str, Any], str]:
    """构造 text_notice 整卡跳转卡片。

    结构照抄宿主 ``wecom/cards/tool_guard.py:168-176``，注意两条约束：
      * ``card_type`` 为 text_notice；
      * ``card_action.type`` 必须是 1 或 2（**0 会被服务端拒绝**）。
    """
    url = h5_base_url(cfg)
    if token:
        url = f"{url}?t={token}"
    card_cfg = cfg.get("card") or {}
    card = {
        "card_type": "text_notice",
        "main_title": {
            "title": _truncate(card_cfg.get("title") or "手动查数", TITLE_MAX),
            "desc": _truncate(card_cfg.get("desc") or "", DESC_MAX),
        },
        "card_action": {"type": 1, "url": url},
    }
    return card, url


# ---------------------------------------------------------------------------
# 方案 B 探针：模板卡片按钮回调（event.template_card_event）
#
# 可行性依据（读源码确认，非猜测）：
#   * 事件名 ``event.template_card_event`` 由 ``aibot/message_handler.py:85``
#     的 ``emitter.emit(f"event.{event_type}", frame)`` 发出，event_type 取自
#     ``body.event.eventtype``；
#   * ``WSClient`` 继承 ``pyee.AsyncIOEventEmitter``（``client.py:22``），
#     ``on()`` 支持多监听器 —— 宿主已在 ``channel.py:1598`` 挂了一个，
#     我们再挂一个不会互相覆盖；
#   * 宿主 dispatcher 按 ``task_id`` 前缀路由，不认识的前缀在
#     ``dispatcher.py:162-165`` 直接 return（静默忽略），所以用 ``qqc_probe_``
#     前缀不会惊动宿主；
#   * 点击者身份在 ``body.from.userid``（宿主 ``tool_guard.py:205-218``
#     就是这么取的）；
#   * ``update_template_card(frame, card, userids)`` 支持只替换指定人的卡片
#     （``client.py:260-284``）—— 宿主只用过不带 userids 的全员替换形式
#     （``tool_guard.py:362``），**带 userids 的差异化形式由本次探针验证**。
# ---------------------------------------------------------------------------

_PROBE_TASK_PREFIX = "qqc_probe_"
_probe_nonces: dict[str, dict[str, Any]] = {}
_probe_loops: list[Any] = []


def _probe_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    return cfg.get("probe") or {}


_AT_MENTION = re.compile(r"^\s*(@[^\s@]+\s*)+")


def strip_mention(text: str) -> str:
    """剥掉群聊消息开头的 @机器人 前缀。

    群里发给机器人的消息实际是 ``@小爱同学 手动查询数据``（宿主
    ``match_trigger`` 走 contains 分支就是因为这个前缀），所以任何需要
    「整句相等」的判断都必须先剥掉它，否则在群里永远匹配不上。
    """
    return _AT_MENTION.sub("", text or "").strip()


def _frame_brief(res: Any) -> str:
    """回执帧只关心 errcode/errmsg —— WS 单向发送，有回执才有意义。"""
    if not isinstance(res, dict):
        return repr(res)[:120]
    return f"errcode={res.get('errcode')!r} errmsg={res.get('errmsg')!r}"


def _attach_card_listener(channels: list[Any]) -> None:
    """给 wecom client 挂 ``template_card_event`` 监听（幂等）。"""
    for ch in channels or []:
        client = getattr(ch, "_client", None)
        if client is None or getattr(client, "_qqc_probe_attached", False):
            continue
        on = getattr(client, "on", None)
        if not callable(on):
            continue
        try:
            on("event.template_card_event", _on_card_event_sync)
            setattr(client, "_qqc_probe_attached", True)
            loop = getattr(ch, "_loop", None)
            if loop is not None and loop not in _probe_loops:
                _probe_loops.append(loop)
            logger.info("%s card listener attached on %s", LOG_PREFIX, _label(ch))
        except Exception:  # noqa: BLE001
            logger.exception(
                "%s attach card listener failed on %s", LOG_PREFIX, _label(ch)
            )


def _on_card_event_sync(frame: Any) -> None:
    """WS 线程同步回调：只做日志，处理抛给主循环。

    这里**不能** await —— pyee 对非协程 listener 是同步调用，当前线程是
    SDK 的 WS 接收线程。
    """
    try:
        body = frame.get("body") or {} if isinstance(frame, dict) else {}
        event = body.get("event") or {}
        tce = event.get("template_card_event") or event
        task_id = str(tce.get("task_id") or "")
        from_info = body.get("from") or {}
        # 整帧结构打全：这是「回调到底带不带 userid」的关键证据
        logger.warning(
            "%s CARD EVENT task_id=%s event_key=%s tce_keys=%s "
            "from_keys=%s userid=%s chatid=%s chattype=%s",
            LOG_PREFIX,
            task_id,
            str(tce.get("event_key") or "")[:160],
            sorted(tce.keys()) if isinstance(tce, dict) else tce,
            sorted(from_info.keys()) if isinstance(from_info, dict) else from_info,
            from_info.get("userid") if isinstance(from_info, dict) else None,
            from_info.get("chatid") if isinstance(from_info, dict) else None,
            from_info.get("chattype") if isinstance(from_info, dict) else None,
        )
        if task_id.startswith(_PANEL_TASK_PREFIX):
            handler = _handle_panel_button      # 群聊按钮交付（正式链路）
        elif task_id.startswith(_PROBE_TASK_PREFIX):
            handler = _handle_probe_event       # 探针
        else:
            return                              # 宿主自己的卡片，交给它
        loop = next((lp for lp in _probe_loops if lp.is_running()), None)
        if loop is None:
            logger.warning("%s card event: no running loop, drop", LOG_PREFIX)
            return
        asyncio.run_coroutine_threadsafe(handler(frame, task_id), loop)
    except Exception:  # noqa: BLE001 - WS 线程里绝不能抛
        logger.exception("%s card event sync handler failed", LOG_PREFIX)


async def _handle_probe_event(frame: Any, task_id: str) -> None:
    """处理探针卡片点击：按点击者身份差异化替换卡片（5s 窗口内）。"""
    body = frame.get("body") or {} if isinstance(frame, dict) else {}
    from_info = body.get("from") or {}
    userid = str(from_info.get("userid") or "")
    item = _probe_nonces.get(task_id) or {}
    owner = str(item.get("owner") or "")
    is_owner = bool(userid) and userid == owner

    title = "✅ 你就是这个面板的归属人" if is_owner else "🔒 这不是你的面板"
    desc = f"点击者={userid or '(空)'} 归属={owner or '(空)'}"
    card = {
        "card_type": "text_notice",
        "task_id": task_id,
        "main_title": {
            "title": _truncate(title, TITLE_MAX),
            "desc": _truncate(desc, DESC_MAX),
        },
        # text_notice 必须有 card_action，且 type 只能是 1/2（0 会被拒）
        "card_action": {"type": 1, "url": "https://qwenpaw.agentscope.io"},
    }

    for ch in cached_channels():
        client = getattr(ch, "_client", None)
        fn = getattr(client, "update_template_card", None)
        if not callable(fn):
            continue
        try:
            res = await fn(frame, card, [userid])
            logger.warning(
                "%s PROBE update ok task=%s user=%s owner=%s res=%s",
                LOG_PREFIX, task_id, userid, owner, _frame_brief(res),
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "%s PROBE update failed task=%s user=%s", LOG_PREFIX, task_id, userid
            )
        return
    logger.warning("%s PROBE no usable channel for update", LOG_PREFIX)


def build_probe_card(nonce: str) -> dict[str, Any]:
    """button_interaction 探针卡（结构照抄宿主 ``tool_guard.py:107-141``）。

    照抄时才能避开两个坑：``button_list`` 必须在 root（不能包进
    ``card_action``）；按钮的 ``key`` 会被服务端原样塞回 ``event_key``。
    """
    return {
        "card_type": "button_interaction",
        "task_id": f"{_PROBE_TASK_PREFIX}{nonce}",
        "main_title": {
            "title": _truncate("查数面板探针", TITLE_MAX),
            "desc": _truncate("点一下按钮，验证回调身份", DESC_MAX),
        },
        "button_list": [
            {
                "text": "打开查数面板",
                "style": 1,
                "key": json.dumps(
                    {"a": "open", "n": nonce},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            }
        ],
    }


# ---------------------------------------------------------------------------
# 群聊按钮交付（方案 B 正式实现）
#
# 真机实测（2026-09-18 群聊）：按钮回调稳定、能区分点击者；``update_template_card``
# 带 ``userids`` 只改点击者那一端。所以群聊改成——**群里只发一张带按钮的卡，
# 链接只给点击按钮的本人**，其他人点了只看到一句提示。
#
# ⚠️ 实测帧结构：``body.from`` **只有 userid**，没有 chatid/chattype。所以
# 发卡时必须按 task_id 把 owner/chatid/session/token 存进本地表，回调时反查。
# ---------------------------------------------------------------------------

_PANEL_TASK_PREFIX = "qqc_panel_"
_panel_buttons: dict[str, dict[str, Any]] = {}


def _clean_panel_buttons(ttl: float) -> None:
    now = time.time()
    for k in [
        k for k, v in _panel_buttons.items()
        if now - float(v.get("ts") or 0) > ttl
    ]:
        _panel_buttons.pop(k, None)


def build_panel_button_card(
    task_id: str,
    cfg: dict[str, Any],
    button_text: str,
    jump_url: str = "",
) -> dict[str, Any]:
    """群聊用的 button_interaction 卡：只有按钮，**不含任何 token**。

    ``jump_url`` 非空时给按钮补 ``type=1`` + ``url`` —— 这是企微的未文档化
    能力（官方 Button 结构体只有 text/style/key，但官方社区技术支持明确说过
    ``button_list`` 的按钮设 ``type=1`` 可跳转 url）。带上它，用户点一下就
    直接打开页面，不用等服务端换卡再点第二次。

    url 里只有 ``task_id``、没有 token，拿到 url 也换不出数据；真正的 token
    要等页面打开后拿 task 去换（服务端按"最近一次按钮点击者"判定身份）。
    """
    card_cfg = cfg.get("card") or {}
    button: dict[str, Any] = {
        "text": _truncate(button_text or "打开查数面板", 20),
        "style": 1,
        "key": json.dumps(
            {"a": "open", "n": task_id},
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    }
    if jump_url:
        button["type"] = 1
        button["url"] = jump_url
    return {
        "card_type": "button_interaction",
        "task_id": task_id,
        "main_title": {
            "title": _truncate(card_cfg.get("title") or "手动查数", TITLE_MAX),
            "desc": _truncate(
                card_cfg.get("buttonDesc") or "点击按钮，打开只属于你的面板",
                DESC_MAX,
            ),
        },
        "button_list": [button],
    }


def _notice_card(task_id: str, title: str, desc: str, url: str = "") -> dict:
    """text_notice 卡（必须有 card_action，且 type 只能是 1/2）。"""
    return {
        "card_type": "text_notice",
        "task_id": task_id,
        "main_title": {
            "title": _truncate(title, TITLE_MAX),
            "desc": _truncate(desc, DESC_MAX),
        },
        "card_action": {
            "type": 1,
            "url": url or "https://qwenpaw.agentscope.io",
        },
    }


def build_owner_link_card(
    task_id: str,
    token: str,
    cfg: dict[str, Any],
) -> dict[str, Any]:
    """给本人看的「带链接」面板卡（text_notice，点卡任意位置即打开）。

    这张卡只会在两个地方出现，**且都只有本人看得到**：
      * 群聊按钮回调里替换给点击者（``_handle_panel_button``）
      * 一步交付里替换给发起人（``deliver_card_to_owner_only``）

    群里那张卡本体永远是不含链接的按钮卡，所以链接不会在群里扩散。
    """
    guard = cfg.get("groupGuard") or {}
    card, _url = build_card(cfg, token)
    card["task_id"] = task_id
    lt = str(guard.get("linkCardTitle") or "").strip()
    ld = str(guard.get("linkCardDesc") or "").strip()
    mt = card.setdefault("main_title", {})
    if lt:
        mt["title"] = _truncate(lt, TITLE_MAX)
    if ld:
        mt["desc"] = _truncate(ld, DESC_MAX)
    return card


def pick_group_card(
    guard: dict[str, Any],
    cfg: dict[str, Any],
    task_id: str,
    token: str,
    owner_id: str,
    button_text: str,
    jump_url: str = "",
) -> tuple[dict[str, Any], list[str] | None, bool]:
    """群聊这次该发哪张卡、要不要定向可见。

    返回 ``(card, visible_to, one_step)``。

    **安全底线（写死，不给人误配的机会）**：``oneStep`` 只有在 ``visibleToUser``
    同时为真、且拿得到 owner 时才生效 —— 没有定向可见就发带链接的卡，等于把
    链接贴到群里。所以：

      visibleToUser=false + oneStep=true  →  仍是**无链接**的按钮卡（拒绝换卡）
      visibleToUser=true  + oneStep=false →  无链接按钮卡，但只给发起人看（探针）
      visibleToUser=true  + oneStep=true  →  带链接整卡跳转卡，点 1 次打开
    """
    visible = bool(guard.get("visibleToUser", False)) and bool(owner_id)
    one_step = visible and bool(guard.get("oneStep", False))
    if one_step:
        card = build_owner_link_card(task_id, token, cfg)
    else:
        card = build_panel_button_card(task_id, cfg, button_text, jump_url)
    return card, ([owner_id] if visible else None), one_step


async def deliver_card_to_owner_only(
    channels: list[Any],
    frame: Any,
    card: dict[str, Any],
    owner: str,
) -> tuple[bool, str]:
    """差异化更新：只把 ``owner`` 看到的那张卡换掉，别人看到的还是原卡。

    返回 ``(ok, reason)``。**失败一律当作"保留原卡"处理** —— 原卡是不含
    链接的按钮卡，所以任何失败都只是退回两步，绝不会把链接漏到群里。
    """
    if not owner:
        return False, "no owner userid"
    last = "no usable channel"
    for ch in channels:
        client = getattr(ch, "_client", None)
        fn = getattr(client, "update_template_card", None)
        if not callable(fn):
            last = f"{_label(ch)}: update_template_card unavailable"
            continue
        try:
            res = await fn(frame, card, [owner])
        except Exception as exc:  # noqa: BLE001 - 异常绝不能冒到发卡主链路
            last = f"{_label(ch)}: {type(exc).__name__}: {exc}"[:300]
            continue
        ok, why = _check_reply(res)
        if ok:
            return True, f"ok via {_label(ch)}"
        last = f"{_label(ch)}: {why}"
    return False, last


async def _handle_panel_button(frame: Any, task_id: str) -> None:
    """群聊按钮点击：只把**点击者**看到的那张卡换成结果。"""
    body = frame.get("body") or {} if isinstance(frame, dict) else {}
    from_info = body.get("from") or {}
    userid = str(from_info.get("userid") or "")
    cfg = load_config()
    guard = cfg.get("groupGuard") or {}
    hint = str(guard.get("triggerHint") or "手动查询数据")
    item = _panel_buttons.get(task_id) or {}
    owner = str(item.get("owner") or "")

    # 记下"最近一次点这个按钮的人"：一步跳转（buttonJump）模式下，页面打开后
    # 要拿 task 来换 token，服务端只能靠这条记录判断打开者是不是本人。
    if item:
        item["last_click"] = {"userid": userid, "ts": time.time()}

    if not item:
        card = _notice_card(task_id, "⌛ 面板已过期", "请重新发送触发词获取新面板")
        kind = "expired"
    elif userid and userid == owner:
        # 本人：把带链接的面板卡给他（只他可见）。这张卡是 text_notice 整卡
        # 跳转 —— 点卡片任意位置即可打开，比让他在替换后的卡上再找一个小
        # 按钮更符合直觉（一步跳转实测不可行，见配置注释）。
        card = build_owner_link_card(task_id, str(item.get("token") or ""), cfg)
        kind = "owner"
    else:
        name = str(item.get("owner_name") or owner)
        # 触发时 meta 通常已带 user_name，直接用；查不到中文名再回 auth-center
        # （回调有 5s 窗口，能省一次查询就省）
        if not name or name == owner:
            try:
                scope = await asyncio.wait_for(auth_scope(owner), timeout=2.0)
                name = str((scope or {}).get("userName") or "") or name
            except Exception:  # noqa: BLE001 - 查不到就用 loginid，不能卡住
                pass
        card = _notice_card(
            task_id,
            "🔒 这不是你的面板",
            f"它由 {name} 发起，只有本人可用。请在群里自己发送「{hint}」。",
        )
        kind = "other"

    upd_ok, upd_reason = await deliver_card_to_owner_only(
        cached_channels(), frame, card, userid
    )
    logger.info(
        "%s panel button kind=%s task=%s user=%s owner=%s %s reason=%s",
        LOG_PREFIX, kind, task_id, userid, owner,
        "UPDATED" if upd_ok else "FAILED", upd_reason[:300],
    )


def _issue_panel_token(item: dict[str, Any], owner: str, cfg: dict[str, Any]) -> str:
    """按面板归属重新签一张 token（group + guard_free）。"""
    return issue_token(
        sender_id=owner,
        chatid=str(item.get("chatid") or ""),
        chat_type="group",
        session_id=str(item.get("session_id") or ""),
        ttl=int((cfg.get("security") or {}).get("token_ttl_sec", TOKEN_TTL_SEC)),
        guard_free=True,
    )


def redeem_panel_token(
    task_id: str,
    device: str,
    cfg: dict[str, Any],
) -> tuple[bool, str, str, str]:
    """一步跳转模式：页面打开后拿 ``task_id`` 换 token。

    返回 ``(ok, token, code, message)``。code 取值：
      ok / pending（回调还没到，前端继续轮询）/ not_owner（点按钮的是别人）
      / expired / too_many / disabled

    判定依据是**最近一次按钮点击者**（``_handle_panel_button`` 在回调里记下
    的 ``body.from.userid``）。因为按钮 url 里只有 task_id、没有 token，
    别人就算拿到这个 url 也换不出数据。
    """
    guard = _guard_cfg(cfg)
    if not guard.get("buttonJump", False):
        return False, "", "disabled", "一步跳转未启用"
    _clean_panel_buttons(float(guard.get("buttonTtlSec", 3600) or 3600))
    item = _panel_buttons.get(task_id)
    if not item:
        return False, "", "expired", "面板已过期，请重新发送触发词获取新卡片。"

    hint = str(guard.get("triggerHint") or "手动查询数据")
    owner = str(item.get("owner") or "")
    device = str(device or "").strip()
    devices = item.setdefault("devices", set())

    # 这台设备换过一次就算自己人：别人点按钮会把 last_click 顶掉，若只看
    # last_click，本人换设备/刷新反而会被误伤，所以先认设备。
    if device and device in devices:
        return True, _issue_panel_token(item, owner, cfg), "ok", ""

    last = item.get("last_click") or {}
    who = str(last.get("userid") or "")
    if not who:
        # 页面比回调先到（或跳转型按钮压根不推回调）→ 让前端再等等
        return False, "", "pending", "正在确认身份…"
    if who != owner:
        name = str(item.get("owner_name") or owner)
        return False, "", "not_owner", (
            f"这是 {name} 的查数面板，只有本人可以使用。"
            f"如需查询，请在群里自己发送「{hint}」，机器人会给你只属于你的面板。"
        )

    cap = int(guard.get("jumpRedeemMax", 6) or 6)
    used = int(item.get("redeem_n") or 0)
    if used >= cap:
        return False, "", "too_many", "这张面板打开次数过多，请重新发送触发词获取新卡片。"
    item["redeem_n"] = used + 1
    if device:
        devices.add(device)
    return True, _issue_panel_token(item, owner, cfg), "ok", ""


async def send_probe_card(
    channels: list[Any],
    frame: Any,
    owner: str,
    chat_type: str = "",
    chatid: str = "",
) -> tuple[bool, str]:
    """发一张 button_interaction 探针卡，返回 ``(是否发出, task_id)``。

    优先走 ``send_message``（主动推送，不占入站帧的 ``req_id``）：
    同一 ``req_id`` 连续 reply 两次有覆盖风险，而探针卡经常要和正常查数卡
    一起发（``alsoOnTrigger``）。没有 chatid 时（单聊）用 userid 作为
    chatid（``client.py:296`` 明确支持），仍失败才回退 ``reply_template_card``。
    """
    nonce = secrets.token_hex(4)
    task_id = f"{_PROBE_TASK_PREFIX}{nonce}"
    _probe_nonces[task_id] = {"owner": owner, "ts": time.time()}
    card = build_probe_card(nonce)
    target = str(chatid or "").strip() or str(owner or "").strip()
    body = {"msgtype": "template_card", "template_card": card}

    if target:
        for ch in channels:
            client = getattr(ch, "_client", None)
            fn = getattr(client, "send_message", None)
            if not callable(fn):
                continue
            try:
                res = await fn(target, body)
                logger.warning(
                    "%s PROBE card sent(proactive) task=%s owner=%s chat=%s "
                    "target=%s res=%s",
                    LOG_PREFIX, task_id, owner, chat_type, target, _frame_brief(res),
                )
                return True, task_id
            except Exception:  # noqa: BLE001
                logger.exception(
                    "%s PROBE proactive send failed on %s", LOG_PREFIX, _label(ch)
                )

    for ch in channels:
        client = getattr(ch, "_client", None)
        fn = getattr(client, "reply_template_card", None)
        if not callable(fn):
            continue
        try:
            res = await fn(frame, card)
            logger.warning(
                "%s PROBE card sent(reply) task=%s owner=%s chat=%s res=%s",
                LOG_PREFIX, task_id, owner, chat_type, _frame_brief(res),
            )
            return True, task_id
        except Exception:  # noqa: BLE001
            logger.exception(
                "%s PROBE card send failed on %s", LOG_PREFIX, _label(ch)
            )
    logger.warning("%s PROBE no channel could send card", LOG_PREFIX)
    return False, task_id


def _label(channel: Any) -> str:
    bot = getattr(channel, "bot_id", None) or getattr(channel, "_bot_id", None)
    return f"{type(channel).__name__}({bot or 'no-bot-id'})"


def diagnose(channel: Any) -> str:
    """把 channel / client 的关键事实一次性打全，P0 阶段靠它定位。"""
    if channel is None:
        return "channel=None"
    if inspect.isawaitable(channel):
        return "channel is awaitable (missing await!)"
    client = getattr(channel, "_client", None)
    methods = (
        sorted(m for m in dir(client) if m.startswith(("reply_", "send_")))
        if client is not None
        else []
    )
    return (
        f"{_label(channel)} enabled={getattr(channel, 'enabled', '<none>')!r} "
        f"client={type(client).__name__ if client is not None else None} "
        f"methods={methods}"
    )


def _check_reply(result: Any) -> tuple[bool, str]:
    """WS 发送是单向的（``aibot/client.py:127`` send_reply 不等业务回执），
    所以"没抛异常"不等于卡片真的发出去了。回执帧若带了 errcode 就必须认账。
    """
    if isinstance(result, dict):
        code = result.get("errcode")
        if code not in (None, 0, "0"):
            return False, f"errcode={code} errmsg={result.get('errmsg')!r}"
    return True, "ok"


def take_processing_sid(request: Any, channels: list[Any]) -> str:
    """取出宿主的"思考中"占位流 id，并注销它的 keepalive。

    ``wecom/channel.py:1081`` 会在每轮开始先发一条 ``🤔 Thinking...`` 的
    流式占位（``finish=False``），把 stream_id 暂存在
    ``request._wecom_processing_stream_id`` 上。

    它只能被"定稿"，不能撤回（aibot client 没有 recall 能力）。若不接管：
      * 有真实回复时会被内容覆盖 → 正常；
      * 但我们 SHORT_CIRCUIT 后 payload 是空的，``renderer.py:227`` 的
        ``if btype == "text" and b.get("text")`` 会把空文本过滤掉 → parts 为空
        → ``send_message_content`` 直接 return → **占位从未被 finish**
        → keepalive 每 20s 刷新一次，撑到 180s 才 force-finish。
    这就是那个甩不掉的空气泡的来源。

    取出后**立刻置空**，避免下游 ``_inject_processing_sid``
    （``channel.py:1108``）再把它注回 send_meta 造成重复 finish。
    """
    sid = str(getattr(request, "_wecom_processing_stream_id", "") or "")
    if not sid:
        return ""
    for ch in channels:
        tasks = getattr(ch, "_keepalive_tasks", None)
        if isinstance(tasks, dict):
            task = tasks.pop(sid, None)
            if task is not None and not task.done():
                task.cancel()
    try:
        setattr(request, "_wecom_processing_stream_id", "")
    except Exception:  # noqa: BLE001 - request 可能不允许写属性
        logger.debug("%s cannot clear processing sid", LOG_PREFIX)
    return sid


def sid_still_owned_by_host(request: Any, channels: list[Any]) -> bool:
    """占位 sid 是否还"活"在宿主手里、能被自然顶替。

    host 模式的全部前提。``send_content_parts``（``channel.py:1362-1371``）
    只有在 ``request._wecom_processing_stream_id`` 还没被清、
    **且** keepalive task 仍在 ``_keepalive_tasks`` 里时才肯复用这条流；
    两者缺一那条占位就真的没人管了。真发生时我们要立刻改发套路，
    而不是把一句注定落不到占位上的话塞进 payload。
    """
    sid = str(getattr(request, "_wecom_processing_stream_id", "") or "")
    if not sid:
        return False
    for ch in channels:
        tasks = getattr(ch, "_keepalive_tasks", None)
        if isinstance(tasks, dict) and sid in tasks:
            task = tasks[sid]
            if not (hasattr(task, "done") and task.done()):
                return True
    return False


async def watch_host_placeholder(
    request: Any,
    channels: list[Any],
    frame: Any,
    stream_id: str,
    text: str,
    delay: float = 6.0,
) -> None:
    """host 模式的兜底：等一会儿看看宿主到底有没有把占位用掉。

    正常情况下 ``send_content_parts`` 会在毫秒级内 pop 掉 keepalive task、
    借这条 sid 发第一批内容，占位当场被顶替。若 ``delay`` 秒后 task 还挂在
    ``_keepalive_tasks`` 里，说明这一轮的 completed 事件压根没走到它的收尾
    流程（或者它不愿意用这条流）——那我们再自己 finish，免得把上次的
    "空气泡"又请回来。
    """
    if not stream_id:
        return
    try:
        await asyncio.sleep(delay)
    except asyncio.CancelledError:
        return
    for ch in channels:
        tasks = getattr(ch, "_keepalive_tasks", None)
        if not isinstance(tasks, dict) or stream_id not in tasks:
            logger.info("%s placeholder consumed by host sid=%s…", LOG_PREFIX, stream_id[:16])
            return
    # 兜底时才真正接管：参照 host 自 recycl 的顺序，先把 keepalive 掐掉
    for ch in channels:
        tasks = getattr(ch, "_keepalive_tasks", None)
        if isinstance(tasks, dict):
            task = tasks.pop(stream_id, None)
            if task is not None and not task.done():
                task.cancel()
    try:
        setattr(request, "_wecom_processing_stream_id", "")
    except Exception:  # noqa: BLE001
        pass
    ok, reason = await finish_placeholder(channels, frame, stream_id, text)
    logger.warning(
        "%s placeholder NOT consumed within %.0fs; self-finished %s reason=%s sid=%s…",
        LOG_PREFIX,
        delay,
        "OK" if ok else "FAILED",
        reason,
        stream_id[:16],
    )


async def _send_one(
    channel: Any,
    frame: Any,
    card: dict[str, Any],
    stream_id: str = "",
    stream_content: str = "",
    visible_to: list[str] | None = None,
) -> tuple[bool, str]:
    """发一张卡片。

    ``stream_id`` 非空时走 ``reply_stream_with_card``（``aibot/client.py:209``），
    带上占位流的 id 把那条"思考中"**原地顶替成卡片** —— 用户最终只看到一条
    消息；不带 id 则是普通的 ``reply_template_card``，会多出独立一条。

    ⚠️ **``reply_stream_with_card`` 是未经验证的 API**：全仓库搜索宿主源码
    零处调用（`msgtype=stream_with_template_card` 从未被宿主用过），2026-09-17
    实测调用不报错但企微端**不渲染卡片**，因此默认关闭（``absorb_placeholder``）。
    再次启用前必须真机验证。另注：``content`` 传空串会让 stream 被定稿为空，
    怀疑是卡片不渲染的原因之一，故改为必填非空文案。
    """
    if inspect.isawaitable(channel):
        return False, "channel is coroutine (missing await)"
    if not getattr(channel, "enabled", False):
        return False, f"disabled [{diagnose(channel)}]"
    client = getattr(channel, "_client", None)
    if client is None:
        return False, f"no ws client [{diagnose(channel)}]"

    if stream_id:
        reply = getattr(client, "reply_stream_with_card", None)
        if not callable(reply):
            return False, f"no reply_stream_with_card [{diagnose(channel)}]"
        try:
            result = await reply(
                frame,
                stream_id=stream_id,
                content=stream_content or "请稍候…",
                finish=True,
                template_card=card,
            )
        except Exception as exc:  # noqa: BLE001
            return False, f"raised {type(exc).__name__}: {exc}"
        return _check_reply(result)

    reply = getattr(client, "reply_template_card", None)
    if not callable(reply):
        return False, f"no reply_template_card [{diagnose(channel)}]"

    # ---- 定向可见（visible_to_user）------------------------------------
    # 企微**应用消息** API 有这个字段：只有列表里的人看得到这条消息。智能机器人
    # 的 WS 协议没文档承诺支持，2026-09-21 前未知。它若成立，群聊就能一步打开
    # （直接发带链接卡 + 只给发起人看），不必再走"点按钮换卡"两步。
    #   ⚠️ 风险是字段被**静默忽略** —— 那样卡片会全员可见。所以主链路必须先
    #   用**不含链接**的卡验证可见性（visibleToUser=true + oneStep=false），
    #   肉眼确认群里其他人看不到之后，才允许打开 oneStep。
    # 服务端不认时 reply 会直接报错 → 自动退回全员可见，绝不让人收不到卡片。
    if visible_to:
        raw = getattr(client, "reply", None)
        if callable(raw):
            try:
                res = await raw(
                    frame,
                    {
                        "msgtype": "template_card",
                        "template_card": card,
                        "visible_to_user": list(visible_to),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                logger.info(
                    "%s visible_to_user rejected (%s: %s); fallback to plain card",
                    LOG_PREFIX, type(exc).__name__, str(exc)[:200],
                )
            else:
                v_ok, v_why = _check_reply(res)
                if v_ok:
                    return True, f"ok(visible) via {_label(channel)}"
                logger.info(
                    "%s visible_to_user ack error (%s); fallback to plain card",
                    LOG_PREFIX, v_why[:200],
                )

    try:
        result = await reply(frame, card)
    except Exception as exc:  # noqa: BLE001 - P0 就要看清错误长什么样
        return False, f"raised {type(exc).__name__}: {exc}"
    return _check_reply(result)


async def finish_placeholder(
    channels: list[Any], frame: Any, stream_id: str, text: str
) -> tuple[bool, str]:
    """用一段文本把占位流定稿。

    占位只能"定稿"、不能撤回，所以只要没被卡片吸收，就必须显式收尾；
    否则它会一直挂在会话里显示 ``🤔 Thinking...`` —— keepalive 已被我们
    取消、sid 也已清空，没人会再管它。
    """
    if not stream_id:
        return False, "no placeholder"
    for ch in channels:
        client = getattr(ch, "_client", None)
        reply = getattr(client, "reply_stream", None)
        if not callable(reply):
            continue
        try:
            result = await reply(
                frame, stream_id=stream_id, content=text, finish=True
            )
        except Exception as exc:  # noqa: BLE001
            return False, f"raised {type(exc).__name__}: {exc}"
        return _check_reply(result)
    return False, "no reply_stream capability"


async def send_card(
    channels: list[Any],
    frame: Any,
    card: dict[str, Any],
    stream_id: str = "",
    stream_content: str = "",
    visible_to: list[str] | None = None,
) -> tuple[bool, str]:
    """逐个候选尝试发卡片，第一个成功即止。返回 (是否成功, 说明)。

    多实例下 frame 不属于某个 client 时，服务端会直接报错，因此"试错"是
    安全的：发错只是一条失败日志，不会误发到别处。

    ``stream_id`` 非空时先尝试"占位吸收"（把 Thinking 占位就地变成卡片）；
    **一旦失败必须退回普通发卡** —— 优化失败绝不能让用户收不到卡片。
    """
    if not channels:
        return False, "no wecom channel"

    if stream_id:
        failures: list[str] = []
        for ch in channels:
            ok, reason = await _send_one(
                ch, frame, card, stream_id=stream_id, stream_content=stream_content
            )
            if ok:
                return True, f"absorbed via {_label(ch)}"
            failures.append(f"{_label(ch)}: {reason}")
        logger.info(
            "%s placeholder absorb failed (%s); fallback to plain card",
            LOG_PREFIX,
            " ; ".join(failures)[:300],
        )

    failures = []
    for ch in channels:
        ok, reason = await _send_one(ch, frame, card, visible_to=visible_to)
        if ok:
            return True, f"ok via {_label(ch)}"
        failures.append(f"{_label(ch)}: {reason}")
    return False, " ; ".join(failures)


async def send_text(
    channels: list[Any], text: str, meta: dict[str, Any]
) -> tuple[bool, str]:
    """退化路径：发一条 markdown 文本（带 H5 链接）。

    ``WecomChannel.send``（``wecom/channel.py:1409``）在有 frame 时优先走
    frame，无 frame 时退回 ``_client.send_message``，因此私聊（chatid 为空）
    也能送达 —— 这是卡片不可用时的兜底。
    """
    last = "no enabled channel"
    for ch in channels:
        if not getattr(ch, "enabled", False):
            continue
        try:
            await ch.send("", text, meta)
        except Exception as exc:  # noqa: BLE001
            last = f"{_label(ch)}: {type(exc).__name__}: {exc}"
            continue
        return True, f"ok via {_label(ch)}"
    return False, last


# ---------------------------------------------------------------------------
# Hook
# ---------------------------------------------------------------------------


class QueryCardTriggerHook(HookBase):
    """PRE_DISPATCH 命中触发词 → 借本轮 frame 回一张卡片。"""

    phase = Phase.PRE_DISPATCH
    name = "qdm_query_card.trigger"
    priority = 50

    async def run(self, ctx: Any) -> HookResult:
        started = time.perf_counter()
        try:
            return await self._run(ctx, started)
        except Exception as exc:  # noqa: BLE001 - 插件永不拖垮宿主
            logger.exception("%s hook crashed: %s", LOG_PREFIX, exc)
            return HookResult()

    async def _probe(self, ctx: Any, cfg: dict[str, Any], info: dict) -> Any:
        """方案 B 探针：发一张 button_interaction 卡，等点击回调验证身份。

        验证三件事（看日志）：
          1. 按钮点击是否真的推回 ``template_card_event``；
          2. 回调 ``body.from.userid`` 是不是点击者本人；
          3. ``update_template_card(userids=[点击者])`` 是否只对点击者生效。
        """
        if not info["has_frame"]:
            logger.warning("%s probe: no frame, skip", LOG_PREFIX)
            return HookResult()
        channels = candidate_channels(ctx)
        if not channels:
            single = await resolve_channel(ctx)
            channels = [single] if single is not None else []
        remember_channels(channels)
        _attach_card_listener(channels)

        request = getattr(ctx, "request", None)
        meta = getattr(request, "channel_meta", None) or {}
        frame = meta.get("wecom_frame")
        owner = str(meta.get("wecom_sender_id") or "")
        sent, task_id = await send_probe_card(
            channels,
            frame,
            owner,
            str(info.get("chat_type") or ""),
            chatid=str(meta.get("wecom_chatid") or ""),
        )

        tip = (
            "探针卡片已发出，请**点一下卡片里的按钮**，然后把企微端看到的"
            "变化告诉我（尤其是群里其他人是否也变了）。"
            if sent else "探针卡片发送失败，详见日志。"
        )
        return HookResult(action=HookAction.SHORT_CIRCUIT, payload=_reply_msg(tip))

    async def _run(self, ctx: Any, started: float) -> HookResult:
        cfg = load_config()
        if not cfg.get("enabled", True):
            return HookResult()

        text = input_text(ctx)
        if not text:
            return HookResult()

        hit, how = match_trigger(text, cfg)
        info = probe_meta(ctx)

        # 企微入站时顺手挂上卡片事件监听（幂等）。放在这里是因为此时一定能
        # 拿到 channel 实例，而 register() 阶段 WS 还没起来。
        # 注意：这**不只是探针用**——群聊按钮交付（buttonDelivery）要靠它收
        # 按钮回调，所以无条件挂载，不受 probe 开关影响。
        pcfg = _probe_cfg(cfg)
        # 开关名义上归 groupGuard（正式依赖），旧配置放在 probe 段下也兼容
        attach = bool(
            (cfg.get("groupGuard") or {}).get(
                "attachListener", pcfg.get("attachListener", True)
            )
        )
        if attach and info["has_frame"]:
            try:
                _attach_card_listener(candidate_channels(ctx))
            except Exception:  # noqa: BLE001
                logger.exception("%s attach listener failed", LOG_PREFIX)

        if pcfg.get("enabled"):
            # 群里消息带「@机器人」前缀，必须剥离后再比，否则永远匹配不上
            probe_trigger = str(pcfg.get("trigger") or "")
            if probe_trigger and strip_mention(text) == probe_trigger:
                return await self._probe(ctx, cfg, info)

        if not hit:
            # 不是触发词 → 追问增强：把 direct 的上次结果补回 Agent 上下文，
            # 或识别「上月/上周」这类时间追问直接重查（见 followUp 配置）。
            return await _follow_up(ctx, cfg, text, info)
        # 没有 frame 说明本轮不是企微入站（CLI / 其他渠道）——天然过滤，
        # 不必再按 agent 逐个配白名单。
        logger.info(
            "%s TRIGGERED how=%s text=%r frame=%s meta_keys=%s chat_type=%s agent=%s",
            LOG_PREFIX,
            how,
            text[:80],
            info["has_frame"],
            info["meta_keys"],
            info["chat_type"],
            info["agent_id"],
        )

        if not info["has_frame"]:
            logger.warning(
                "%s no wecom_frame this turn; skip card (%s)", LOG_PREFIX, how
            )
            return HookResult()

        cfg = load_config()
        delivery = cfg.get("delivery") or {}
        channels = candidate_channels(ctx)
        if not channels:
            # 兜底：manager 暴露方式变了，退回官方 get_channel
            single = await resolve_channel(ctx)
            channels = [single] if single is not None else []
        request = getattr(ctx, "request", None)
        meta = getattr(request, "channel_meta", None) or {}
        frame = meta.get("wecom_frame")

        # 缓存触发 frame：submit 注入查询时透传回去，让结果流式落回原会话
        _remember_frame(str(getattr(ctx, "session_id", "") or ""), frame)

        # HTTP 侧没有 ctx，回推只能靠这里缓存下来的实例
        remember_channels(channels)

        # 群聊「按钮交付」判定必须在签发 token **之前**：按钮模式下链接只递给
        # 点按钮的本人，认领机制会挡住本人换设备，所以这类 token 直接标 gf=1。
        is_group = bool(meta.get("is_group")) or str(
            info.get("chat_type") or ""
        ).lower() == "group"
        guard = cfg.get("groupGuard") or {}
        button_delivery = is_group and bool(guard.get("buttonDelivery", False))
        token = issue_token(
            sender_id=str(meta.get("wecom_sender_id") or ""),
            chatid=str(meta.get("wecom_chatid") or ""),
            chat_type=str(
                meta.get("wecom_chat_type") or ("group" if meta.get("is_group") else "single")
            ),
            session_id=str(getattr(ctx, "session_id", "") or ""),
            ttl=int((cfg.get("security") or {}).get("token_ttl_sec", TOKEN_TTL_SEC)),
            guard_free=button_delivery,
        )

        if button_delivery:
            _clean_panel_buttons(float(guard.get("buttonTtlSec", 3600) or 3600))
            task_id = f"{_PANEL_TASK_PREFIX}{secrets.token_hex(6)}"
            owner_id = str(meta.get("wecom_sender_id") or "")
            _panel_buttons[task_id] = {
                "owner": owner_id,
                # meta 里通常直接带 user_name，省一次 auth-center 查询
                "owner_name": str(meta.get("user_name") or "") or owner_id,
                "chatid": str(meta.get("wecom_chatid") or ""),
                "session_id": str(getattr(ctx, "session_id", "") or ""),
                "token": token,
                "ts": time.time(),
            }
            button_text = str(guard.get("buttonText") or "打开查数面板")
            jump_url = ""
            if bool(guard.get("buttonJump", False)):
                base = h5_base_url(cfg)
                if base:
                    jump_url = f"{base}?task={urllib.parse.quote(task_id)}"

            card, visible_to, one_step = pick_group_card(
                guard, cfg, task_id, token, owner_id, button_text, jump_url
            )
            # one_step 时链接在卡片里；这个变量只用于 markdown 兜底，恒空，
            # 免得哪天兜底被打开把链接泄到群里
            url = ""
            if jump_url:
                logger.info(
                    "%s button jump enabled task=%s url=%s",
                    LOG_PREFIX, task_id, jump_url,
                )
            if visible_to:
                logger.info(
                    "%s visible_to_user=[%s] one_step=%s task=%s",
                    LOG_PREFIX, owner_id, one_step, task_id,
                )
            # 关键：按钮模式下**必须**关掉 markdown 兜底 —— 否则卡片一旦发送
            # 失败，兜底会把链接以 markdown 发到群里，链接就泄漏了。
            delivery = dict(delivery)
            delivery["fallback_markdown"] = False
            delivery["placeholder_closing_text"] = str(
                guard.get("buttonClosingText")
                or "查数面板已生成，请点击下方卡片里的按钮打开（链接只有你能看到）。"
            )
            delivery["fallback_text"] = str(
                guard.get("buttonFallbackText") or "查数面板发送失败，请重新发送触发词。"
            )
            logger.info(
                "%s group button delivery task=%s owner=%s", LOG_PREFIX, task_id, owner_id
            )
        else:
            card, url = build_card(cfg, token)
        short_circuit = bool(delivery.get("short_circuit", False))
        absorb = bool(delivery.get("absorb_placeholder", False))
        mode = str(delivery.get("placeholder_mode") or "host").strip().lower()
        # 是否接管占位流，完全取决于 mode：
        #   host —— 一个字都不碰，留给宿主的自然收尾链路
        #           （``on_event_message_completed`` 注入 sid → send_content_parts
        #           用这条 sid 发第一批 chunk → 占位被原生顶替）。我们一旦 pop 了
        #           keepalive task，``channel.py:1370`` 就会判定 sid 已失效并弃用它
        #           → 立刻退化成没人收尾的空气泡，所以这条千万不能手贱。
        #   self —— 抢过来自己 finish（退化路径）
        sid = ""
        if short_circuit and mode == "self":
            sid = take_processing_sid(request, channels)
        logger.info(
            "%s candidates=%d frame=%s mode=%s placeholder=%s -> %s",
            LOG_PREFIX,
            len(channels),
            type(frame).__name__,
            mode,
            sid[:16] + "…" if sid else "none",
            [diagnose(c) for c in channels],
        )
        # 探针：顺手再发一张 button_interaction 卡，省得记新触发词
        if pcfg.get("enabled") and pcfg.get("alsoOnTrigger"):
            try:
                await send_probe_card(
                    channels,
                    frame,
                    str(meta.get("wecom_sender_id") or ""),
                    str(info.get("chat_type") or ""),
                    chatid=str(meta.get("wecom_chatid") or ""),
                )
            except Exception:  # noqa: BLE001 - 探针绝不能影响正常发卡
                logger.exception("%s probe card alongside failed", LOG_PREFIX)

        ok, reason = await send_card(
            channels,
            frame,
            card,
            sid if absorb else "",
            stream_content=str(delivery.get("placeholder_closing_text") or ""),
            visible_to=visible_to,
        )

        # ---- 一步交付（instantDelivery，⚠️ 已证伪）-----------------------
        # 卡片发出去后，立刻试着只把**发起人**看到的那张卡换成带链接的面板卡。
        # **2026-09-21 真机结论：走不通。** 企微返回
        #   errcode=846606 "request already responded, cannot respond again"
        # 即同一个 req_id 只允许响应一次——发卡已经用掉了它，没有第二次机会。
        # 保留代码只为留档，**不要打开这个开关**，改走 visibleToUser + oneStep。
        instant = False
        if ok and button_delivery and bool(guard.get("instantDelivery", False)):
            try:
                link_card = build_owner_link_card(task_id, token, cfg)
                instant, why = await deliver_card_to_owner_only(
                    channels, frame, link_card, owner_id
                )
                logger.info(
                    "%s instant delivery %s task=%s owner=%s reason=%s",
                    LOG_PREFIX,
                    "OK" if instant else "FAILED",
                    task_id,
                    owner_id,
                    str(why)[:300],
                )
            except Exception:  # noqa: BLE001 - 实验路径炸了也只是退回按钮卡
                logger.exception(
                    "%s instant delivery crashed task=%s", LOG_PREFIX, task_id
                )
        if instant:
            delivery["placeholder_closing_text"] = str(
                guard.get("instantClosingText")
                or "查数面板已就绪，请点击上方卡片打开（链接只有你能看到）。"
            )

        # 带 type/url 的按钮是企微的未文档化能力，服务端可能直接拒收。
        # 拒了就退回纯回调按钮，绝不能让用户收不到卡片。
        if not ok and button_delivery and jump_url:
            logger.warning(
                "%s jump button rejected (%s); retry as plain callback button",
                LOG_PREFIX, reason[:200],
            )
            card = build_panel_button_card(task_id, cfg, button_text, "")
            ok, reason = await send_card(
                channels,
                frame,
                card,
                sid if absorb else "",
                stream_content=str(delivery.get("placeholder_closing_text") or ""),
            )

        # 占位没被卡片吸收（退化路径）→ 必须给它一个收尾，否则就变成一条
        # 永远停在 "🤔 Thinking..." 的孤儿消息。
        if sid and not reason.startswith("absorbed"):
            closing = str(
                delivery.get("placeholder_closing_text")
                or "已为你打开查数面板，请在上方卡片中选择条件。"
            )
            ok_ph, reason_ph = await finish_placeholder(channels, frame, sid, closing)
            logger.info(
                "%s placeholder closed %s reason=%s text=%r",
                LOG_PREFIX,
                "OK" if ok_ph else "FAILED",
                reason_ph,
                closing,
            )

        markdown_sent = False
        if not ok and delivery.get("fallback_markdown", False):
            card_cfg = cfg.get("card") or {}
            markdown = f"[{card_cfg.get('title') or '手动查数'}]({url})"
            if card_cfg.get("desc"):
                markdown += f"\n{card_cfg['desc']}"
            ok_md, reason_md = await send_text(channels, markdown, meta)
            markdown_sent = ok_md
            logger.info(
                "%s markdown fallback %s reason=%s text=%r",
                LOG_PREFIX,
                "SENT" if ok_md else "FAILED",
                reason_md,
                markdown,
            )

        logger.info(
            "%s card %s reason=%s url=%s elapsed=%.0fms",
            LOG_PREFIX,
            "SENT" if ok else "FAILED",
            reason,
            url,
            (time.perf_counter() - started) * 1000,
        )

        short_circuit = bool(delivery.get("short_circuit", False))
        if not short_circuit:
            # 放行让 Agent 正常作答（调试期用；正式环境建议开 true）
            return HookResult()

        # 短路后 Agent 不再说话，这里要同时决定两件事：
        #   1. payload 带不带字 —— host 模式下必须带，占位要靠这段字被顶掉
        #      （空文本会被 ``channel.py:1374`` 整段跳过，占位没人管）；
        #      但这种情况下宿主会先借占位流发同一个 sid 的第一段 chunk，
        #      占位自然定稿，我们不再需要第三张流的旁门左道。
        #   2. 卡片没发出去时，至少要让用户拿到链接。
        fallback = str(delivery.get("fallback_text") or "")
        closing = str(
            delivery.get("placeholder_closing_text")
            or "已为你打开查数面板，请在上方卡片中选择条件。"
        )
        if ok or markdown_sent:
            host_owned = mode == "host" and sid_still_owned_by_host(request, channels)
            text_for_stream = closing if host_owned else fallback
            if host_owned:
                # 记账：把这一轮的占位交给宿主，同时留个延时哨兵。
                # 万一它没被消费（日志会打出 NOT consumed），由哨兵补刀，
                # 不会出现"安静 orphan"那种最难查的情况。
                sid = str(getattr(request, "_wecom_processing_stream_id", "") or "")
                delay = float(delivery.get("placeholder_watch_sec", 6) or 0)
                if sid and delay > 0:
                    task = asyncio.get_running_loop().create_task(
                        watch_host_placeholder(
                            request, channels, frame, sid, closing, delay
                        )
                    )
                    _watchdogs.add(task)
                    task.add_done_callback(_watchdogs.discard)
            logger.info(
                "%s placeholder mode=%s host_owned=%s payload=%r",
                LOG_PREFIX,
                mode,
                host_owned,
                text_for_stream,
            )
            return HookResult(
                action=HookAction.SHORT_CIRCUIT, payload=_reply_msg(text_for_stream)
            )
        # 卡片 + markdown 都没走通 → 至少把链接吐出来，否则像机器人没反应。
        return HookResult(
            action=HookAction.SHORT_CIRCUIT, payload=_reply_msg(fallback or url)
        )


async def _follow_up(ctx: Any, cfg: dict[str, Any], text: str, info: dict) -> Any:
    """未命中触发词时：给 Agent 补回上次 direct 结果（或时间追问直查）。"""
    fu = cfg.get("followUp") or {}
    if not fu.get("enabled", True):
        return HookResult()
    session_id = str(getattr(ctx, "session_id", "") or "")
    if not session_id:
        return HookResult()
    request = getattr(ctx, "request", None)
    meta = getattr(request, "channel_meta", None) or {}
    uid = str(meta.get("wecom_sender_id") or "")
    item = _last_query_of(session_id, uid, float(fu.get("ttlSec", 1800) or 1800))
    if not item:
        return HookResult()
    # 宿主重启后内存是空的，追问轮也要把 channel 实例补进缓存
    channels = candidate_channels(ctx)
    if channels:
        remember_channels(channels)

    # 1) 时间追问：改时间重查 + 推送，Agent 完全不跑（默认关）
    if fu.get("autoTimeShift"):
        shift = _match_time_shift(text)
        rng = item["body"].get("range") or {}
        new = (
            _shift_range(str(rng.get("start") or ""), str(rng.get("end") or ""), *shift)
            if shift else None
        )
        if new:
            body = json.loads(json.dumps(item["body"]))
            body["range"] = dict(body.get("range") or {})
            body["range"]["start"], body["range"]["end"] = new
            payload = dict(item["payload"])
            ok, out, why = await direct_query(payload, body)
            logger.info(
                "%s follow-up time shift %s -> %s %s why=%s",
                LOG_PREFIX, rng.get("start"), new[0], "OK" if ok else "FAIL", why,
            )
            if ok:
                sent, reason = await push_text(payload, out)
                if sent:
                    _remember_last_query(session_id, payload, body, out)
                    # 短路后 Agent 不跑；payload 必须非空，否则占位流无人收尾
                    return HookResult(
                        action=HookAction.SHORT_CIRCUIT,
                        payload=_reply_msg(
                            f"已按 {new[0]} ~ {new[1]} 重新查询，结果见上一条消息。"
                        ),
                    )
                logger.warning("%s follow-up push failed: %s", LOG_PREFIX, reason)
            # 直查失败 → 退回上下文注入，让 Agent 自己想办法

    # 2) 上下文注入：不额外发消息、不额外跑一轮 Agent
    if fu.get("contextInject", True):
        if _follow_up_context(ctx, item, cfg):
            logger.info(
                "%s follow-up context injected sid=%s user=%s",
                LOG_PREFIX, session_id, uid,
            )
    return HookResult()


def _reply_msg(text: str) -> Any:
    """SHORT_CIRCUIT 的 payload 契约上必须是 Msg 实例（``runtime/hooks.py:55-62``）。"""
    from agentscope.message import Msg, TextBlock

    return Msg(
        name="qdm-query-card",
        role="assistant",
        content=[TextBlock(type="text", text=text)],
    )


# ---------------------------------------------------------------------------
# 结果回推：把 H5 的提交结果送回原会话
# ---------------------------------------------------------------------------


def _names(items: Any, limit: int = 40) -> str:
    """把 ``[{code,name}]`` 渲染成 ``名称(code)``，超量截断。"""
    if not isinstance(items, list) or not items:
        return "（无）"
    parts: list[str] = []
    for it in items[:limit]:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or it.get("code") or "")
        code = str(it.get("code") or "")
        parts.append(f"{name}({code})" if code else name)
    shown = "、".join(parts)
    if len(items) > limit:
        shown += f" 等 {len(items)} 项"
    return shown


def render_summary(body: dict[str, Any]) -> str:
    """给 H5 完成页的条件摘要（人读的短文本，不含指令）。"""
    rng = body.get("range") or {}
    seg = " / ".join(x for x in (str(rng.get("start") or ""), str(rng.get("end") or "")) if x)
    parts = []
    if seg:
        parts.append(seg)
    m = _names(body.get("metrics"), limit=3)
    if m and m != "（无）":
        parts.append(m)
    d = _names(body.get("dims"), limit=3)
    if d and d != "（无）":
        parts.append(d)
    return " ｜ ".join(parts) or "（未指定条件）"


# ---------------------------------------------------------------------------
# 注入文本构造：召回友好的纯中文模板（2026-09-18 方向 A）
#
# 背景（实测结论，勿回退）：
#   注入消息会被 qdm-harness 的召回器当作用户问题做 bigram 检索，召回结果
#   决定注入哪些 wiki 文档。旧版富文本（ASCII 编码满天飞）会命中 >20 个
#   指标候选 → multi_single_candidate_limit_exceeded → 降级 free 模式，
#   注入 7 个通用文件共 ~84KB（其中 metrics/index.md 54KB 是 426 指标清单）。
#
#   召回器规则（retrieval.js / context/build.js）：
#   - exact 命中 = 「问题文本包含完整别名」（规范化后：去标点/空格、小写）；
#   - fuzzy 命中 = 别名 bigram 覆盖率 ≥0.5 且 ≥2 个（英文别名极易被 ASCII
#     大杂烩凑齐：af19SaleWeight + ST0001 就能凑出 knowLostWeight）；
#   - 指标 spec 恰好 1 个 → SINGLE（注入 spec+playbook，~25KB，最优）；
#   - 中英文别名同写会坏事：byTarget 按 targetPath 去重保留得分高的英文 ID，
#     丢掉中文 exact 项 → 中文兄弟指标失去 bigram 压制 → fuzzy 洪泛。
#
#   因此铁律：**指标只写纯中文名（不写英文 ID）**；口径/维度/过滤全用中文；
#   ASCII 只允许出现在日期与区域码（CN01/DC01 实测安全，ST0001 有毒，由
#   预检兜底）；高危通用词不出现（如「用户」会凑出「19点后用户数」）。
# ---------------------------------------------------------------------------

_CALIBER_ZH = {"SUMMARY": "汇总", "SALES_STORE_DAY_AVG": "店日均"}

_HARNESS_CLI_PATH = (
    Path.home()
    / ".qwenpaw/plugins/qdm-harness-qwenpaw/dist/data-harness-cli/src/main.js"
)
_HARNESS_INSTANCE_DIR = Path.home() / ".qdm/harness-data/instance"

_harness_root_cache: tuple[Path, Path] | None | bool = False  # False=未探测

_REC_REQ_TEXT = (
    "要求：调用查数工具执行本次查询并把结果直接回复。"
    "回复先用 1-3 句话给出要点，再用 markdown 表格展示（表头用指标名），"
    "行数超过 20 行时只展示前 20 行并注明总行数，金额数值可换算成万元并注明，"
    "若报错请把错误信息用通俗语言转述。数据权限由服务端自动求交，无需手动传参。"
)


def _han_only(items: Any, limit: int = 40) -> list[str]:
    """取 ``[{code,name}]`` 里的中文名列表（丢弃英文编码）。"""
    out: list[str] = []
    if not isinstance(items, list):
        return out
    for it in items[:limit]:
        if isinstance(it, dict):
            name = str(it.get("name") or "").strip()
            if name:
                out.append(name)
    return out


def _render_han_submission(body: dict[str, Any], include_value_ids: bool) -> str:
    """召回友好的注入文本：指标/口径/维度/过滤全中文。

    ``include_value_ids``：过滤值是否带 ID。区域码（CN01/DC01）实测安全，
    但门店码（ST0001）会与英文别名撞 bigram（如 knowLostWeight），所以
    带 ID 的版本必须过召回预检，不过就降级到无 ID 版（Agent 按 playbook
    里的 ``dim values`` 流程自行查码）。
    """
    rng = body.get("range") or {}
    cmp_ = body.get("compare") or {}
    lines = ["[手动查数面板提交]"]

    start, end = str(rng.get("start") or ""), str(rng.get("end") or "")
    if start or end:
        grain = {
            "bizDate": "日",
            "bizWeek": "周",
            "bizMonth": "月",
            "bizYear": "年",
        }.get(str(rng.get("grain") or ""), "")
        seg = f"{start} ~ {end}" if start and end else (start or end)
        if grain:
            seg += f"（{grain}粒度）"
        lines.append(f"时间：{seg}")

    policy = str(body.get("policyName") or body.get("policy") or "")
    policy = _CALIBER_ZH.get(policy, policy)
    if policy:
        lines.append(f"口径：{policy}")

    metrics = _han_only(body.get("metrics"))
    lines.append("指标：" + ("、".join(metrics) if metrics else "（无）"))
    dims = _han_only(body.get("dims"))
    lines.append("行维度：" + ("、".join(dims) if dims else "（无）"))

    filters = body.get("filters") or []
    if isinstance(filters, list) and filters:
        segs: list[str] = []
        for f in filters:
            if not isinstance(f, dict):
                continue
            dim_name = str(f.get("name") or "").strip()
            vals = f.get("values") or []
            labels: list[str] = []
            for v in vals[:50]:
                if not isinstance(v, dict):
                    continue
                vname = str(v.get("name") or "").strip()
                vid = str(v.get("id") or "").strip()
                if not vname and not vid:
                    continue
                if vname and vid and include_value_ids:
                    labels.append(f"{vname}({vid})")
                else:
                    labels.append(vname or vid)
            if labels:
                if dim_name:
                    segs.append(f"{dim_name}维度只看{'、'.join(labels)}")
                else:
                    segs.append("只看" + "、".join(labels))
        if segs:
            lines.append("过滤：" + "；".join(segs))

    on = [k for k, v in (("同比", cmp_.get("yoy")), ("环比", cmp_.get("mom"))) if v]
    if on:
        lines.append("对比：" + "、".join(on))

    lines.append(_REC_REQ_TEXT)
    return "\n".join(lines)


def _render_legacy_submission(payload: dict[str, Any], body: dict[str, Any]) -> str:
    """旧版富文本（ASCII 编码齐全，人好读但召回必降级 free）。

    仅作最后保底：预检不可用或两级中文模板都没命中时使用——功能正确性
    优先于注入体积。
    """
    rng = body.get("range") or {}
    cmp_ = body.get("compare") or {}
    lines = ["[手动查数] 用户在查数面板提交了一次查询："]

    start, end = str(rng.get("start") or ""), str(rng.get("end") or "")
    if start or end:
        grain = {
            "bizDate": "日",
            "bizWeek": "周",
            "bizMonth": "月",
            "bizYear": "年",
        }.get(str(rng.get("grain") or ""), "")
        seg = f"{start} ~ {end}"
        if grain:
            seg += f"（{grain}粒度）"
        lines.append(f"时间范围：{seg}")

    policy = str(body.get("policyName") or body.get("policy") or "")
    if policy:
        lines.append(f"统计口径：{policy}")

    lines.append(f"指标：{_names(body.get('metrics'))}")
    lines.append(f"维度：{_names(body.get('dims'))}")

    filters = body.get("filters") or []
    if isinstance(filters, list) and filters:
        lines.append("过滤条件（值必须用 ID，不得用名称）：")
        for f in filters:
            if not isinstance(f, dict):
                continue
            code = str(f.get("code") or "")
            name = str(f.get("name") or code)
            vals = f.get("values") or []
            ids, labels = [], []
            for v in vals[:50]:
                if not isinstance(v, dict):
                    continue
                vid = str(v.get("id") or "")
                if not vid:
                    continue
                ids.append(vid)
                vname = str(v.get("name") or "")
                labels.append(f"{vname}({vid})" if vname else vid)
            if ids:
                lines.append(
                    f"- {name}（{code}）：{('、'.join(labels))}"
                    + (f" 等 {len(vals)} 项" if len(vals) > 50 else "")
                )
                lines.append(f"  → 对应 CLI 参数：--filter {code}={','.join(ids)}")

    on = [k for k, v in (("同比", cmp_.get("yoy")), ("环比", cmp_.get("mom"))) if v]
    if on:
        lines.append("对比：" + "、".join(on))

    who = str(payload.get("u") or "")
    if who:
        lines.append(f"查询人：{who}")
    lines.append(f"提交时间：{time.strftime('%Y-%m-%d %H:%M:%S')}")

    lines.append(
        "请调用 qdm-metric-cli 执行查询，并把结果返回给用户。"
        "权限范围（区域/仓区域/大分类）由服务端 --data-auth 自动注入求交，无需手动传。"
    )
    lines.append(
        "结果呈现要求："
        "①先用 1-3 句话给结论/要点；"
        "②数据用 markdown 表格展示（表头=指标名，避免堆原始 JSON）；"
        "③行数很多时只展示前 20 行并注明总行数；"
        "④数值保留原始精度但可对金额做万元换算并注明；"
        "⑤查询报错时，把 CLI 的错误信息用通俗语言转述给用户，不要只贴错误码。"
    )
    return "\n".join(lines)


def _harness_cli_and_root() -> tuple[Path, Path] | None:
    """定位宿主 harness CLI 与实例根目录（结果缓存）。"""
    global _harness_root_cache
    if _harness_root_cache is not False:
        return _harness_root_cache or None
    _harness_root_cache = None
    try:
        if not _HARNESS_CLI_PATH.is_file():
            return None
        inst = _HARNESS_INSTANCE_DIR
        if not inst.is_dir():
            return None
        best: Path | None = None
        best_key = (-1.0, "")
        for child in inst.iterdir():
            if not child.is_dir() or not (child / ".harness").is_dir():
                continue
            try:
                key = (child.stat().st_mtime, child.name)
            except OSError:
                continue
            if key > best_key:
                best, best_key = child, key
        if best is not None:
            _harness_root_cache = (_HARNESS_CLI_PATH, best)
    except Exception as exc:  # noqa: BLE001 - 探测失败不能拖垮提交
        logger.debug("%s harness cli probe failed: %s", LOG_PREFIX, exc)
    return _harness_root_cache or None


def _recall_probe(text: str) -> tuple[str, int] | None:
    """用宿主同款召回器预演注入文本。

    返回 ``(mode, playbook 数)``，mode ∈ single/multi_single/report/free。
    任何异常/超时返回 ``None``（调用方跳过预检走保底模板）。
    每次调用起一个 node 子进程（~1-3s），只在提交时使用。
    """
    found = _harness_cli_and_root()
    if not found:
        return None
    cli, root = found
    node = shutil.which("node")
    if not node:
        return None
    env = dict(os.environ)
    env["PWD"] = str(root)
    env["HARNESS_WORKSPACE_ROOT"] = str(root)
    # WorkBuddy 等宿主会注入 NODE_OPTIONS（safe-delete shim），必须清掉
    env.pop("NODE_OPTIONS", None)
    try:
        result = subprocess.run(
            [node, str(cli), "context", "--question", text, "--format", "json"],
            cwd=str(root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            env=env,
            shell=False,
            check=False,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("%s recall probe error: %s", LOG_PREFIX, exc)
        return None
    if result.returncode != 0:
        return None
    try:
        data = json.loads(result.stdout)
    except Exception:  # noqa: BLE001
        return None
    instr = str(data.get("instruction") or "")
    m = re.search(r"Harness mode:\s*(\S+?)\.?\s", instr + " ")
    mode = m.group(1) if m else "free"
    n_play = sum(
        1
        for f in data.get("contextFiles") or []
        if str((f or {}).get("reason")) == "selected playbook"
    )
    return mode, n_play


def _probe_good(probe: tuple[str, int] | None, n_metrics: int) -> bool:
    """判定预演结果是否达标：注入的 playbook 必须恰好覆盖所选指标。"""
    if probe is None:
        return False
    mode, n_play = probe
    if n_metrics <= 1:
        return mode == "single" and n_play == 1
    return mode == "multi_single" and n_play == n_metrics


def pick_submission_text(payload: dict[str, Any], body: dict[str, Any]) -> tuple[str, str]:
    """三级降级挑选注入文本，返回 ``(text, tier)``。

    1. ``han``            纯中文模板，过滤值带 ID（最快：Agent 直接拼命令）；
    2. ``han_noid``       纯中文模板，过滤值不带 ID（Agent 按 playbook 用
       ``dim values`` 自查编码，多一次 CLI 调用）；
    3. ``han_unchecked``  同 ``han``，但预检器不可用、未经预演（期望最优）；
    4. ``legacy``         旧版富文本（召回降级 free、注入 84KB，功能保底）。

    前两级都必须通过召回预检（注入的 playbook 恰好覆盖所选指标）才可采用；
    预检器不可用时用 ``han_unchecked``（legacy 实测 100% free，不如它）。
    """
    t1 = _render_han_submission(body, True)
    t2 = _render_han_submission(body, False)
    n_metrics = len(_han_only(body.get("metrics")))

    probe1 = _recall_probe(t1)
    if _probe_good(probe1, n_metrics):
        return t1, "han"
    logger.info(
        "%s recall probe tier1 miss: %s (metrics=%d)", LOG_PREFIX, probe1, n_metrics
    )

    if t2 != t1:
        probe2 = _recall_probe(t2)
        if _probe_good(probe2, n_metrics):
            return t2, "han_noid"
        logger.info(
            "%s recall probe tier2 miss: %s (metrics=%d)", LOG_PREFIX, probe2, n_metrics
        )
    else:
        probe2 = None
        logger.info("%s recall probe tier2 skipped (same as tier1)", LOG_PREFIX)

    if probe1 is None and probe2 is None:
        # 预检器本身不可用（CLI/node 缺失）：宿主的召回器照跑，而 legacy
        # 实测 100% 降级 free（84KB）。此时 han 是期望最优解，直接采用。
        return t1, "han_unchecked"
    return _render_legacy_submission(payload, body), "legacy"


def inject_into_session(payload: dict[str, Any], text: str) -> tuple[bool, str]:
    """注入一条入站消息（``result_mode=inject``）。

    payload 结构抄自宿主自己的卡片回调 ``tool_guard.py:408-423``，与真实
    入站消息一致，因此等价于"用户又发了一条消息"：结果回到原群/私聊，
    且**进入该 session 上下文**，用户可继续追问。代价是触发 Agent 跑一轮。
    """
    try:
        from qwenpaw.schemas import ContentType, TextContent
    except Exception as exc:  # noqa: BLE001
        return False, f"import TextContent failed: {exc}"

    channels = cached_channels()
    if not channels:
        return False, "no cached wecom channel (trigger the bot once first)"

    sender_id = str(payload.get("u") or "")
    chatid = str(payload.get("c") or "")
    chat_type = "group" if int(payload.get("g") or 0) else "single"
    is_group = chat_type == "group"
    session_id = str(payload.get("s") or "")
    if not session_id:
        session_id = (
            f"wecom:group:{chatid}" if (is_group and chatid) else f"wecom:{sender_id}"
        )

    native = {
        "channel_id": "wecom",
        "sender_id": sender_id,
        "user_id": sender_id,
        "session_id": session_id,
        "content_parts": [TextContent(type=ContentType.TEXT, text=text)],
        "meta": {
            "wecom_sender_id": sender_id,
            "wecom_chatid": chatid,
            "wecom_chat_type": chat_type,
            "is_group": is_group,
            "from_card_action": True,
        },
    }

    # ⭐ 透传触发词的 frame（若还在 TTL 内）：注入轮由此获得 wecom_frame，
    # 复用宿主正常提问的回复链路 —— 流式开着时 reply_stream 落回原会话气泡；
    # 即使哪天流式被关，send_content_parts 也会走 frame 回复。没有这行，
    # 流式分支会在无 frame 时把整条回复静默丢弃（on_streaming_end 直接
    # return，on_event_message_completed 又被 streaming 接手而跳过）。
    cached = _lookup_frame(session_id)
    if cached is not None:
        native["meta"]["wecom_frame"] = cached
        logger.info("%s inject with cached frame (session=%s)", LOG_PREFIX, session_id)
    else:
        logger.warning(
            "%s inject WITHOUT frame (session=%s, TTL=%ss 内未触发过或已过期) "
            "—— 流式开启时结果可能无法送达",
            LOG_PREFIX,
            session_id,
            _FRAME_TTL_SEC,
        )

    failures = []
    for ch in channels:
        enqueue = getattr(ch, "_enqueue", None)
        if not callable(enqueue):
            failures.append(f"{_label(ch)}: no _enqueue")
            continue
        try:
            enqueue(native)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{_label(ch)}: {type(exc).__name__}: {exc}")
            continue
        return True, f"ok via {_label(ch)}"
    return False, " ; ".join(failures) or "no channel"


async def push_text(payload: dict[str, Any], text: str) -> tuple[bool, str]:
    """纯推送（``result_mode=push``）：不进上下文、不触发 Agent。"""
    sender_id = str(payload.get("u") or "")
    chatid = str(payload.get("c") or "")
    is_group = bool(int(payload.get("g") or 0))
    session_id = str(payload.get("s") or "")
    if not session_id:
        session_id = (
            f"wecom:group:{chatid}"
            if (is_group and chatid)
            else f"wecom:{sender_id}"
        )
    meta = {
        "wecom_sender_id": sender_id,
        "wecom_chatid": chatid,
        "wecom_chat_type": "group" if is_group else "single",
    }
    # ⭐ 必须透传触发词轮缓存的 frame：单聊 chatid 为空，宿主
    # ``wecom/channel.py:1449`` 在既无 frame 又无 chatid 时**静默丢弃**
    # 消息且 send() 不抛异常（2026-09-18 14:34 实测空气泡）。
    cached = _lookup_frame(session_id)
    if cached is not None:
        meta["wecom_frame"] = cached
        logger.info("%s push with cached frame (session=%s)", LOG_PREFIX, session_id)
    elif not (is_group and chatid):
        # 没有 frame 又是单聊 → 必然被宿主丢弃。与其假成功，不如显式失败
        # （direct 路径会回滚 nonce 并向 H5 报错，用户可重新发触发词）。
        logger.warning(
            "%s push impossible: no cached frame and empty chatid (session=%s)",
            LOG_PREFIX,
            session_id,
        )
        return False, "no frame/chatid（触发词超过30分钟或未触发过，请重新发送触发词后再提交）"
    else:
        logger.warning("%s push WITHOUT frame (session=%s)", LOG_PREFIX, session_id)
    return await send_text(cached_channels(), text, meta)


# ---------------------------------------------------------------------------
# 追问增强（followUp）：补回 direct 模式丢掉的会话上下文
#
# direct 模式的结果是「推给用户」而不是「进上下文」，用户追问「再看看上月的」
# 时 Agent 看不到上次结果。宿主给了官方口子 ``HookContext.inject_context``
# （``runtime/hooks.py:113``），runtime 会在跑 Agent 之前把它拼成一条 system
# 提示插到 ``input_msgs`` 最前面（``runtime.py:492-522``）——**不额外发消息、
# 不额外触发 Agent 轮次**，那一轮本来就要跑，只是让 Agent 多看见一段上下文。
# ---------------------------------------------------------------------------


def _remember_last_query(
    session_id: str, payload: dict[str, Any], body: dict[str, Any], text: str
) -> None:
    """记下本次 direct 查询，供下一轮追问使用。"""
    if not session_id:
        return
    now = time.time()
    for k in [k for k, v in _last_queries.items() if now - v["ts"] > 24 * 3600]:
        _last_queries.pop(k, None)
    _last_queries[session_id] = {
        "ts": now,
        "uid": str(payload.get("u") or ""),
        "payload": {
            "u": str(payload.get("u") or ""),
            "c": str(payload.get("c") or ""),
            "g": int(payload.get("g") or 0),
            "s": session_id,
        },
        "body": body,
        "text": text,
    }


def _last_query_of(session_id: str, uid: str, ttl: float) -> dict[str, Any] | None:
    """取本会话最近一次查询（过期/换人则返回 None）。

    群聊里 session_id 是整个群共享的，所以还要比对提问人，避免把 A 的
    查询结果塞给 B 的追问。
    """
    item = _last_queries.get(session_id)
    if not item:
        return None
    if ttl > 0 and time.time() - item["ts"] > ttl:
        _last_queries.pop(session_id, None)
        return None
    if uid and item.get("uid") and item["uid"] != uid:
        return None
    return item


def _add_months(d: date, n: int) -> date:
    """按月平移，月末自动收敛（3/31 - 1 月 = 2/28）。"""
    m = d.month - 1 + n
    y = d.year + m // 12
    m = m % 12 + 1
    return date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def _shift_range(start: str, end: str, unit: str, delta: int) -> tuple[str, str] | None:
    """把一段时间整体平移：保持原区间跨度，只改起点。"""
    try:
        d1, d2 = date.fromisoformat(start), date.fromisoformat(end)
    except ValueError:
        return None
    if unit == "month":
        n1, n2 = _add_months(d1, delta), _add_months(d2, delta)
    elif unit == "week":
        n1, n2 = d1 + timedelta(weeks=delta), d2 + timedelta(weeks=delta)
    else:
        n1, n2 = d1 + timedelta(days=delta), d2 + timedelta(days=delta)
    return n1.isoformat(), n2.isoformat()


# 顺序敏感：先匹配更长的说法（"上个月" 优先于 "上月" 其实等价，但
# "前一周"/"上周" 要排在 "周" 之前）
_TIME_SHIFT_RULES: list[tuple[str, int, tuple[str, ...]]] = [
    ("month", -1, ("上个月", "上月", "上一月", "前一个月", "前一月")),
    ("month", 0, ("本月", "这个月", "当月", "这月")),
    ("week", -1, ("上周", "上个周", "上一周", "上个星期", "上星期", "前一周")),
    ("week", 0, ("本周", "这周", "这个周", "这星期", "本星期")),
    ("day", -1, ("昨天", "昨日", "前一天", "前一日")),
    ("day", 0, ("今天", "今日", "本日", "当日")),
]

# 分析型追问：命中这些词说明用户要解读而不是重查，走上下文注入交给 Agent
_ANALYSIS_WORDS: tuple[str, ...] = (
    "为什么", "为啥", "为何", "什么原因", "原因", "怎么回", "怎么看", "如何",
    "分析", "解读", "拆解", "归因", "下降", "上升", "下滑", "增长", "波动",
    "异常", "趋势", "对比一下", "说明什么",
)


def _match_time_shift(text: str) -> tuple[str, int] | None:
    """识别「上月/上周」这类只要改时间的追问。

    命中排除词（为什么/原因/下降…）时不算——那是**分析型**追问，用户要的是
    解读而不是另一张表，交给 Agent + 上下文注入才对。宁可漏判也不要误判：
    误判会让用户问"为什么下降"时收到一张他没要的表。
    """
    for w in _ANALYSIS_WORDS:
        if w in text:
            return None
    for unit, delta, words in _TIME_SHIFT_RULES:
        for w in words:
            if w in text:
                return unit, delta
    return None


def _render_context_snippet(body: dict[str, Any], text: str, max_rows: int) -> str:
    """把上次查询压缩成一段给 Agent 看的上下文。"""
    lines = str(text or "").splitlines()
    keep = max(1, int(max_rows or 20)) + 2  # + 表头与分隔行
    table = "\n".join(lines[:keep])
    if len(lines) > keep:
        table += f"\n…（表格共 {len(lines)} 行，已截断）"
    return (
        "[查数面板上一次查询结果（插件直查，已推送给用户）]\n"
        f"查询条件：{render_summary(body)}\n"
        f"结果如下：\n{table}\n"
        "说明：这份数据已经发给用户了。如果用户的问题与它有关，请直接基于上述"
        "数据回答，**不要重复输出整张表格**，也不要重新调用查数工具；"
        "只有当时间范围或筛选条件与上述不同时，才需要重新查。"
    )


def _follow_up_context(ctx: Any, item: dict[str, Any], cfg: dict[str, Any]) -> bool:
    """把上次结果注入本轮 Agent 上下文。返回是否成功。"""
    fu = cfg.get("followUp") or {}
    injector = getattr(ctx, "inject_context", None)
    if not callable(injector):
        logger.warning("%s ctx has no inject_context; follow-up disabled", LOG_PREFIX)
        return False
    snippet = _render_context_snippet(
        item["body"], item["text"], int(fu.get("maxRows", 20) or 20)
    )
    injector(snippet, priority=50, source="qdm-query-card")
    return True


# ---------------------------------------------------------------------------
# 直连查数（queryMode=direct）：插件自己调 CLI，零 LLM、零召回
# ---------------------------------------------------------------------------

_direct_cli_cache: str = ""


def _cli_binary_name() -> str:
    """CLI 可执行文件名：Windows 带 ``.exe``，其他平台无后缀。"""
    return "qdm-metric-cli.exe" if os.name == "nt" else "qdm-metric-cli"


def _runtime_arch_dir() -> str:
    """qdm instance 的 runtimes 平台目录名（形如 ``runtimes/<os>-<arch>``）。

    Windows 上宿主只下发 ``windows-amd64``；Linux/macOS 按实际架构推，
    推不出来就退化成 ``linux-amd64``（x86_64 仍是最常见的部署形态）。
    """
    if os.name == "nt":
        return "windows-amd64"
    machine = (platform.machine() or "").lower()
    if sys.platform == "darwin":
        return "darwin-arm64" if machine in ("arm64", "aarch64") else "darwin-amd64"
    return "linux-arm64" if machine in ("aarch64", "arm64") else "linux-amd64"


def _cli_search_roots() -> list[str]:
    """CLI 的自动搜索根目录（平台相关）。

    Windows 的快照装在 ``E:\\harness\\*\\bin``；Linux/macOS 没有这个约定，
    改在 ``$HOME/.qdm`` 下找。想完全自定义就用 ``cli.path`` 配置或
    ``QDM_METRIC_CLI`` 环境变量（优先级都高于这里的自动探测）。
    """
    if os.name == "nt":
        roots = [r"E:\harness\*\bin"]
    else:
        # 用 "/" 拼接：glob 在两个平台都认，且不受 pathlib 的平台类绑定影响
        home = os.path.expanduser("~")
        roots = [
            f"{home}/.qdm/harness-data/*/bin",
            f"{home}/.qdm/*/bin",
            f"{home}/harness/*/bin",
        ]
    extra = os.environ.get("QDM_METRIC_CLI_SEARCH", "").strip()
    if extra:
        roots.insert(0, extra)
    return roots


def _find_instance_cli(cfg: dict[str, Any]) -> str:
    """直连查询必须用 instance 托管的 qdm-metric-cli。

    实测（2026-09-18）：E:\\harness\\*\\bin 下的快照二进制与 auth-center
    签发的 blob 不匹配，传 ``--auth-blob`` 一律 AUTHORIZATION_FAILED(rc=77)；
    instance runtimes 里的才是 runtime MCP 给 Agent 用的那份（版本匹配）。
    """
    global _direct_cli_cache
    if _direct_cli_cache:
        return _direct_cli_cache
    explicit = str((cfg.get("direct") or {}).get("cliPath") or "").strip()
    if explicit and os.path.isfile(explicit):
        _direct_cli_cache = explicit
        return explicit
    base = str(
        Path.home()
        / ".qdm"
        / "harness-data"
        / "instance"
        / "*"
        / "runtimes"
        / _runtime_arch_dir()
        / _cli_binary_name()
    )
    try:
        cands = glob.glob(base)
    except Exception:  # noqa: BLE001
        cands = []
    if cands:
        _direct_cli_cache = max(cands, key=os.path.getmtime)
        logger.info("%s instance cli resolved: %s", LOG_PREFIX, _direct_cli_cache)
    return _direct_cli_cache


def _auth_blob(user_id: str, cfg: dict[str, Any]) -> str:
    """从 auth-center 的 runtime MCP 端点取该用户的 qdm1enc blob（同步）。

    与 Agent 执行时 runtime 注入 ``QDM_AUTH_BLOB`` 用的是**同一个端点、
    同一把 token**，不引入新的权限面。返回裸 ``qdm1enc.*`` 串
    （外层 JSON 整串传给 ``--auth-blob`` 会 rc=77，实测）。
    """
    import urllib.request

    d = cfg.get("direct") or {}
    url = str(d.get("runtimeMcpUrl") or "http://127.0.0.1:8765/mcp")
    # 候选按优先级排列，取**第一个真实存在的**：配置里写死的路径在别的
    # 平台上多半不存在（DEFAULT_CONFIG 那条就是 Windows 路径），此时自动
    # 落到环境变量上，Linux/macOS 不必改代码。
    tok_file = ""
    for cand in (
        str(d.get("runtimeTokenFile") or ""),
        os.environ.get("QDM_AUTH_RUNTIME_TOKEN_FILE", ""),
    ):
        if cand and os.path.isfile(cand):
            tok_file = cand
            break
    if not tok_file:
        raise RuntimeError(
            "runtime token 文件不存在（已试 direct.runtimeTokenFile 与 "
            "环境变量 QDM_AUTH_RUNTIME_TOKEN_FILE）"
        )
    token = Path(tok_file).read_text(encoding="utf-8").strip()
    rpc = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "qdm_auth_lookup_blob",
            "arguments": {"channel": "wecom", "user_id": user_id},
        },
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(rpc).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/event-stream",
        },
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=float(d.get("blobTimeoutSec", 8.0) or 8.0)) as resp:
        raw = resp.read().decode("utf-8", "ignore")
    data = None
    for line in raw.splitlines():  # 流式端点可能是 SSE
        if line.startswith("data:"):
            data = json.loads(line[5:].strip())
            break
    if data is None:
        data = json.loads(raw)
    result = data.get("result") or {}
    if result.get("isError"):
        raise RuntimeError("MCP 返回错误")
    content = (result.get("content") or [{}])[0]
    blob = (json.loads(content.get("text") or "{}") or {}).get("blob", "")
    if not str(blob).startswith("qdm1enc."):
        raise RuntimeError("blob 响应格式异常")
    return str(blob)


def _build_direct_args(body: dict[str, Any]) -> list[str]:
    """把 H5 提交体翻译成 ``analysis execute`` 参数。不支持就抛 ValueError。"""
    args: list[str] = ["analysis", "execute"]

    codes = [
        str(m.get("code") or "")
        for m in body.get("metrics") or []
        if isinstance(m, dict)
    ]
    codes = [c for c in codes if c]
    if not codes:
        raise ValueError("没有指标")
    for c in codes:
        args += ["--metric", c]

    for d in body.get("dims") or []:
        if isinstance(d, dict) and d.get("code"):
            args += ["--agg-dim", str(d["code"])]

    for f in body.get("filters") or []:
        if not isinstance(f, dict):
            continue
        code = str(f.get("code") or "")
        ids = [
            str(v.get("id") or "")
            for v in f.get("values") or []
            if isinstance(v, dict)
        ]
        ids = [i for i in ids if i]
        if code and ids:
            args += ["--filter", f"{code}={','.join(ids)}"]

    rng = body.get("range") or {}
    start, end = str(rng.get("start") or ""), str(rng.get("end") or "")
    grain = str(rng.get("grain") or "bizDate")
    if not start or not end:
        raise ValueError("缺少时间范围")
    args += ["--start-date", start, "--end-date", end]
    # 周/月粒度用 --time-grain（CLI 自动按 bizWeek/bizMonth 分组），
    # 避免自己做日期→YYYYWW/YYYYMM 的口径换算
    if grain == "bizWeek":
        args += ["--time-grain", "WEEK"]
    elif grain == "bizMonth":
        args += ["--time-grain", "MONTH"]
    elif grain != "bizDate":
        raise ValueError(f"暂不支持的时间粒度: {grain}")

    args += ["--statistic-policy", str(body.get("policy") or "SUMMARY")]
    cmp_ = body.get("compare") or {}
    if cmp_.get("yoy"):
        args.append("--yoy")
    if cmp_.get("mom"):
        args.append("--mom")
    args += [
        "--data-auth",
        "--output",
        "envelope",
        "--dim-labels",
        "add",
        "--format",
        "json",
    ]
    return args


def _format_direct_result(
    res: dict[str, Any], body: dict[str, Any], max_rows: int
) -> str:
    """envelope JSON → 企微 markdown 表格（列名用 meta 里的中文名）。"""
    rows = res.get("data") or []
    if not rows:
        return "查询完成：没有符合条件的数据。"

    meta = res.get("meta") or {}
    metric_names = {
        str(m.get("code")): str(m.get("name") or m.get("code"))
        for m in meta.get("metrics") or []
        if isinstance(m, dict) and m.get("code")
    }
    dim_names: dict[str, str] = {}
    dm = meta.get("dimensionMetas")
    if isinstance(dm, dict):
        for k, v in dm.items():
            dim_names[str(k)] = (
                str(v.get("name") or k) if isinstance(v, dict) else str(v)
            )
    elif isinstance(dm, list):
        for item in dm:
            if isinstance(item, dict) and item.get("code"):
                dim_names[str(item["code"])] = str(item.get("name") or item["code"])
    # 兜底：meta 缺失时用 H5 提交体里的中文名
    for m in body.get("metrics") or []:
        if isinstance(m, dict) and m.get("code"):
            metric_names.setdefault(
                str(m["code"]), str(m.get("name") or m["code"])
            )
    for dd in body.get("dims") or []:
        if isinstance(dd, dict) and dd.get("code"):
            dim_names.setdefault(str(dd["code"]), str(dd.get("name") or dd["code"]))

    keys = list(rows[0].keys())
    # ID 列已有同名展示列（manageAreaId/manageAreaName）时丢弃 ID 列
    drop = {
        k
        for k in keys
        if k.endswith("Id") and f"{k[:-2]}Name" in keys
    }
    keys = [k for k in keys if k not in drop]

    # 列序：按用户在面板里的选择顺序（指标 → 该指标的同比/环比 → 维度 → 其它），
    # CLI 返回的列序不稳定（实测双指标时与选择顺序相反）
    picked = [
        str(x.get("code"))
        for x in body.get("metrics") or []
        if isinstance(x, dict) and x.get("code")
    ]
    ordered: list[str] = []
    for code in picked:
        if code in keys:
            ordered.append(code)
        # 衍生列：<code>同比增长率 / <code>环比增长率
        ordered += [k for k in keys if k.startswith(code) and k != code]
    ordered += [k for k in keys if k not in ordered]

    def header(k: str) -> str:
        if k in metric_names:
            return metric_names[k]
        # 衍生列：把英文指标前缀替换成中文名（af19SaleWeight同比增长率 → 19点后销售重量同比增长率）
        for code, name in metric_names.items():
            if k.startswith(code) and len(k) > len(code):
                return name + k[len(code) :]
        if k.endswith("Name"):
            base = k[:-4]  # manageAreaName -> manageArea
            for cand in (base, f"{base}Id", f"{base}_id"):
                if cand in dim_names:
                    return dim_names[cand]
        if k in dim_names:
            return dim_names[k]
        return k

    keys = ordered

    shown = rows[: max(1, max_rows)]
    lines = [
        "| " + " | ".join(header(k) for k in keys) + " |",
        "|" + " --- |" * len(keys),
    ]
    for r in shown:
        lines.append("| " + " | ".join(str(r.get(k, "")) for k in keys) + " |")
    note = f"共 {len(rows)} 行"
    if len(rows) > len(shown):
        note += f"，仅展示前 {len(shown)} 行"
    return "\n".join(lines) + f"\n\n（{note}）"


async def direct_query(
    payload: dict[str, Any], body: dict[str, Any]
) -> tuple[bool, str, str]:
    """queryMode=direct：插件直接调 CLI 查数（含数据权限），零 LLM。

    返回 ``(ok, text, reason)``。任何失败都返回 ``ok=False``，
    由调用方回退 agent 注入路径，用户无感。
    """
    cfg = load_config()
    d = cfg.get("direct") or {}
    user = str(payload.get("u") or "")
    cli = _find_instance_cli(cfg)
    if not cli:
        return False, "", "instance CLI 未找到"
    if not user:
        return False, "", "缺少用户标识"
    try:
        args = _build_direct_args(body)
    except ValueError as exc:
        return False, "", str(exc)

    t0 = time.perf_counter()
    try:
        blob = await asyncio.to_thread(_auth_blob, user, cfg)
        timeout = float(d.get("timeoutSec", 90.0) or 90.0)
        env = dict(os.environ)
        env.pop("NODE_OPTIONS", None)  # 规避安全删除 shim 对子进程的影响
        async with _sem():  # 复用全局 CLI 并发闸门
            proc = await asyncio.create_subprocess_exec(
                cli,
                *args,
                "--auth-blob",
                blob,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                env=env,
            )
            try:
                out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            except asyncio.TimeoutError:
                proc.kill()
                return False, "", f"CLI 超时（{timeout:.0f}s）"
    except Exception as exc:  # noqa: BLE001
        logger.warning("%s direct query failed: %s", LOG_PREFIX, exc)
        return False, "", f"{type(exc).__name__}: {exc}"

    dt = (time.perf_counter() - t0) * 1000
    if proc.returncode == 77:
        return False, "", "权限校验失败(rc=77)"
    if proc.returncode != 0:
        msg = (err or b"").decode("utf-8", "ignore").strip()[:200]
        return False, "", msg or f"CLI 退出码 {proc.returncode}"
    try:
        res = json.loads(out.decode("utf-8", "ignore"))
    except ValueError:
        return False, "", "CLI 返回非 JSON"
    table = _format_direct_result(res, body, int(d.get("maxRows", 20) or 20))
    n_rows = len(res.get("data") or [])
    logger.info(
        "%s direct query ok user=%s metrics=%d rows=%d elapsed=%.0fms",
        LOG_PREFIX,
        user,
        len(body.get("metrics") or []),
        n_rows,
        dt,
    )
    return True, table, f"{dt:.0f}ms"


# ---------------------------------------------------------------------------
# 维度值实时搜索（dim values）+ auth-center 权限展示 + 提交限流
# ---------------------------------------------------------------------------

_cli_path_cache: str = ""
_conditions_cache: dict[str, Any] = {}
_conditions_mtime: float = -1.0
_dimvalues_sem: Any = None
_dimvalues_cache: dict[str, tuple[float, Any]] = {}
_scope_cache: dict[str, tuple[float, Any]] = {}
_pending_jobs: dict[str, float] = {}  # session_id -> 冷却截止时间戳


def _conditions() -> dict[str, Any]:
    """读 conditions.json（按 mtime 缓存）。失败返回空 dict，接口会优雅降级。"""
    global _conditions_cache, _conditions_mtime
    path = Path(__file__).resolve().parent / "conditions.json"
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return _conditions_cache or {}
    if mtime == _conditions_mtime and _conditions_cache:
        return _conditions_cache
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("%s conditions.json load failed: %s", LOG_PREFIX, exc)
        return _conditions_cache or {}
    _conditions_cache, _conditions_mtime = data, mtime
    return data


def _dim_names() -> dict[str, dict[str, Any]]:
    return {d.get("code"): d for d in (_conditions().get("dimensions") or [])}


def find_cli(cfg: dict[str, Any] | None = None) -> str:
    """定位 qdm-metric-cli.exe。

    顺序：配置 cli.path → 环境变量 QDM_METRIC_CLI → E:\\harness\\*\\bin 下
    mtime 最新的一个（本机多套快照都装在各自 bin/ 里）。结果缓存。
    """
    global _cli_path_cache
    if _cli_path_cache:
        return _cli_path_cache
    cfg = cfg or load_config()
    candidates: list[str] = []
    explicit = str((cfg.get("cli") or {}).get("path") or "").strip()
    if explicit:
        candidates.append(explicit)
    env = os.environ.get("QDM_METRIC_CLI", "").strip()
    if env:
        candidates.append(env)
    for root in _cli_search_roots():
        try:
            snaps = glob.glob(str(Path(root) / _cli_binary_name()))
        except Exception:  # noqa: BLE001
            continue
        candidates.extend(sorted(snaps, key=os.path.getmtime, reverse=True))
    for c in candidates:
        if c and os.path.isfile(c):
            _cli_path_cache = c
            logger.info("%s cli resolved: %s", LOG_PREFIX, c)
            return c
    logger.warning("%s qdm-metric-cli.exe not found", LOG_PREFIX)
    return ""


def _sem() -> Any:
    global _dimvalues_sem
    if _dimvalues_sem is None:
        n = int((load_config().get("limits") or {}).get("dimValuesConcurrency", 4))
        _dimvalues_sem = asyncio.Semaphore(max(1, n))
    return _dimvalues_sem


async def dim_values(
    code: str, keyword: str, limit: int
) -> tuple[bool, Any, str]:
    """调 CLI 的 ``dim values`` 拿维度值（传 ID）。

    全局信号量闸门 + TTL 缓存。CLI 是公开命令，不需要鉴权参数
    （``authorization_gate_test.go:79`` 把它标为 commandPublic）。
    """
    cfg = load_config()
    cli = find_cli(cfg)
    if not cli:
        return False, None, "qdm-metric-cli 不可用"
    limits = cfg.get("limits") or {}
    limit_max = int(limits.get("dimValuesLimitMax", 200))
    limit = max(1, min(int(limit or 20), limit_max))
    keyword = keyword.strip()[:128]
    cache_sec = float(limits.get("dimValuesCacheSec", 60) or 0)

    key = f"{code}|{keyword}|{limit}"
    if cache_sec > 0:
        hit = _dimvalues_cache.get(key)
        if hit and hit[0] > time.time():
            return True, hit[1], "cache"

    timeout = float((cfg.get("cli") or {}).get("timeoutSec", 20.0) or 20.0)
    args = [cli, "dim", "values", "--code", code, "--limit", str(limit),
            "--format", "json", "--output", "data"]
    if keyword:
        args += ["--keyword", keyword]
    try:
        async with _sem():
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            try:
                out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            except asyncio.TimeoutError:
                proc.kill()
                return False, None, f"CLI 超时（{timeout:.0f}s）"
    except Exception as exc:  # noqa: BLE001
        return False, None, f"CLI 启动失败: {type(exc).__name__}"
    if proc.returncode != 0:
        msg = (err or b"").decode("utf-8", "ignore").strip()[:200]
        return False, None, msg or f"CLI 退出码 {proc.returncode}"
    try:
        items = json.loads(out.decode("utf-8", "ignore"))
        rows = [
            {"id": str(x.get("dimFieldId") or ""), "name": str(x.get("dimFieldValue") or "")}
            for x in items if isinstance(x, dict)
        ]
    except Exception as exc:  # noqa: BLE001
        return False, None, f"CLI 输出解析失败: {exc}"
    if cache_sec > 0:
        _dimvalues_cache[key] = (time.time() + cache_sec, rows)
        if len(_dimvalues_cache) > 512:  # 防膨胀
            now = time.time()
            for k in [k for k, v in _dimvalues_cache.items() if v[0] < now]:
                _dimvalues_cache.pop(k, None)
    return True, rows, "ok"


def _scope_sync(
    base: str, system_code: str, login_id: str, timeout: float, api_key: str
) -> Any:
    """同步拉 auth-center 的 scope（跑在线程里，别堵 event loop）。

    auth-center 的 /v1/* 是数据面接口，要求 Bearer/X-API-Key
    （配置里 ``api-keys`` 列表）。不带 key 一律 401。
    """
    import urllib.request

    url = (base.rstrip("/") + "/v1/iam/scope?loginid="
           + urllib.parse.quote(login_id) + "&systemCode=" + urllib.parse.quote(system_code))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    headers = {"Accept": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    req = urllib.request.Request(url, headers=headers)
    with opener.open(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "ignore"))


async def auth_scope(login_id: str) -> dict[str, Any]:
    """取用户的数据权限范围，给 H5 做只读展示。

    只是**展示**：真正的权限求交在查询时由 CLI ``--data-auth`` 做
    （服务端注入 sapArea2Id/dcSapArea2Id/categoryLevel1Id，只收窄不放大），
    这里拿不到也绝不影响查询。auth-center 不在线时优雅降级。
    """
    if not login_id:
        return {"available": False, "reason": "未知用户", "loginId": "", "userName": ""}
    cfg = load_config()
    ac = cfg.get("authCenter") or {}
    if not ac.get("enabled", True):
        return {"available": False, "reason": "未启用", "loginId": login_id, "userName": ""}
    cache_sec = float(ac.get("cacheSec", 300) or 0)
    if cache_sec > 0:
        hit = _scope_cache.get(login_id)
        if hit and hit[0] > time.time():
            return hit[1]
    base = str(ac.get("baseUrl") or "http://127.0.0.1:4008")
    system_code = str(ac.get("systemCode") or "BI")
    timeout = float(ac.get("timeoutSec", 4.0) or 4.0)
    api_key = str(
        ac.get("apiKey") or os.environ.get("QDM_AUTH_API_KEY") or ""
    )
    try:
        raw = await asyncio.to_thread(
            _scope_sync, base, system_code, login_id, timeout, api_key
        )
    except Exception as exc:  # noqa: BLE001
        # 区分 HTTP 401/403（key 不对）和其它网络错误，否则排查全是同一句话
        status = getattr(exc, "code", None) or getattr(exc, "status", None)
        if status in (401, 403):
            reason = f"权限服务拒绝调用（HTTP {status}，检查 authCenter.apiKey）"
        else:
            reason = f"权限服务不可用（{type(exc).__name__}{': ' + str(status) if status else ''}）"
        logger.warning("%s auth scope failed login=%s %s", LOG_PREFIX, login_id, reason)
        result = {
            "available": False,
            "reason": reason,
            "loginId": login_id,
            "userName": "",
        }
        if cache_sec > 0:
            _scope_cache[login_id] = (time.time() + min(cache_sec, 60), result)
        return result

    claims = ((raw or {}).get("claims") or {}).get("qdm.scope") or {}
    user = (raw or {}).get("user") or {}
    found = bool((raw or {}).get("found"))
    raw_user_name = str(user.get("userName") or "")
    names = _dim_names()
    dims = []
    for code, key in (
        ("sapArea2Id", "sapArea2Ids"),
        ("dcSapArea2Id", "dcSapArea2Ids"),
        ("categoryLevel1Id", "categoryLevel1Ids"),
    ):
        vals = claims.get(key) or []
        if vals:
            dims.append({
                "code": code,
                "name": (names.get(code) or {}).get("name") or code,
                "values": [str(v) for v in vals],
            })
    reason = "" if found else f"权限系统未找到账号 {login_id}"
    if found and not dims:
        reason = "账号已找到，但没有已配置的维度授权项"
        logger.info("%s auth scope empty dims login=%s", LOG_PREFIX, login_id)
    result = {
        "available": True,
        "found": found,
        "loginId": login_id,
        "userName": raw_user_name,
        "dims": dims,
        "reason": reason,
    }
    if cache_sec > 0:
        _scope_cache[login_id] = (time.time() + cache_sec, result)
    return result


class SubmitBusy(Exception):
    """429：提交过快 / 排队已满。message 是给用户看的文案。"""

    def __init__(self, message: str, retry_after: int = 0) -> None:
        super().__init__(message)
        self.message = message
        # 剩余秒数，回给前端做倒计时（前端不用自己解析中文文案）
        self.retry_after = retry_after


def _validate_submission(body: dict[str, Any]) -> str:
    """提交参数预检。返回错误文案（空串=通过）。

    必须在 ``claim_submit_slot`` **之前**跑：坏参数不应占冷却槽，
    否则用户改完参数还要干等（实测踩过：空参数 500 后槽位泄漏）。
    前端同样会先拦一道（``syncSubmit``），坏参数根本不该出网。
    """
    metrics = body.get("metrics")
    if not isinstance(metrics, list) or not metrics:
        return "请至少选择一个指标"
    rng = body.get("range") or {}
    if not (str(rng.get("start") or "") and str(rng.get("end") or "")):
        return "请选择查询时间范围"
    if str(rng.get("grain") or "") not in ("bizDate", "bizWeek", "bizMonth", "bizYear"):
        return "时间粒度无效，请刷新页面重试"
    policy = str(body.get("policy") or "")
    if not re.fullmatch(r"[A-Za-z0-9_]{1,64}", policy):
        return "统计口径无效，请重新打开面板"
    return ""


def submit_slot_key(payload: dict[str, Any], session_id: str) -> str:
    """限流键：**按人**而不是按会话。

    群聊里 ``s`` 是整群的会话 id，拿它做键会让 A 的查询把 B 一起挡住
    （同群两人先后各查一次，第二个人要陪着等满冷却）。所以键里必须带上
    发起者账号。
    """
    user = str(payload.get("u") or "")
    return f"{user}|{session_id}" if user else session_id


def _sweep_slots(now: float) -> None:
    for sid in [s for s, exp in _pending_jobs.items() if exp <= now]:
        _pending_jobs.pop(sid, None)


def claim_submit_slot(session_id: str) -> None:
    """提交占坑：每人一个坑 + 全局在途上限。

    inject 模式下插件感知不到 Agent 何时查完，所以占坑时长只能启发式取
    ``limits.submitInFlightSec``（兜底上限）。真正的收口在
    ``finish_submit_slot`` —— 任务一结束就把截止时间收缩到
    ``limits.submitCooldownSec``，失败则整个释放。
    """
    limits = load_config().get("limits") or {}
    in_flight = float(limits.get("submitInFlightSec", 60) or 0)
    max_pending = int(limits.get("maxPendingSubmissions", 4) or 0)
    now = time.time()
    _sweep_slots(now)
    exp = _pending_jobs.get(session_id)
    if exp and exp > now:
        left = max(1, int(exp - now + 0.999))
        raise SubmitBusy(
            f"上一次提交还在处理中，请等结果返回后再提交（约 {left} 秒后可再次提交）。",
            retry_after=left,
        )
    if max_pending > 0 and len(_pending_jobs) >= max_pending:
        raise SubmitBusy("当前查询排队较多，请稍等片刻再提交。", retry_after=5)
    if in_flight > 0:
        _pending_jobs[session_id] = now + in_flight


def finish_submit_slot(session_id: str, ok: bool = True) -> None:
    """任务结束时的收口。

    ``ok=True`` → 把坑收缩到 ``now + submitCooldownSec``（默认 15s，只防手抖）；
    ``ok=False`` → 整个释放，用户改完参数立刻能原 token 重试 —— 报错不该罚等待。
    """
    if not session_id:
        return
    if not ok:
        _pending_jobs.pop(session_id, None)
        return
    cooldown = float((load_config().get("limits") or {}).get("submitCooldownSec", 15) or 0)
    if cooldown <= 0:
        _pending_jobs.pop(session_id, None)
        return
    now = time.time()
    new_exp = now + cooldown
    exp = _pending_jobs.get(session_id)
    _pending_jobs[session_id] = new_exp if (exp is None or new_exp < exp) else exp


def release_submit_slot(session_id: str) -> None:
    """异常兜底：直接把坑抹掉（等价于 ok=False 的收口）。"""
    if session_id:
        _pending_jobs.pop(session_id, None)


# ---------------------------------------------------------------------------
# HTTP 接口（挂在 /api/qdm-query-card 下）
# ---------------------------------------------------------------------------


def build_router() -> Any:
    """构建插件的 HTTP 路由。

    宿主 ``auth.py:720`` 的判定是 ``if not is_auth_enabled() or not
    has_registered_users(): return True`` —— 两者必须同时成立才鉴权。
    本机实测 ``/api/auth/status`` 为 ``{"enabled": false, "has_users": false}``，
    因此这些接口当前**天然匿名可达**。一旦将来启用认证，需要把反向代理
    IP 加进 ``security.allow_no_auth_hosts``（纯配置，不改代码）。
    """
    if not _HAS_FASTAPI:
        logger.warning("%s fastapi unavailable; http routes disabled", LOG_PREFIX)
        return None

    router = APIRouter()

    @router.get("/ping")
    async def ping():
        return {"ok": True, "plugin": "qdm-query-card", "ts": int(time.time())}

    @router.get("/bootstrap")
    async def bootstrap(t: str = "", o: str = "", w: str = ""):
        """H5 启动握手。

        ``o`` 是 H5 存在 localStorage 里的浏览器句柄（群聊认领用）；
        ``w`` 是 strict 模式下打开者自报的企微账号。
        """
        ok, payload, why = verify_token(t, consume=False)
        if not ok:
            return JSONResponse({"ok": False, "error": why}, status_code=401)
        cfg = load_config()
        scope = await auth_scope(str(payload.get("u") or ""))
        owner_name = str(scope.get("userName") or "") or str(payload.get("u") or "")
        allowed, gcode, msg = check_panel_guard(
            payload, cfg, opener=o, who=w, owner_name=owner_name
        )
        guard_info = {
            "applies": guard_applies(payload, cfg),
            "mode": guard_mode(cfg),
            "owner": {
                "loginId": str(payload.get("u") or ""),
                "name": owner_name,
            },
        }
        if not allowed:
            logger.info(
                "%s panel blocked code=%s login=%s opener=%s",
                LOG_PREFIX, gcode, payload.get("u") or "", (o or "")[:8],
            )
            return JSONResponse(
                {"ok": False, "code": gcode, "error": msg, "guard": guard_info},
                status_code=409,
            )
        return {
            "ok": True,
            "scope": "group" if int(payload.get("g") or 0) else "single",
            "expires_at": int(payload.get("e") or 0),
            "consumed": nonce_used(payload),
            "auth": scope,
            "guard": guard_info,
        }

    @router.get("/redeem")
    async def redeem(task: str = "", d: str = ""):
        """一步跳转模式：页面打开后拿 task_id 换 token。

        ``task`` 只有 task_id、没有身份，所以能不能换出 token 完全取决于
        「最近一次点按钮的人是不是面板归属人」。回调通常比页面加载快，
        但为稳妥，前端拿不到就按 ``pending`` 再轮询几次。
        """
        cfg = load_config()
        tid = str(task or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_\-@]{1,128}", tid):
            return JSONResponse(
                {"ok": False, "code": "bad_task", "error": "参数不合法"}, status_code=400
            )
        ok_r, token, code, msg = redeem_panel_token(tid, str(d or ""), cfg)
        logger.info("%s redeem code=%s task=%s", LOG_PREFIX, code, tid[:40])
        if ok_r:
            return {"ok": True, "token": token}
        # pending 用 202：不是错误，是"再等等"
        status = 409 if code in ("not_owner", "expired", "too_many") else 202
        return JSONResponse({"ok": False, "code": code, "error": msg}, status_code=status)

    @router.get("/dim-values")
    async def dim_values_api(
        t: str = "", code: str = "", keyword: str = "", limit: int = 20
    ):
        """维度值实时搜索（门店/城市/大分类等无本地枚举的维度）。

        必须携带有效 token（不核销），维度 code 必须在 conditions.json 里，
        防止拿这个口子当任意 CLI 网关。
        """
        ok, _payload, why = verify_token(t, consume=False)
        if not ok:
            return JSONResponse({"ok": False, "error": why}, status_code=401)
        code = (code or "").strip()
        if not re.fullmatch(r"[A-Za-z0-9_]{1,64}", code):
            return JSONResponse({"ok": False, "error": "invalid code"}, status_code=400)
        if code not in _dim_names():
            return JSONResponse(
                {"ok": False, "error": f"unknown dimension: {code}"}, status_code=400
            )
        ok2, rows, reason = await dim_values(code, keyword or "", limit)
        if not ok2:
            return JSONResponse({"ok": False, "error": reason}, status_code=502)
        return {"ok": True, "code": code, "items": rows}

    @router.post("/submit")
    async def submit(request: Request):
        started = time.perf_counter()
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            return JSONResponse({"ok": False, "error": "invalid json"}, status_code=400)
        if not isinstance(body, dict):
            return JSONResponse({"ok": False, "error": "bad body"}, status_code=400)

        # 校验不核销 → 过限流闸门 → 真正核销放行。
        # 顺序不能反：被 429 拦下来的用户必须还能拿同一个 token 重试；
        # 而"已用过"的 token 必须在注入**之前**拦下（409），否则同一查询
        # 会被注入两次、Agent 跑两轮。
        ok, payload, why = verify_token(str(body.get("t") or ""), consume=False)
        if not ok:
            logger.warning("%s submit rejected: %s", LOG_PREFIX, why)
            return JSONResponse({"ok": False, "error": why}, status_code=401)

        # 群聊面板防护：必须在限流/核销之前拦，别让非本人白白占掉 cool down
        cfg0 = load_config()
        owner_name = ""
        if guard_applies(payload, cfg0):
            # 拿到中文名，拦截文案才像人话（bootstrap 已缓存，这里几乎零成本）
            scope = await auth_scope(str(payload.get("u") or ""))
            owner_name = str(scope.get("userName") or "") or str(payload.get("u") or "")
        allowed, gcode, gmsg = check_panel_guard(
            payload,
            cfg0,
            opener=str(body.get("o") or ""),
            who=str(body.get("w") or ""),
            owner_name=owner_name,
        )
        if not allowed:
            logger.info(
                "%s submit blocked by guard code=%s login=%s",
                LOG_PREFIX, gcode, payload.get("u") or "",
            )
            return JSONResponse(
                {"ok": False, "code": gcode, "error": gmsg, "blocked": True},
                status_code=409,
            )

        # 参数预检在限流之前：坏参数直接 400，不占冷却槽（见 _validate_submission）
        bad = _validate_submission(body)
        if bad:
            logger.info("%s submit invalid params: %s", LOG_PREFIX, bad)
            return JSONResponse({"ok": False, "error": bad}, status_code=400)

        already_used = nonce_used(payload)

        session_id = str(payload.get("s") or "")
        if not session_id:
            chatid = str(payload.get("c") or "")
            is_group = bool(int(payload.get("g") or 0))
            session_id = (
                f"wecom:group:{chatid}"
                if (is_group and chatid)
                else f"wecom:{payload.get('u') or ''}"
            )
        if not already_used:
            try:
                # 限流按人（群聊里同会话不同人互不干扰，见 submit_slot_key）
                claim_submit_slot(submit_slot_key(payload, session_id))
            except SubmitBusy as busy:
                logger.info(
                    "%s submit throttled sid=%s retry_after=%ss",
                    LOG_PREFIX, session_id, busy.retry_after,
                )
                return JSONResponse(
                    {
                        "ok": False,
                        "code": "busy",
                        "error": busy.message,
                        "retryAfter": busy.retry_after,
                    },
                    status_code=429,
                )
            if not consume_nonce(payload):
                # 并发请求抢先核销 → 同上，必须拦在注入之前
                already_used = True
        if already_used:
            logger.info("%s submit replay blocked sid=%s", LOG_PREFIX, session_id)
            return JSONResponse(
                {
                    "ok": False,
                    "error": "already used",
                    "replayed": True,
                    "message": "这个链接已经提交过一次查询，请回企微会话查看结果；需要再查请重新发送触发词。",
                },
                status_code=409,
            )

        cfg = load_config()
        job_id = hashlib.sha1(
            f"{payload.get('n')}{time.time()}".encode("utf-8")
        ).hexdigest()[:12]

        try:
            return await _do_submit(
                cfg=cfg,
                job_id=job_id,
                payload=payload,
                body=body,
                session_id=session_id,
                started=started,
            )
        except Exception as exc:  # noqa: BLE001
            # 兜底：占坑之后任何未预期异常都必须回滚冷却槽与 nonce，
            # 否则用户改完参数还要干等冷却结束（实测踩过 92 秒事故）
            logger.exception("%s submit internal error job=%s", LOG_PREFIX, job_id)
            release_submit_slot(submit_slot_key(payload, session_id))
            rollback_nonce(payload)
            return JSONResponse(
                {"ok": False, "error": f"internal error: {exc}"}, status_code=500
            )

    return router


async def _do_submit(
    *,
    cfg: dict[str, Any],
    job_id: str,
    payload: dict[str, Any],
    body: dict[str, Any],
    session_id: str,
    started: float,
):
    """submit 的主体（占坑之后的部分），由 submit 的 try/except 兜底。"""
    # ---- 直连模式（queryMode=direct）：零 LLM，失败自动回退 agent ----
    if str(cfg.get("queryMode") or "agent") == "direct":
        ok, text, dwhy = await direct_query(payload, body)
        if ok:
            sent, reason = await push_text(payload, text)
            logger.info(
                "%s submit mode=direct job=%s %s reason=%s elapsed=%.0fms",
                LOG_PREFIX,
                job_id,
                "DELIVERED" if sent else "FAILED",
                reason,
                (time.perf_counter() - started) * 1000,
            )
            if sent:
                # 记下这次查询：direct 结果不进上下文，追问时靠它补回 Agent
                _remember_last_query(session_id, payload, body, text)
                # 直查已经跑完（通常 1-3 秒），坑立刻收缩到 15s，别再按 180s 罚站
                finish_submit_slot(submit_slot_key(payload, session_id), ok=True)
                return {
                    "ok": True,
                    "job_id": job_id,
                    "summary": render_summary(body),
                    "mode": "direct",
                    "message": "查询完成，结果已直接发送到企微会话。",
                }
            # 直查成功但推送失败：回滚冷却与 nonce，让用户原 token 重试
            release_submit_slot(submit_slot_key(payload, session_id))
            rollback_nonce(payload)
            return JSONResponse(
                {"ok": False, "error": f"deliver failed: {reason}"},
                status_code=502,
            )
        logger.warning(
            "%s direct query failed (%s); fallback to agent", LOG_PREFIX, dwhy
        )
        # 失败 → 继续走下方 agent 注入路径

    # pick_submission_text 内部跑召回预检子进程（~0.6-2s），放线程池，
    # 避免阻塞宿主事件循环（WS 心跳/其它会话的消息处理）
    text, tier = await asyncio.to_thread(pick_submission_text, payload, body)
    logger.info(
        "%s submission tier=%s user=%s metrics=%d",
        LOG_PREFIX,
        tier,
        payload.get("u") or "",
        len(_han_only(body.get("metrics"))),
    )
    summary = render_summary(body)
    mode = str(cfg.get("result_mode") or "inject")

    if mode == "push":
        sent, reason = await push_text(payload, text)
    else:
        sent, reason = inject_into_session(payload, text)

    logger.info(
        "%s submit mode=%s job=%s %s reason=%s elapsed=%.0fms",
        LOG_PREFIX,
        mode,
        job_id,
        "DELIVERED" if sent else "FAILED",
        reason,
        (time.perf_counter() - started) * 1000,
    )
    if not sent:
        # 回滚冷却窗口和 nonce，让用户改完立刻能原 token 重试
        release_submit_slot(submit_slot_key(payload, session_id))
        rollback_nonce(payload)
        return JSONResponse(
            {"ok": False, "error": f"deliver failed: {reason}"}, status_code=502
        )
    # 注入/推送已发出：Agent 侧还要跑多久插件无从得知，但插件自己的活干完了，
    # 坑收缩到 submitCooldownSec（默认 15s）即可，不再猜一个 180s
    finish_submit_slot(submit_slot_key(payload, session_id), ok=True)
    return {
        "ok": True,
        "job_id": job_id,
        "summary": summary,
        "message": "查询已提交，正在处理，结果将出现在对应的企微会话里。",
    }


# ---------------------------------------------------------------------------
# 入口：模块必须导出名为 plugin 的对象（``plugins/loader.py:531``）
# ---------------------------------------------------------------------------


class QdmQueryCardPlugin:
    def register(self, api: PluginApi) -> None:
        load_config()
        api.register_runtime_hook(QueryCardTriggerHook())

        router = build_router()
        if router is not None:
            try:
                api.register_http_router(router, prefix="/qdm-query-card")
                logger.info("%s http routes mounted at /api/qdm-query-card", LOG_PREFIX)
            except Exception as exc:  # noqa: BLE001
                logger.exception("%s register_http_router failed: %s", LOG_PREFIX, exc)

        logger.info("%s plugin registered (P1-a: card + h5 + submit)", LOG_PREFIX)


plugin = QdmQueryCardPlugin()
