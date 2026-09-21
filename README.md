# qdm-query-card

企业微信「智能机器人」里的**手动查数卡片插件**：用户在群里或私聊发一句触发词，机器人回一张卡片，点开是 H5 面板，勾选指标 / 时间 / 维度 / 过滤后提交，结果直接推回会话。

它要解决的核心问题是 **绕开 Agent 的文档召回链路** —— 正常查数流程是「提问 → 召回文档 → 明确参数 → 调用查询」，前两步耗时且费 token。本插件让参数由用户显式勾选，插件拿到参数后直接调查数 CLI（带数据权限），**零 LLM、零召回**，实测 2–4 秒返回。

---

## 能力一览

| 能力 | 说明 |
| --- | --- |
| 触发回卡 | 命中触发词后回企微模板卡片；群聊带「@机器人」前缀也能命中 |
| H5 多卡片面板 | 505 个指标按业务相关性分成 12 组，横向滑动 / Tab / 键盘 ←→ 切换，手机与电脑同一套 |
| 条件勾选 | 时间范围 + 粒度 + 同环比 + 统计口径 + 指标多选 + 维度（随指标级联裁剪）+ 过滤，带前端校验 |
| 直连查数 | `queryMode=direct` 时插件自己调 CLI，2–4s 返回；失败自动退回 Agent 模式 |
| 数据权限 | 通过 auth-center 取权限维度并注入查数参数，服务端求交，只收窄不放大 |
| 群聊隔离 | 群里只发按钮卡、不含任何链接；只有触发者本人能把卡换成带链接的面板卡 |
| 追问增强 | 结果可回写 Agent 上下文，用户追问「为什么下降」时 Agent 能看到上次结果 |

---

## 工作流

```
用户发「手动查询数据」
        │
        ▼
插件 PRE_DISPATCH 命中触发词 ──► 签发一次性 token（HMAC）
        │
        ├── 私聊：直接发「整卡跳转」卡片（点卡片即开 H5）
        │
        └── 群聊：只发「按钮卡」（群里不出现任何 URL）
                    │
                    点按钮 ──► 企微推 template_card_event 回调（带点击者 userid）
                    │
                    ├─ 本人 → update_template_card(userids=[本人]) 换成带链接卡
                    └─ 他人 → 只给 TA 换成「这不是你的面板」
        │
        ▼
H5 勾选条件 → POST /submit
        │
        ▼
queryMode=direct → 插件调 CLI（2-4s，零 LLM）
queryMode=agent  → 参数注入 Agent，由 Agent 查数
        │
        ▼
结果推回会话
```

---

## 目录结构

```
.
├── plugin/
│   ├── plugin.py               # 插件主体（hook + HTTP 路由 + 卡片 + 直连查数）
│   ├── plugin.json             # 插件元数据（id / 版本 / 兼容宿主版本）
│   ├── trigger.example.json    # 配置模板（复制到 trigger.json 后填写）
│   ├── conditions.json         # 指标/维度元数据（由 tools/gen_conditions.py 生成）
│   ├── metricGroups.json       # 指标业务分组规则（可自行调整分组与顺序）
│   └── h5/index.html           # H5 面板（单文件，无构建）
└── tools/
    ├── mount.sh                # 把 plugin/ 挂到宿主插件目录（Windows 用 junction）
    └── gen_conditions.py       # 从 CLI registry 生成 conditions.json
```

> `plugin/trigger.json` 与 `plugin/.hmac_secret` **不在仓库里**（见 `.gitignore`）—— 前者含域名与密钥，后者是 token 签名密钥，每个部署各自生成。

---

## 前置依赖

| 依赖 | 用途 | 缺失时的表现 |
| --- | --- | --- |
| **QwenPaw 宿主** 2.1.0–2.2.0 | 插件运行时 + 企微通道 | 插件不加载 |
| **qdm-auth-center** | `4008` 权限维度 API、`8765` runtime MCP（取数据权限 blob） | H5 顶部提示权限不可用；`direct` 静默退回 `agent` |
| **qdm-metric-cli** | 直连查数与维度值搜索 | `direct` 模式失败，退回 `agent` |
| **Python 3.10+** | 插件本体 | — |

---

## 部署步骤

### 1. 挂载插件目录

```bash
./tools/mount.sh
```

Windows 上创建 junction、Linux/macOS 上创建软链，把 `plugin/` 挂到 `~/.qwenpaw/plugins/qdm-query-card`。

> 为什么挂链接而不是复制：宿主 loader 从外部路径安装时会 `rmtree + copytree`，等于每改一行都要重装一次。挂链接后改动即时生效。

### 2. 生成运行配置

```bash
cp plugin/trigger.example.json plugin/trigger.json
```

然后至少修改这两项：

- `publicBaseUrl` —— H5 的对外访问地址（**宿主要用 `--host 0.0.0.0` 启动才会监听非 loopback**），例如 `http://10.0.191.16:8088`
- `authCenter.apiKey` —— 建议**留空**，改用环境变量 `QDM_AUTH_API_KEY`（避免密钥进配置文件）

### 3. 启动依赖服务

```bash
# qdm-auth-center（同时监听 4008 与 8765）
# Windows: E:\harness\deploy\qdm-auth-center\start.bat
```

> ⚠️ 它是**单点依赖且不常驻**：进程没了两个端口全挂，插件会静默退回 `agent` 模式（日志关键字 `fallback to agent`），权限维度也取不到。部署到服务器时建议用 systemd / supervisor 托管。

### 4. 重启宿主

```bash
qwenpaw serve --host 0.0.0.0 --port 8088
```

**`plugin.py` 的任何改动都必须重启宿主**（没有插件 reload 路由）；`trigger.json` / `metricGroups.json` / `h5/index.html` 是热加载的，改完刷新即可。

### 5. 验证

群聊或私聊发一句「手动查询数据」，应当收到卡片；点开勾选条件提交，2–4 秒后收到结果表格。

---

## 配置项详解

配置在 `plugin/trigger.json`（热加载，改完即生效）。完整字段见 `plugin/trigger.example.json`，这里只说明需要留意的：

### `queryMode`

| 值 | 行为 |
| --- | --- |
| `direct`（推荐） | 插件直接调查数 CLI，2–4s、零 LLM；失败自动退回 `agent` |
| `agent` | 参数注入会话，由 Agent（召回 + CLI）查数并叙述 |

### `direct` / `cli`

```jsonc
{
  "direct": {
    "runtimeMcpUrl": "http://127.0.0.1:8765/mcp",
    "runtimeTokenFile": "",   // 留空则用环境变量 QDM_AUTH_RUNTIME_TOKEN_FILE
    "cliPath": ""             // 留空则自动探测；生产环境建议显式指定
  },
  "cli": { "path": "" }       // 维度值搜索用的 CLI，留空则自动探测
}
```

CLI 自动探测顺序：配置 `cliPath` → 环境变量 `QDM_METRIC_CLI` → 平台默认搜索根。

### `groupGuard`（群聊隔离）

| 字段 | 说明 |
| --- | --- |
| `buttonDelivery` | `true` 时群聊只发按钮卡，**链接不出现在群里** |
| `buttonJump` | ⚠️ **保持 `false`**。给按钮加 `type=1 + url` 能一步打开页面，但实测企微**不推回调**，服务端拿不到点击者身份，隔离会失效 |
| `instantDelivery` | ❌ **已证伪，保持 `false`**。2026-09-21 真机：企微返回 `errcode=846606 request already responded, cannot respond again` —— 一个 `req_id` 只能响应一次，发卡已经用掉了它 |
| `visibleToUser` | ❌ **已证伪，保持 `false`**。2026-09-21 真机：带该字段的回复被服务端**正常接受**（errcode=0）但**静默忽略**，群里其他人照样看得到、点得到那张卡 |
| `oneStep` | ⛔ 依赖 `visibleToUser`，后者无效则它永远发不出去，保持 `false`。安全底线仍在：`pick_group_card` 保证误配时**永不**发出带链接的卡 |
| `mode` | `claim`（首开认领）/ `strict`（输入账号比对）/ `none`。按钮交付签发的 token 自带免认领标记，同一用户的手机与电脑互不干扰 |
| `attachListener` | 卡片事件监听开关，正式链路依赖它，保持 `true` |

### `followUp`（追问增强）

direct 模式的结果不进会话上下文，所以插件会缓存最近一次查询（参数 + 结果），用户下一句不是触发词时：

- `contextInject: true` —— 把「参数 + 结果」注入本轮 Agent 输入（**不多发消息、不多跑一轮**）
- `autoTimeShift` —— 识别「上月/上周/昨天」后直接改时间重查推送，Agent 完全不跑。配了分析词排除表，「为什么这个月下降」不会误判成时间追问

### `limits`

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `submitCooldownSec` | `15` | 提交**完成后**真正保留的冷却，只防手抖连点。改 `0` = 完全不冷却 |
| `submitInFlightSec` | `60` | 占坑的兜底上限，只在任务异常中断时才会用到 |
| `maxPendingSubmissions` | `4` | 全局在途提交上限（DataQL 的并发信号量是**进程内**的，必须在插件层收口） |
| `dimValuesConcurrency` / `dimValuesCacheSec` | `4` / `60` | 维度值实时搜索的并发与缓存 |

冷却是按**人**算的（键 = `用户 + 会话`），群聊里 A 的查询不会挡住 B。三道防线，顺序固定：

1. **前端预检**：指标/时间范围没选，按钮直接禁用并给内联提示，请求根本不出网
2. **后端预检**（`_validate_submission`）：坏参数直接 400，**不占冷却槽**
3. **限流**（`claim_submit_slot`）：通过后占坑 → 任务一结束就由 `finish_submit_slot` 把坑收缩到 `submitCooldownSec`；**失败则整个释放**，改完参数立刻能重试

所以用户感知的等待 = 查询本身耗时 + 15s，不再是固定 180s。直连模式查询通常 1-3 秒。

---

## 群聊隔离：为什么是两步

企微「智能机器人」是 WS 模式，**没有 corpid / agentid，拿不到网页授权**，整卡跳转（`card_action.type=1:url`）也不带点击者身份。唯一能拿到「谁点了」的通道是**交互型卡片按钮的回调事件**（`template_card_event`，带 `body.from.userid`）。

而实测发现：**按钮一旦带 `url`，企微就不再推回调** —— 跳转是纯客户端行为。所以「点一下直接打开」与「区分是谁点的」在群聊里不可兼得。

本插件选择保住隔离，流程是两步：

1. 群里只有一张按钮卡（无任何链接）
2. 你点按钮 → 只有你看到的那张卡变成带链接的面板卡 → 点它进 H5

第二步点的是**整张卡**（`text_notice` 整卡跳转），不是再找一个小按钮。私聊不受影响，一直是点开即用。

**隔离已真机复验**（2026-09-21 14:43）：`linjiahong2` 触发的面板，群友 `zhujinxia`
点按钮 → 回调拿到 `userid=zhujinxia` ≠ owner → 判定 `kind=other`，只有他看到的那张卡
被换成「🔒 这不是你的面板」，发起人的卡片不受影响。

### 一次尝试与它的结论：`instantDelivery` ❌

上面第二步本来可以省：群里 @机器人那一刻，**入站消息帧本身就带着 `body.from.userid`**，身份不用靠按钮回调去取。所以只要在卡片发出后，立刻把**只有发起人看到的那张卡**换成带链接版本，他就点 1 次能进 H5。

协议层看着是通的 —— 读 SDK 源码可知 `update_template_card` 内部就是
`reply(frame, body, RESPONSE_UPDATE)`，而 `reply` **只取 `frame["headers"]["req_id"]，
完全不校验帧类型**，入站帧也有 `req_id`。

但 2026-09-21 真机给出了终审：

```
errcode=846606, errmsg=request already responded, cannot respond again
```

**同一个 `req_id` 只允许响应一次**，发卡已经把它用掉了，没有第二次机会。所以这条路
彻底关闭（代码保留只为留档，`instantDelivery` 恒 `false`）。

### 第二次尝试与它的结论：`visibleToUser` ❌

既然只有**一次**回复机会，差异化信息就只能塞进这唯一一次回复里 —— 也就是企微应用
消息 API 的 `visible_to_user` 字段（只有列表里的人看得到这条消息）。

按安全顺序先只开 `visibleToUser`（卡片仍是**不含链接**的按钮卡），验证群里其他人
看不看得到。2026-09-21 14:42 真机结果：

```
14:42:23  visible_to_user=[linjiahong2] one_step=False
14:42:23  card SENT reason=ok              ← 服务端正常接受，errcode=0，没走回退
14:43:33  CARD EVENT userid=zhujinxia      ← 群友看得到，而且点得到
```

**字段被静默忽略** —— 不报错、不拒绝，就是不生效。所以这条路也关闭了
（`visibleToUser` / `oneStep` 恒 `false`，代码保留只为留档）。

### 群聊「一步打开」的三道墙

| 尝试 | 结论 |
| --- | --- |
| 按钮带 `url` 一步跳转（`buttonJump`） | 企微**不推回调**，拿不到点击者身份 |
| 发卡后再差异化更新（`instantDelivery`） | `errcode=846606`，一个 `req_id` 只能响应一次 |
| 回复时指定可见人（`visibleToUser`） | 字段被**静默忽略**，群友照样看得到、点得到 |

三条路都试过了：**只要坚持「群里其他人连面板都打不开」这条隔离底线，一步就是做不到的。**

如果哪天更看重"少点一次"，唯一可换的方案是回到最初的**整卡跳转卡**（`buttonDelivery: false`）
—— 点 1 次直接进 H5，群里任何人也能点开，但**打开后会被页面拦住**（token 绑定发起人，
`mode: claim` 认领 / `strict` 账号比对）。差别是：现在别人**连页面都进不去**，那时别人
进得去但立刻被拒。链接本身不会显示在群聊里，但仍可点。要不要这么换是产品权衡，
改一个配置即可，无需改代码。

---

## 平台兼容性

| 部分 | Windows | Linux | macOS |
| --- | --- | --- | --- |
| 卡片 / H5 / token 签发 / 群聊隔离 | ✅ | ✅ | ✅ |
| 权限维度 API（HTTP） | ✅ | ✅ | ✅ |
| CLI 自动探测 | ✅ | ✅ | ✅ |
| `tools/mount.sh` | junction | 软链 | 软链 |

需要留意的平台差异（代码已处理）：

- CLI 二进制名：Windows 是 `qdm-metric-cli.exe`，其他平台无后缀
- runtimes 目录：`windows-amd64` / `linux-amd64` / `linux-arm64` / `darwin-arm64`，按 `platform.machine()` 推导
- 搜索根：Windows 扫 `E:\harness\*\bin`，其他平台扫 `$HOME/.qdm/**`，也可用 `QDM_METRIC_CLI_SEARCH` 覆盖
- subprocess 全部 `shell=False` + 参数列表；`CREATE_NO_WINDOW` 用 `getattr` 兜底（Linux 上不存在该常量）
- token 文件路径：配置里写死的路径若不存在，自动回退到环境变量 `QDM_AUTH_RUNTIME_TOKEN_FILE`

依赖侧需要对应平台的二进制：`qdm-auth-center`、`qdm-metric-cli` 都要用 Linux/macOS 版本。

---

## 排障手册

| 现象 | 原因 / 处理 |
| --- | --- |
| 日志 `fallback to agent` | auth-center 没起（4008/8765 无监听）或 CLI 找不到 → 查这两个端口 |
| H5 顶部「权限未能读取」 | 同上；若显示「未找到账号」则是 IAM 里没登记该 loginid |
| 「无法确认身份」 | `buttonJump` 被打开了。企微跳转型按钮不推回调，改回 `false` |
| 「上一次提交还在处理中，约 N 秒后可再次提交」 | 冷却期内重复提交，按钮会倒计时、到点自动恢复。嫌长就把 `limits.submitCooldownSec` 调小（热加载，改 `trigger.json` 即刻生效）；**报错不会占冷却槽**，空参数也不会（预检在限流前） |
| 群里触发词不命中、私聊正常 | 群消息带 `@机器人` 前缀，插件已做剥离；若自己改匹配逻辑要注意这点 |
| 手机能开、电脑打不开（或反之） | 认领机制把两个浏览器当成了两个人。按钮交付的 token 带免认领标记，确认走的是按钮交付 |
| 改了 `plugin.py` 没生效 | 必须重启宿主，插件没有热加载 |
| `curl 127.0.0.1:4008` 返回 502 | 环境里有 HTTP 代理，502 是代理返回的。**判端口用 socket 直连**，别信 curl |

### 关键日志关键字

```
[qqc] TRIGGERED                    触发词命中
[qqc] group button delivery        群聊按钮卡已发
[qqc] CARD EVENT                   收到卡片点击回调（带 userid）
[qqc] panel button kind=owner|other|expired
[qqc] submit mode=direct|inject   提交模式
[qqc] direct query failed         直连失败（会退回 agent）
[qqc] fallback to agent           已退回 Agent 模式
```

---

## 测试

测试脚本放在仓库外的 `E:\harness\deploy\qdm-query-card-tests\`（保持本仓库只有插件本体）：

| 脚本 | 覆盖 |
| --- | --- |
| `api_guard_test.py` | HTTP 端到端：token 签发、认领、拦截、提交（28 项） |
| `button_delivery_test.py` | 群聊按钮交付：卡里不含链接、本人拿 token、他人被拦 |
| `jump_and_crossdevice_test.py` | 免认领标记、换设备、兑换次数上限（38 项） |
| `probe_test.py` | 卡片事件监听与回调处理（35 项） |
| `platform_test.py` | 跨平台 CLI 探测与 token 兜底（14 项） |
| `cooldown_test.py` | 提交冷却：完成后收缩、失败释放、按人不按会话、预检不占槽（20 项） |
| `instant_delivery_test.py` | 一步交付：群里卡绝不含链接、只替换发起人、失败安全退回（23 项，路径已证伪，留档） |
| `visible_delivery_test.py` | 定向可见 / 一步打开：误配绝不泄密、字段被拒自动退回、send_card 透传（31 项） |
| `concurrency_test.py` | 并发压测（HTTP 层，需宿主在跑）：`--mode dry` 不查数，`--mode real` 真实并发提交 |
| `h5_smoke.js` | H5 启动流程（normal / task / claimed / who_required 四种模式） |

`h5_smoke.js` 用 node + 迷你 DOM stub 跑，不需要浏览器：

```bash
node h5_smoke.js normal
```

`concurrency_test.py` 打的是宿主 HTTP 接口，**必须绕过本机 `http_proxy`**（脚本里已
强制 `ProxyHandler({})`，否则 127.0.0.1 也会走代理拿到假 502）：

```bash
python concurrency_test.py                # dry，并发 16，不产生真实查询
python concurrency_test.py --mode real --n 8   # 真实并发查数，避开业务高峰
```

实测参考（并发 8）：`/bootstrap` p50≈931ms（每次都问一次 auth-center）、
`/dim-values` 冷≈1881ms → 热≈57ms（60s 缓存生效）、坏参数 `/submit` 全 400 且不占冷却槽。

---

## 数据安全

- token 是 HMAC 签名的，含签发者 userid、会话、过期时间，提交时校验；一次性 nonce 防重放
- 群聊按钮交付下**群里不出现任何链接**；且强制关闭了 markdown 兜底（否则卡片发送失败会把链接以 markdown 泄到群里）
- 数据权限由服务端注入求交，用户勾选项只能让范围更小
- 密钥不要进仓库：`authCenter.apiKey` 留空、走环境变量 `QDM_AUTH_API_KEY`
