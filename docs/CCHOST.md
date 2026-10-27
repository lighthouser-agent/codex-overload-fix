# 团队接入CCHost重试

已有CCHost配置能正常认证时，用`scripts/cchost.py`安装。
每个人使用自己的认证配置，仓库不保存凭据。

## 接入已有配置

需要Python3.11或更新版本。
macOS和Linux安装命令相同：

```bash
git clone https://github.com/lighthouser-agent/codex-overload-fix.git
cd codex-overload-fix
python3 scripts/cchost.py install --dry-run
python3 scripts/cchost.py install
python3 scripts/cchost.py status
```

配置优先按`CODEX_HOME`定位，否则从`.codex-cli`和`.codex`识别唯一启用CCHost的配置。
安装器发现多份CCHost配置时，要求用`--config`指定目标。
它验证中继健康后才修改配置，并保留原文件备份。
重复安装不会覆盖第一次保存的恢复信息。

```bash
python3 scripts/cchost.py install --config /absolute/path/config.toml --port 8787
```

CCHost表里的`auth`、`env_key`和其他认证设置保持原值。
安装器也保留`model_provider = "cchost"`、模型、MCP和其他设置。
它只管理已有CCHost表中的四个字段：

| 字段 | 安装后的值 |
|---|---|
| `base_url` | `http://127.0.0.1:8787` |
| `supports_websockets` | `false` |
| `stream_max_retries`和`request_max_retries` | `0` |

中继使用HTTP/SSE，客户端重试交给中继统一处理。
安装后仍使用`cchost`这个provider，原有会话沿用该provider。
已经启动的客户端需要重启，才能读取新设置。

## 使用网络代理

后台服务不依赖终端里的代理环境变量。
需要HTTP代理时明确传入地址：

```bash
python3 scripts/cchost.py install --proxy http://127.0.0.1:7890
```

中继支持HTTP CONNECT代理，代理地址不能包含凭据。
代理出口仍须能访问`cchost.ai`。

## Windows或手动运行

Windows自启动尚未适配和实机验证。
可以在一个终端手动运行中继，保持该终端开启：

```powershell
python relay/relay.py --upstream https://cchost.ai/v1 --port 8787 --max-attempts 10 --commit-deadline 300 --forward-late-overload
```

手动运行必须传`--upstream`。不传时中继沿用上游项目的ChatGPT默认地址。
其余参数不传时也沿用上游默认值：最多尝试30次，未提交的流等待120秒。

另一个终端接入配置：

```powershell
python scripts/cchost.py install --manual --config C:/path/to/config.toml
python scripts/cchost.py status --config C:/path/to/config.toml
```

手动模式仅管理配置。中继未启动时，安装器不会把客户端指向该端口。
在手动模式和自动服务之间切换时重新安装。卸载按最近一次安装的模式决定是否停止服务。
卸载先恢复配置，再关闭运行中继的终端。

## 查看运行状态

自动服务最多尝试10次，包含首次请求。
每次请求的重试总预算为300秒。
出现可观察输出后直接转发，不再重放该请求。

```bash
python3 scripts/cchost.py status
```

状态检查同时核对受管配置和本地健康接口。
健康接口成功只能证明本地服务已运行，上游认证仍需实际请求验证。

| 系统 | 服务与日志 |
|---|---|
| macOS | LaunchAgent为`com.cchost.codex-retry`，日志在`~/Library/Application Support/cchost-codex-retry/` |
| Linux | systemd用户服务为`cchost-codex-retry.service`，日志在`$XDG_STATE_HOME/cchost-codex-retry/`，未设置时使用`~/.local/state/` |

中继日志写入`relay.log`，按8MB轮换。
日志沿用上游格式，`tools/analyze_log.py`可以直接汇总。
请求行记录字节数、模型、推理强度和各部分大小，不记录内容。
每次尝试的前12个事件记录类型和实际服务的模型`served=`，不记录内容。
放行和结束时记录事件类型、字段名和错误码。
请求体和认证头原样转发，响应正文和错误消息不写入日志。
自动服务丢弃中继的标准输出，避免和`relay.log`重复。
`relay.log`写不进去时，日志行改写到错误输出。
macOS的错误输出在`service.log`，Linux的错误输出进入systemd日志。

日志中的`retryable`表示中继正在重试，`giving up`表示重试预算耗尽。
`ok, streaming`后的方括号是放行时的事件形态，`stream ended`记录放行后的结束事件。
放行后出现`failure`，说明错误发生在可见输出之后，中继不会重放。
预算耗尽后，客户端会收到原来的错误。
Orca的自动继续由现有容量巡检提供，本仓库不安装该巡检。
后台服务引用当前克隆目录。移动仓库后需要重新安装服务。
Linux服务默认随用户登录启动。只用SSH登录且需要登出后继续运行时，先在目标机器配置`loginctl enable-linger 用户名`。
每台机器的自动服务只用于一份受管CCHost配置。切换配置或端口前先卸载，避免多个配置共用同一个服务后互相停用。
旧版安装器使用另一个服务名，也监听8787。使用团队入口前，先用对应旧版卸载脚本撤回旧服务。
端口已被手动中继或其他进程占用时，安装器拒绝注册服务。先关闭它，或换端口，或改用`--manual`。

安装失败会恢复之前的服务定义；首次安装失败会移除本次注册的服务。
配置写入失败也会回滚本次服务改动，原配置保留。

## 恢复直连

```bash
python3 scripts/cchost.py uninstall
```

有显式配置时，卸载也传同一份`--config`。
安装器恢复原来的上游、WebSocket设置和客户端重试设置。
它只撤回自己管理的字段，保留后来修改的模型和其他配置。
如果有人手动改了受管字段，卸载会保留现场并报错。
备份和日志留在原处，便于检查。

## 重试边界

只在输出尚未开始且明确收到过载错误时重试SSE请求。
HTTP500、502、503、504可重试；HTTP429只在明确标记过载时重试。
额度、计费和认证错误直接返回。
重试耗尽时保留上游状态、响应头和错误体，遵守`Retry-After`。
取消请求会中断已建立的上游连接，并停止后续重试。

上游中继在流开始后、正文出现前遇到过载时会断开连接，让客户端自己重试。
自动服务传`--forward-late-overload`，改为原样转发这个错误。
CCHost配置关闭了客户端重试，断开只会显示`stream disconnected`。
Orca容量巡检认不出这种失败，原样转发才能触发自动继续。
手动运行中继时也应传这个参数。

中继不能增加CCHost或模型服务端容量。
上游仍持续拒绝时，请求会等待重试后失败。
Codex客户端和上游服务自行执行的重试不由中继保证。

## 已验证范围

自动测试覆盖模拟过载、重试耗尽、错误分类、输出后不重放和取消。
配置测试覆盖认证保留、重复安装、并发修改检测和卸载恢复。
SSE的嵌套错误和顶层错误都支持识别。
HTTP错误和SSE错误共用额度、计费和认证判断。判断按完整词匹配，`load-balanced`不会被当成余额错误。
CCHost有时把SSE标为`text/plain`，或省略Content-Type。
这两种情况会根据正文开头识别SSE。
正文不是SSE时直接转发，保留原响应头。
可以继续缓冲的只有`response.created`、`response.in_progress`、`response.queued`、`response.metadata`、`ping`，以及明确为空的assistant消息。
CCHost过载时先发`response.metadata`再发错误，所以它也算初始化事件。
推理、工具、有内容或未知事件都会停止重放。
TCP或TLS建立连接失败时可以重试，已经发送请求后的模糊网络失败直接返回。
尚未提交的SSE最多等待300秒；非流式成功响应使用独立的600秒读取超时。
CI在macOS、Linux和Windows上运行测试。
它也在macOS和Linux上模拟安装服务。
CI通过不等于实机安装验证，Linux和Windows尚未实机安装。

```bash
python3 -m unittest discover -s tests -t . -v
```

测试使用本地模拟上游，不访问真实模型服务。

服务上线后仍须观察真实CCHost过载请求。
成功完成一次普通请求可以验证认证和转发，不能证明容量错误已经消失。
