# 团队接入CCHost重试

已有CCHost配置能正常认证时，用`scripts/cchost.py`安装。
每个人使用自己的认证配置，仓库不保存凭据。

## 接入已有配置

需要Python3.11或更新版本。
macOS和Linux安装命令相同：

```bash
python3 scripts/cchost.py install --dry-run
python3 scripts/cchost.py install
python3 scripts/cchost.py status
```

安装器发现多份CCHost配置时，要求用`--config`指定目标。
它验证中继健康后才修改配置，并保留原文件备份。
重复安装不会覆盖第一次保存的恢复信息。

```bash
python3 scripts/cchost.py install --config /absolute/path/config.toml --port 8787
```

CCHost表里的`auth`、`env_key`和其他认证设置保持原值。
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
python relay/relay.py --upstream https://cchost.ai/v1 --port 8787 --max-attempts 10
```

另一个终端接入配置：

```powershell
python scripts/cchost.py install --manual --config C:/path/to/config.toml
python scripts/cchost.py status --config C:/path/to/config.toml
```

手动模式仅管理配置。中继未启动时，安装器不会把客户端指向该端口。
卸载先恢复配置，再关闭运行中继的终端。

## 查看运行状态

默认最多尝试10次，包含首次请求。
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

日志中的`retryable`表示中继正在重试，`giving up`表示重试预算耗尽。
`stream committed`后的方括号是放行时的事件形态，`stream ended`记录放行后的结束事件。
放行后出现`failure`，说明错误发生在可见输出之后，中继不会重放。
预算耗尽后，客户端会收到原来的错误。
Orca的自动继续由现有容量巡检提供，本仓库不安装该巡检。
后台服务引用当前克隆目录。移动仓库后需要重新安装服务。
Linux服务默认随用户登录启动。只用SSH登录且需要登出后继续运行时，先在目标机器配置`loginctl enable-linger 用户名`。
每台机器的自动服务只用于一份受管CCHost配置。切换配置或端口前先卸载，避免多个配置共用同一个服务后互相停用。
旧版安装器使用另一个服务名，也监听8787。使用团队入口前，先用对应旧版卸载脚本撤回旧服务。

安装失败会恢复之前的服务定义；首次安装失败会移除本次注册的服务。
配置写入失败也会回滚本次服务改动，原配置保留。

## 恢复直连

```bash
python3 scripts/cchost.py uninstall
```

有显式配置时，卸载也传同一份`--config`。
安装器恢复原来的上游、WebSocket设置和客户端重试设置。
它只撤回自己管理的字段，保留后来修改的模型和其他配置。
备份和日志留在原处，便于检查。

## 已验证范围

自动测试覆盖模拟过载、重试耗尽、错误分类、输出后不重放和取消。
配置测试覆盖认证保留、重复安装、并发修改检测和卸载恢复。
SSE的嵌套错误和顶层错误都支持识别。
CCHost有时把SSE标为`text/plain`，或省略Content-Type。
这两种情况会根据正文开头识别SSE。
正文不是SSE时直接转发，保留原响应头。
可以继续缓冲的只有`response.created`、`response.in_progress`、`response.queued`、`response.metadata`、`ping`，以及明确为空的assistant消息。
CCHost过载时先发`response.metadata`再发错误，所以它也算初始化事件。
推理、工具、有内容或未知事件都会停止重放。
TCP或TLS建立连接失败时可以重试，已经发送请求后的模糊网络失败直接返回。
尚未提交的SSE最多等待300秒；非流式成功响应使用独立的600秒读取超时。
CI配置了macOS、Linux和Windows上的Python测试。
CI尚未运行的状态不能算跨平台实机通过。

服务上线后仍须观察真实CCHost过载请求。
成功完成一次普通请求可以验证认证和转发，不能证明容量错误已经消失。
