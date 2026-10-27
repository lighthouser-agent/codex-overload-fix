# CCHost Codex过载重试

在本机识别CCHost返回的流内过载错误，退避重试，减少任务中断。
上游始终是`https://cchost.ai/v1`，沿用每个人已有的认证。

本仓库是[NewBeeM4n/codex-overload-fix](https://github.com/NewBeeM4n/codex-overload-fix)的CCHost适配版，保留MIT许可证。

## 安装

需要Python3.11或更新版本，以及已经能使用的CCHost配置。
macOS和Linux支持用户级自启动。

```bash
git clone https://github.com/lighthouser-agent/codex-overload-fix.git
cd codex-overload-fix
python3 scripts/cchost.py install --dry-run
python3 scripts/cchost.py install
python3 scripts/cchost.py status
```

安装后重启使用该配置的Codex客户端。
配置优先按`CODEX_HOME`定位，否则从`.codex-cli`和`.codex`识别唯一启用CCHost的配置。
有多份配置时，明确指定：

```bash
python3 scripts/cchost.py install --config /absolute/path/config.toml
```

安装器保留`model_provider = "cchost"`、模型、认证命令、MCP和其他设置。
它只管理已有CCHost表中的四个字段：

| 字段 | 安装后的值 |
|---|---|
| `base_url` | `http://127.0.0.1:8787` |
| `supports_websockets` | `false` |
| `stream_max_retries`和`request_max_retries` | `0` |

中继使用HTTP/SSE，客户端重试交给中继统一处理。
默认最多尝试10次，重试窗口最多300秒，输出开始后停止中继重放。

## 验证和回退

```bash
python3 scripts/cchost.py status
python3 scripts/cchost.py uninstall
```

卸载恢复安装前的四个字段，保留后来修改的其他配置和原始备份。
如果有人手动改了受管字段，卸载会保留现场并报错。
恢复配置后重启Codex客户端。

具体代理设置、Windows手动运行、日志位置和验收范围见[团队使用说明](docs/CCHOST.md)。
仓库原有的`install-mac.sh`、`install-linux.sh`和`install.ps1`保留作上游参考，团队接入统一使用`cchost.py`。

## 重试边界

只在输出尚未开始且明确收到过载错误时重试SSE请求。
只缓冲初始化事件和明确为空的assistant消息。
文本、推理、工具事件和未知事件开始后，后续流原样转发。
HTTP500、502、503、504可重试；HTTP429只在明确标记过载时重试。
连接建立失败可以重试；请求发送后的模糊网络失败直接返回。
额度、余额和认证错误直接返回。
重试耗尽时保留上游状态、响应头和错误体，遵守`Retry-After`。

取消请求会中断已建立的上游连接，并停止后续重试。
日志记录请求字节数、状态、重试次数，以及放行和结束时的事件类型、字段名和错误码。
请求体和认证头原样转发，响应正文和错误消息不写入日志。

中继不能增加CCHost或模型服务端容量。
上游仍持续拒绝时，请求会等待重试后失败。
Codex客户端和上游服务自行执行的重试不由中继保证。

## 测试

```bash
python3 -m unittest discover -s tests -t . -v
```

测试使用本地模拟上游，覆盖流内过载、分块SSE、工具输出和取消。
测试也核对错误透传和配置恢复。
真实服务的容量改善需结合使用情况观察。
