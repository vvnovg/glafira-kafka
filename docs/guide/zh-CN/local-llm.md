# 本地模型

助手不一定要在云端运行：Glafira 对所有 MCP 客户端的响应方式都相同，而客户端可以连接你网络中或你自己计算机上的模型。本章介绍模型可以在哪里运行、本地模型需要什么硬件、如何将其连接到 Glafira，以及如何检查结果。助手如何访问集群，请参见[LLM 助手（MCP）](llm-mcp.md#how-it-works)。

## 模型在哪里运行 {#where}

1. **云端模型**（Claude、GPT 等）：能力最强，但包含集群元数据的请求会离开你的网络；当级别为“[[ui:connections.form.llmLevel.messages]]”时，消息内容也会离开。许多公司禁止这样做。
2. **企业内部模型服务器**，位于你的网络内：数据留在内部网络，模型和硬件由管理员选择。如果还没有这样的服务器，这是下一步的方向。
3. **工程师计算机上的模型**：适用于禁止使用云端，且企业服务器尚不存在或仍在搭建的情况。数据不会离开这台计算机，但模型比云端模型弱，响应也更慢。

本章其余部分讲的是第三种方式；第二种方式的客户端配置方法相同，只是服务器地址不同。

## 本地模型能做什么 {#capabilities}

测试环境：搭载 M4 Pro 和 48 GB 内存的 Mac，vllm-metal 服务器，上下文为 64k 令牌，测试集群包含一个 Broker 和三个主题（7 个分区，约 3,000 条消息）。三个典型任务：检查集群并解释发现的问题、查看没有成员的组的延迟，以及根据服务日志中的时间查找消息。每个模型运行三次。

| 模型（4 位） | 正确回答 | 生成速度 | 集群检查 | 组延迟 | 消息查找 |
|---|---|---|---|---|---|
| Qwen3.6-35B-A3B | 9/9 | 25–30 令牌/秒 | 30–50 秒 | 45–60 秒 | 约 2 分钟 |
| Qwen3.8-27B | 9/9 | 8–10 令牌/秒 | 8–16 分钟 | 10–12 分钟 | 28–46 分钟 |

Qwen3.8-27B 的回答更详细，但它是稠密模型：每生成一个令牌都要用到全部 270 亿个参数，而 Qwen3.6-35B-A3B 只用到约 30 亿个。在 Mac 上日常使用，请选择 Qwen3.6-35B-A3B。

对于需要许多步骤的长时间排查，本地模型的表现不如云端模型：它在工具参数上更容易出错，也可能根据不完整的数据得出错误的结论。请对照 MCP 日志检查它的结论。

## 硬件和模型 {#hardware}

在普通硬件上，活跃参数较少（约 30 亿，如 Qwen3.6-35B-A3B）的 MoE 模型效果最好：它们比同样大小的稠密模型更快。上下文至少为 32k 令牌，最好为 64k。

| 平台 | 模型服务器 | 可运行的模型 |
|---|---|---|
| macOS，Apple Silicon，48 GB 或更多 | vllm-metal 或 Ollama | Qwen3.6-35B-A3B（已测试） |
| macOS，Apple Silicon，32 GB | Ollama | gpt-oss-20b；35B-A3B 处于极限 |
| Linux，显存 24 GB 的 NVIDIA | vLLM | 4 位的 35B-A3B，上下文较小 |
| Windows，NVIDIA | Ollama；vLLM 仅限 WSL2 | 与 Linux 相同 |
| 无 GPU 的 x86，32–64 GB DDR5 | Ollama | 仅限 MoE 模型，速度较慢 |

搭载 Intel 处理器的 Mac 不适合运行本地模型。

## 模型服务器 {#server}

服务器必须提供支持工具调用的 OpenAI 兼容 API。在 Mac 上，[Ollama](https://ollama.com) 是最简单的选择：安装应用，下载模型，并创建一个上下文为 64k 令牌的变体——Ollama 默认使用较小的上下文，Glafira 的工具说明放不下：

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

客户端需要地址 `http://127.0.0.1:11434/v1`、模型 `qwen3.6-64k:35b-a3b` 和任意非空的 API 密钥。Ollama 只监听 `127.0.0.1`，因此其他计算机无法连接到该模型。

空闲 5 分钟后，Ollama 会把模型从内存中卸载，下一个问题要等模型重新加载。要让模型在内存中保留更久，请设置环境变量 `OLLAMA_KEEP_ALIVE`，例如设为 `1h`。

在配备 NVIDIA 的 Linux 上，通常使用 vLLM。它通过 `--tool-call-parser` 参数解析工具调用，该参数按模型系列设置（Qwen3.6 使用 `qwen3_coder`）；在 Ollama 中，由模型模板完成这项工作。

## Glafira 设置 {#glafira-settings}

在“[[ui:settings.mcp.section]]”部分：

- [[ui:settings.mcp.responseLimit]]：一次工具调用的最大响应。100 KB 的响应约占 30k 令牌，因此对于 64k 的上下文，请选择 48 KB 或 24 KB：Glafira 会截断过长的列表，并提示模型缩小请求范围。
- “[[ui:settings.mcp.clients]]”列表中的“[[ui:settings.mcp.colCompact]]”：不含批注和更改建议的工具集，它们的描述约占 1,000 令牌的上下文。客户端连接并经你批准后才会出现该复选框；客户端重新连接后才能看到新的工具列表。

## 检查 {#check}

1. 连接客户端，并在 Glafira 对话框中批准它。
2. 向助手开放一个连接（最好是测试连接），级别为“[[ui:connections.form.llmLevel.metadata]]”。
3. 提三四个典型问题：“检查集群并解释发现的问题”“为什么组 … 的延迟很大？”“哪些主题没有复制？”。
4. 在“[[ui:settings.mcp.audit.title]]”中查看模型调用了哪些工具、结果如何，并将它的回答与 Glafira 中的数据对比。
5. 如果你要打开消息，请检查脱敏：按钮“[[ui:settings.mcp.audit.openInBrowse]]”会显示未脱敏的同一批消息。
6. 断开网络，重复一个问题：没有互联网时也必须能收到回答。

## 限制 {#limits}

- 本地模型的响应比云端模型慢数倍，而在 Mac 上，Qwen3.8-27B 这类稠密模型更慢（参见[表格](local-llm.md#capabilities)）。
- 模型运行时，计算机剩余的内存会减少：在 48 GB 的 Mac 上，一个 350 亿参数的模型约占 20 GB。
- 模型可能得出错误的结论。Glafira 不允许它更改集群中的任何内容，但决定权在你。
