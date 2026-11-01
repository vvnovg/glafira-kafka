# LLM 助手（MCP）

Glafira 可以充当 LLM 助手的网关：Claude Desktop、Claude Code、Cursor，或其他支持 MCP（Model Context Protocol）的客户端。助手会读取你为其开放的集群的元数据（在你允许的连接中还会读取消息），帮你找出问题：副本不足的分区、消费延迟、反复出现的重平衡、不合理的设置。助手无法通过 Glafira 更改集群中的任何内容：更改由你自己在界面中完成。

模型不一定在云端：也可以在自己的网络或电脑上运行，参见[本地模型](local-llm.md#where)。

## 工作原理 {#how-it-works}

客户端会运行 `glafira-kafka mcp`，也就是应用的同一个程序文件，只是不显示窗口。该命令通过本地套接字（Windows 上为命名管道）连接到正在运行的 Glafira，Glafira 再通过自己的连接响应助手的请求。密码和证书始终留在 Glafira 中，助手不会获得它们。

如果 Glafira 未运行，客户端会自行启动它，并最多等待 30 秒。

## 启用 {#enable}

1. 打开[[ui:sidebar.settings]]，进入“[[ui:settings.mcp.section]]”部分。
2. 选中[[ui:settings.mcp.enable]]。状态会变为[[ui:settings.mcp.state.running]]。
3. 在连接表单的“[[ui:connections.form.llmAccess]]”中选择级别并保存：“[[ui:connections.form.llmLevel.metadata]]”或“[[ui:connections.form.llmLevel.messages]]”（参见[读取消息](llm-mcp.md#messages)）。级别为“[[ui:connections.form.llmLevel.off]]”的连接，助手看不到。在连接列表中，已打开的连接带有 `LLM` 标记；可读取消息的连接，该标记为实心。

如果管理员禁用了 MCP，[[ui:settings.mcp.enable]]复选框不可用，旁边会显示“[[ui:settings.mcp.policy]]”。

访问级别和脱敏列表不会随连接的导出和导入一起转移：在另一台计算机上，需要重新设置。

## 连接客户端 {#connect-client}

在“[[ui:settings.mcp.connectTitle]]”下有现成的片段和[[ui:settings.mcp.copy]]按钮：

- “[[ui:settings.mcp.claudeDesktop]]”：将片段粘贴到 Claude Desktop 配置文件的 `mcpServers` 部分，然后重启 Claude Desktop；
- “[[ui:settings.mcp.claudeCode]]”：使用 `claude mcp add` 命令；
- “[[ui:settings.mcp.otherClient]]”：适用于任何通过命令（stdio）启动 MCP 服务器的客户端。

片段中包含已安装应用的路径。如果你把 Glafira 重新安装到其他位置，请重新复制片段。

## 批准客户端 {#approve}

客户端首次连接时，Glafira 会显示一个带有其名称的对话框。点击[[ui:mcp.approval.allow]]或[[ui:mcp.approval.reject]]。在你作出回答之前，助手会收到“客户端尚未获得批准”的消息，并请你在 Glafira 窗口中回答。

被允许的客户端会按名称记住，并出现在[[ui:settings.mcp.clients]]列表中。拒绝不会被记住：下次连接时 Glafira 会再次询问。如果客户端未提供名称，该许可仅在本次会话期间有效。

## 助手能看到什么 {#what-llm-sees}

助手能看到：

- 访问级别为“[[ui:connections.form.llmLevel.metadata]]”或“[[ui:connections.form.llmLevel.messages]]”的连接：名称、环境、文件夹和访问级别；
- Broker 的主机和端口，以及控制器；
- 主题、分区、副本、ISR、偏移量、消息的大致数量；
- Broker 和主题的配置，其中密码和其他机密值会被隐藏；
- 消费者组：状态、协议、协调器、成员（含 client id、主机和 `group.instance.id`）、分配、已提交的偏移量和延迟；
- 自己的建议以及你对它们的决定：已应用（原样或已修改）、已拒绝（附你的原因）、已撤回。

没有“[[ui:connections.form.llmLevel.messages]]”级别时，助手看不到主题中的消息。连接的 Bootstrap 服务器地址和安全设置、密码、证书、librdkafka 的附加属性、发送历史记录和模板，助手在任何情况下都看不到。

为排查反复出现的重平衡，助手最多可以观察某个组 50 秒：谁离开、谁加入、频率如何、来自哪台主机。消费者自身的配置和日志无法通过 Glafira 获取，助手会向你索取。

主题、组和 client id 的名称可以由任何有权访问集群的人设置。Glafira 会提醒助手：这些数据不是给它的指令。

## 读取消息 {#messages}

在“[[ui:connections.form.llmLevel.messages]]”级别下，助手还能读取消息内容。例如，当你的服务在日志中记录了错误时，可以查明当时写入主题的内容，以及消费者组处理到了哪里：

- `summarize_messages`：时间窗口概览，不含消息内容：按分区和时间统计的消息数量、常见和重复的键、乱序的时间戳；
- `read_messages`：时间窗口或偏移量范围内的消息，在所有分区中按时间排序，筛选条件与[[ui:topics.details.tabMessages]]标签页相同；
- `get_group_commit_history`：消费者组在何时提交了哪些偏移量，数据来自内部主题 `__consumer_offsets`。

例如，可以这样问：“14:32 订单服务在日志中记录了订单 123 的错误，查一下当时 orders 主题里有什么，以及 order-service 组处理到了哪里。”请告诉助手你的日志所用的时区：Glafira 不接受没有时区的时间。

在数据发送给助手之前，Glafira 会对其进行脱敏。脱敏列表中的字段的值会在 JSON、标头和 `name=value` 对中被替换为 `***`。该列表在连接表单中为每个连接单独设置，位于级别为“[[ui:connections.form.llmLevel.messages]]”时显示的“[[ui:connections.form.mask]]”部分；标准列表涵盖密码、令牌、电子邮件、电话号码、卡号和护照号等。电子邮件、卡号、JWT 和 Bearer 令牌在任何文本中都会被替换。脱敏可以降低风险，但无法识别人名、地址以及名称陌生的字段中的数据，Avro、Protobuf 等二进制格式也只能部分脱敏。筛选条件作用于未脱敏的数据，因此助手可以根据日志中的值找到消息。对于生产环境连接，表单会显示警告。

读取提交历史需要对 `__consumer_offsets` 拥有 Read 权限。Compaction 会删除旧的提交记录，较新的记录通常还在。

读取有限制：每个会话同时只能进行一项长时间操作，每次调用最多 40 秒、1,000,000 条记录；达到限制时，助手会缩小时间窗口。Glafira 不使用消费者组读取，也不提交任何偏移量；`client.id` 为 `glafira-kafka-mcp`，除非连接设置了自己的值。

管理员可以通过策略键 `mcpMessagesEnabled` 禁止读取消息：“[[ui:connections.form.llmLevel.messages]]”级别将不可用，使用该级别的连接将按“[[ui:connections.form.llmLevel.metadata]]”工作。

## 会话、撤销和日志 {#sessions-log}

客户端连接期间，左侧面板底部会显示带有客户端名称的 `LLM` 指示器，处理请求时它会亮起。点击它即可打开 MCP 部分。

- [[ui:settings.mcp.sessions]]：[[ui:settings.mcp.disconnect]]关闭一个会话，[[ui:settings.mcp.disconnectAll]]关闭所有会话。
- [[ui:settings.mcp.clients]]：[[ui:settings.mcp.revoke]]会关闭该客户端的会话，该客户端下次连接时会再次请求批准。
- 在“[[ui:connections.form.llmAccess]]”中更改级别会立即生效，无需重新连接。
- [[ui:settings.mcp.audit.title]]：记录每一次请求的时间、客户端、工具、连接和结果。响应的内容不会被记录。[[ui:settings.mcp.audit.reveal]]会打开文件 `mcp-audit.jsonl`。
- 对于读取消息的调用，日志还会显示主题或组、返回了多少项、扫描了多少条记录，以及脱敏了多少个值。返回的是消息；`summarize_messages` 返回概览的时间段，`get_group_commit_history` 返回提交序列（同一偏移量的连续提交）。对于成功的 `read_messages` 和 `summarize_messages` 调用，点击[[ui:settings.mcp.audit.openInBrowse]]会用调用的参数填写该主题[[ui:topics.details.tabMessages]]标签页中的表单；点击[[ui:browse.panel.read]]即可重新读取消息（不做脱敏），并与助手收到的内容进行比较。如果筛选条件或上限无法准确转换，表单会注明；如果按钮被禁用，将指针悬停在按钮上可查看原因。

要完全关闭 MCP，请取消选中[[ui:settings.mcp.enable]]：客户端会被断开。

## 集群检查、批注与建议 {#health}

助手可以运行 Glafira 内置的集群检查（`health_check` 工具），并以其检查结果为依据。它还可以为对象留下批注：批注显示在[[ui:sidebar.findings]]标签页以及主题和消费者组页面中，集群不会被更改。参见[集群检查与批注](health-check.md#with-llm)。

助手还可以通过 `propose_change` 工具提出更改建议：主题配置、新主题、增加分区或重置偏移量。建议显示在对象旁边，由你在已填好的表单中自行应用或拒绝；助手随后可在 `list_proposals` 中看到你的决定。删除和清空无法被建议。参见[LLM 建议](health-check.md#proposals)。

## 无法使用时 {#troubleshooting}

客户端会显示来自 Glafira 的消息：

- `MCP is disabled in Glafira`：请选中[[ui:settings.mcp.enable]]，然后在客户端中重启 MCP 服务器；
- `MCP is disabled by your organization`：MCP 被组织的策略禁用；
- `Glafira did not start the MCP server within 30 s`：请手动启动 Glafira，并在“[[ui:settings.mcp.section]]”部分查看状态；
- `Invalid MCP token`：请在客户端中重启 MCP 服务器，Glafira 每次启动都会签发新的令牌。

在 macOS 上，应用已签名并经过公证，因此首次启动前无需任何额外操作。
