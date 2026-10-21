# 集群检查与批注

Glafira 使用一组内置规则检查集群，并在对象旁显示发现的问题：连接树、主题和消费者组列表，以及主题和消费者组的页面。LLM 助手可以为这些检查结果补充自己的批注。检查只读取集群，不会更改其中的任何内容。

## 运行检查 {#run}

打开连接的[[ui:sidebar.cluster]]或[[ui:sidebar.findings]]，然后点击[[ui:health.check]]。一次检查最多需要 45 秒：Glafira 读取 Broker、分区、主题配置（最多 2000 个主题）和消费者组（最多 500 个）。如果主题或消费者组超过上述数量、某些内容无法读取或 45 秒时间已到，结果会标记为[[ui:health.partial]]，检查结果[[ui:health.ruleNames.check.partial]]会说明哪些内容没有读取。

Glafira 不会自行轮询集群：只有在你点击按钮或 LLM 助手请求时才会运行检查。结果会保留到下一次检查。

## 检查内容 {#rules}

- Broker 和分区：没有控制器、不可用和副本不足的分区、单 Broker 集群、Leader 分布不均、Leader 不在首选副本上。
- 主题的持久性：副本因子为 1、`min.insync.replicas` 大于副本因子、`min.insync.replicas` 等于副本因子、`min.insync.replicas` 为 1、不干净的 Leader 选举。
- Broker 配置：自动创建主题、默认副本因子为 1、默认 `min.insync.replicas` 为 1、各 Broker 之间不一致的重要设置。
- 消费者组：有延迟的空组、正在重平衡的组、没有分区的成员、自上次检查以来增长的延迟。

## 严重程度与环境 {#severity}

每个检查结果都有严重程度：[[ui:health.severity.critical]]、[[ui:health.severity.warning]]或[[ui:health.severity.info]]。它取决于连接的环境。例如，副本因子为 1 在 prod 环境中是严重，在 stage 环境中是警告，在 dev、test 和 local 环境中不显示。环境在连接表单中设置。

## “检查结果”标签页 {#tab}

在[[ui:sidebar.findings]]标签页中，检查结果按规则分组，每条都会说明问题以及处理方法。可以按[[ui:health.filterSeverity]]、[[ui:health.filterSource]]和[[ui:health.filterKind]]筛选，也可以按名称搜索。点击对象名称会打开它的页面。

连接树以及主题和消费者组列表中带数字的标记，显示与该对象相关的检查结果和未关闭的 LLM 批注中，严重程度为[[ui:health.severity.critical]]或[[ui:health.severity.warning]]的数量。将指针悬停在标记上可查看它们的标题。

## 隐藏检查结果 {#hide}

如果某个对象的检查结果在预期之内，例如 prod 环境中副本因子为 1 的测试主题，请点击[[ui:health.hide]]。隐藏的检查结果不计入标记，之后的检查中也会保持隐藏。要查看隐藏的结果，请勾选“显示已隐藏”，在那里可以点击[[ui:health.restore]]。

## LLM 批注 {#notes}

通过 MCP 连接的 LLM 助手（参见 [LLM 助手（MCP）](llm-mcp.md#how-it-works)）可以为集群、Broker、主题或消费者组留下批注。批注显示在“[[ui:health.notes]]”和对象页面中，并署有客户端名称。文本按原样显示，不带格式和链接。

- [[ui:health.resolve]]和[[ui:health.dismiss]]会关闭批注，已关闭的批注保存在折叠的部分中。
- [[ui:common.delete]]删除一条批注，[[ui:health.deleteAll]]删除该连接的所有批注。
- 批注在创建 7 天后自动删除。一个连接最多可以有 200 条未关闭的批注。

## 与 LLM 助手配合 {#with-llm}

助手通过 `health_check` 工具运行同样的检查，并看到同样的检查结果，包括带标记的已隐藏结果。如果上次检查不到 5 分钟，助手会直接获得其结果，而不会再次查询集群。例如，可以这样请求：“检查集群的健康状况，并针对最重要的问题留下批注。”
