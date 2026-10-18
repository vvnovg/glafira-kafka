# 消费者组

消费者组是一组共同分担主题读取工作的消费者。Kafka 会将每个分区分配给组中的一个成员，并为每个分区保存该组已提交的偏移量。应用重启后会从该偏移量继续读取。

在[[ui:sidebar.connections]]面板中的连接下方打开[[ui:sidebar.groups]]。某个主题的消费者组也可以在该主题的[[ui:topics.details.tabGroups]]标签页中查看。参见[主题页面](topics.md#topic-details)。

## 组列表 {#group-list}

列表显示集群中的所有消费者组。各列含义：

- [[ui:groups.list.colGroup]]：组 ID（`group.id`）。
- [[ui:groups.list.colState]]：Kafka 报告的状态，例如 `Stable`（成员正在读取）、`Empty`（没有成员）、`PreparingRebalance` 或 `CompletingRebalance`（正在重新分配分区）以及 `Dead`（该组已不存在）。
- [[ui:groups.list.colMembers]]：活动成员数。
- `Assignor`：用于分配分区的策略，例如 `range`。
- [[ui:groups.list.colLag]]：组的总延迟。参见[延迟](groups.md#lag)。
- [[ui:groups.list.colTopics]]：该组已提交偏移量的主题。

在[[ui:groups.list.search]]中输入文本，可显示 ID 包含该文本的组，不区分大小写。旁边的状态列表可只显示处于某一状态的组；[[ui:groups.list.allStates]]会取消此筛选。计数器显示当前显示的组数和组的总数。

成员、分配器和延迟只在滚动时为可见的行加载。数据到达前，单元格显示 `…`。如果无法加载，单元格显示 `—`；将指针悬停在上面可查看原因。

[[ui:groups.list.refresh]]会重新加载列表及其中的数据。如果有 Broker 未响应，会显示警告，提示列表可能不完整。

## 延迟 {#lag}

延迟是该组尚未读取的消息数。对于每个分区，延迟等于日志末尾减去已提交的偏移量。延迟永远不会显示为负数。

延迟只取决于已提交的偏移量。活动消费者读取的位置可能已超过其提交的位置，因此实际延迟可能更小。组再次提交后，延迟会下降。

没有已提交偏移量的分区显示[[ui:groups.lag.noCommit]]，不计入总数。当组完全没有提交（[[ui:groups.lag.noCommits]]），或者无法读取某个分区的偏移量（[[ui:groups.lag.unknown]]）时，组的总延迟显示 `—`。

延迟统计的是偏移量，而不是消息。在使用 Compaction 或事务的主题中，有些偏移量上没有可见的消息，因此该组剩余待读的消息可能比延迟显示的少。

这些值是加载时的快照。要跟踪消费延迟，请打开该组并开启自动刷新。

## 组详情 {#group-details}

点击列表中的某个组，可打开其标签页。顶部摘要显示状态、成员数和分配器。

- [[ui:groups.details.autoRefresh]]：[[ui:groups.details.refreshOff]]、[[ui:groups.details.refresh5]]或[[ui:groups.details.refresh10]]。
- [[ui:groups.details.refresh]]：立即重新加载。
- [[ui:groups.details.join]]：在该组中启动[消费者会话](consumer-sessions.md)。会话表单会填入组 ID 以及该组有提交的主题。此功能需要 Pro，并且在[[ui:common.readOnly]]连接上该按钮被禁用；将指针悬停在按钮上可查看原因。

[[ui:groups.details.partitions]]列出该组有提交的分区以及分配给其成员的分区。表格上方显示组的总延迟。列包括[[ui:groups.details.colTopic]]、[[ui:groups.details.colPartition]]、`Committed`（已提交的偏移量）、`Log end`（分区的结束偏移量）、[[ui:groups.details.colLag]]和[[ui:groups.details.colMember]]（读取该分区的成员的客户端 ID）。

[[ui:groups.details.members]]列出活动成员：`Client ID`、[[ui:groups.details.colHost]]、`Member ID` 和[[ui:groups.details.colAssignment]]（成员的分区，格式为 `topic:partition`）。没有成员的组会显示“[[ui:groups.details.noMembers]]”

第二行有[[ui:groups.details.resetOffsets]]和[[ui:groups.details.deleteGroup]]。在只读连接上，该行会被隐藏。

## 重置偏移量 {#reset-offsets}

重置偏移量会移动组已提交的偏移量，之后该组会重新读取消息或跳过消息。只能为没有活动成员的组（状态为 `Empty` 或 `Dead`）重置偏移量，否则成员会用自己的提交覆盖新的偏移量。请先停止该组的应用。

1. 在组标签页中，点击[[ui:groups.details.resetOffsets]]。组中有成员时，该按钮被禁用；将指针悬停在按钮上可查看原因。
2. 选择[[ui:groups.reset.strategy]]。参见[重置策略](groups.md#reset-strategies)。
3. 选择[[ui:groups.reset.scope]]。参见[重置范围](groups.md#reset-scope)。
4. 点击[[ui:groups.reset.preview]]。
5. 检查表格，然后点击应用按钮。按钮上会显示将要更改的分区数。

预览不会做任何更改。对于每个分区，预览会显示主题、分区、[[ui:groups.reset.colCurrent]]（已提交的偏移量）、[[ui:groups.reset.colNew]]（目标偏移量）以及 `Low / High`（分区的起始和结束偏移量）。无法计算目标的分区会标记为“[[ui:groups.reset.noTarget]]”并注明原因，此类分区会被跳过。

只会应用预览中显示的计划。表单中的任何更改都会隐藏预览，需要再次点击[[ui:groups.reset.preview]]。在生产环境连接上，应用时会要求输入连接名称；在其他连接上则立即应用。

执行之前，应用会再次检查该组是否没有成员；执行之后，[[ui:groups.reset.result]]会列出每个分区及其新的偏移量或错误。

也可以从某条消息准备重置：参见[将组重置到某条消息](browse.md#reset-group-here)。[[ui:common.close]]会关闭重置面板。

## 重置策略 {#reset-strategies}

[[ui:groups.reset.strategy]]为范围内的每个分区设置目标偏移量：

- [[ui:groups.reset.strategyEarliest]]：仍在存储中的第一个偏移量。该组会重新读取主题中保留的全部内容。
- [[ui:groups.reset.strategyLatest]]：分区的末尾。该组会跳过所有积压的消息，只读取新消息。
- [[ui:groups.reset.strategyTimestamp]]：写入时间等于或晚于指定时间的第一条消息，时间在字段“[[ui:groups.reset.timestamp]]”中设置。在没有更新消息的分区中，目标为末尾。
- [[ui:groups.reset.strategyOffset]]：在每个分区中使用指定的[[ui:groups.reset.offset]]，即大于或等于 0 的整数。如果某个分区没有该偏移量，则目标为其起始或结束偏移量。
- [[ui:groups.reset.strategyShift]]：按字段“[[ui:groups.reset.shift]]”中的数量移动已提交的偏移量。负数表示往回重新读取 N 条消息，正数表示跳过 N 条消息。结果会限制在起始和结束偏移量之间。没有已提交偏移量的分区无法平移，会被跳过。

时间为你的本地时间，精确到秒。Kafka 根据消息时间戳查找偏移量。

## 重置范围 {#reset-scope}

[[ui:groups.reset.scope]]设置要重置哪些分区，其他分区保留原有的偏移量。

- [[ui:groups.reset.scopeAll]]：该组已提交偏移量的所有分区。如果该组尚无提交，请改为选择主题。
- [[ui:groups.reset.scopeTopics]]：所列主题的所有分区，包括没有提交的分区。名称之间用逗号、空格或换行分隔。可用于让组从选定位置开始读取新主题。
- [[ui:groups.reset.scopePartitions]]：仅所列分区。请在字段“[[ui:groups.reset.partitionsLabel]]”中以 `topic:partition` 格式输入，用逗号或空格分隔，例如 `orders:0, orders:3`。

## 删除组 {#delete-group}

删除组会移除其所有已提交的偏移量。只能删除没有成员、处于 `Empty` 状态的组。

1. 在组标签页中，点击[[ui:groups.details.deleteGroup]]。组中有成员时，该按钮被禁用；将指针悬停在按钮上可查看原因。
2. 输入组 ID 进行确认。在生产环境连接上，还需输入连接名称。
3. 点击[[ui:common.delete]]。

删除后，组标签页会关闭。如果使用此 `group.id` 的应用再次启动，Kafka 会重新创建该组。由于没有已提交的偏移量，应用会按其 `auto.offset.reset` 设置的位置开始：从头或从末尾。

在[[ui:common.readOnly]]连接上，该按钮会被隐藏。
