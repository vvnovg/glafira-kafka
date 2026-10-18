# 集群与主题

本章介绍集群概览、主题列表、创建和删除主题、主题页面以及主题配置。[[ui:sidebar.connections]]面板中每个连接下方都有[[ui:sidebar.cluster]]和[[ui:sidebar.topics]]的链接，每个链接都在各自的标签页中打开。

## 集群概览 {#cluster-overview}

[[ui:cluster.overview.title]]显示加载时集群的状态。点击[[ui:cluster.overview.refresh]]可重新加载。

- `Cluster ID`：集群标识符。
- [[ui:cluster.overview.controller]]：集群报告为控制器的 Broker 的 ID。在 KRaft 集群中，客户端不直接与控制器通信：集群在这里报告的是一个随机 Broker，而不是活动控制器，并且每次刷新都可能不同。
- [[ui:cluster.overview.brokers]]：Broker 数量。
- [[ui:cluster.overview.topicsInternal]]：全部主题数，括号中为内部主题数。
- [[ui:cluster.overview.partitions]]：所有主题的分区总数。
- `Under-replicated`：并非所有副本都处于同步状态（在 ISR 中）的分区。通常是某个 Broker 宕机或跟不上。
- `Offline`：没有 Leader 的分区，无法写入，也无法读取。

`Under-replicated` 或 `Offline` 的值不为零时会突出显示。

下方是 Broker 表，包含 `ID` 和[[ui:cluster.overview.address]]。[[ui:cluster.overview.controller]]字段中的 Broker 带有“[[ui:cluster.overview.controllerBadge]]”标记。在 KRaft 集群中，这同样是那个随机 Broker，而不是活动控制器。点击某个 Broker，可在表格下方查看其配置。Broker 配置只能查看。使用[[ui:common.config.searchParam]]可按名称查找参数，使用[[ui:common.config.onlyChanged]]可隐藏取默认值的参数。只读参数带有锁形图标。

## 主题列表 {#topic-list}

[[ui:sidebar.topics]]列出集群中的主题。点击某一行，可在单独的标签页中打开该主题。

列包括[[ui:topics.list.colName]]、[[ui:topics.list.colPartitions]]、`RF`（副本因子）、[[ui:topics.list.colMessages]]、`cleanup.policy` 和[[ui:topics.list.colUnderRepl]]（副本不足的分区数）。消息数是近似值：即各分区结束偏移量与起始偏移量之差。Compaction 和事务标记会使实际数量更少。消息数和 `cleanup.policy` 只为滚动到的行加载。

工具栏控件：

- [[ui:topics.list.search]]：输入名称的一部分即可查找主题，不区分大小写。要使用正则表达式搜索，请将其放在两个斜杠之间，例如 `/^orders\..*/`。
- [[ui:topics.list.internal]]：显示内部主题，例如 `__consumer_offsets`。内部主题带有“[[ui:topics.list.internalBadge]]”标记。
- 自动刷新列表：[[ui:topics.list.autoRefreshOff]]、[[ui:topics.list.every10]]或[[ui:topics.list.every30]]。
- [[ui:topics.list.refresh]]：立即重新加载列表和消息数。

工具栏旁会显示与搜索匹配的主题数及主题总数。在[[ui:common.readOnly]]连接上，[[ui:topics.list.create]]和[[ui:topics.list.select]]按钮会被隐藏。

## 创建主题 {#create-topic}

1. 在主题列表中点击[[ui:topics.list.create]]，列表上方会打开[[ui:topics.create.title]]表单。
2. 输入[[ui:topics.create.name]]。名称可包含拉丁字母、数字、`.`、`_` 和 `-`，最多 249 个字符。如果名称中同时含有 `.` 和 `_`，表单会发出警告：此类主题的 Kafka 指标名称可能与其他主题冲突。
3. 设置[[ui:topics.create.partitions]]。各分区可并行读取，而顺序只在单个分区内得到保证。分区数以后可以增加，但永远不能减少。
4. 设置[[ui:topics.create.rf]]，即 Broker 为每个分区保留的副本数量。它不能超过 Broker 数量，表单会在字段下方显示 Broker 数量。默认为 3；如果 Broker 少于 3 个，则为 Broker 数量。`-1` 表示使用 Broker 的默认值。
5. 选择[[ui:topics.create.preset]]：
   - [[ui:topics.create.presetDelete]]：按时间删除旧消息。
   - `compact`：每个键只保留最新的值。
   - `compact + delete`：两者兼有。
   - [[ui:topics.create.presetTest]]：消息保留一小时。
6. 可以添加[[ui:topics.create.extraConfigs]]（可选），每行一个，例如 `retention.ms=86400000`。它们会覆盖预设。以 `#` 开头的行会被忽略。
7. 点击[[ui:topics.create.submit]]。

表单有效时，会显示等效的 `kafka-topics.sh` 命令。你可以复制该命令，例如用于在另一个集群中创建相同的主题。

在 prod 连接上，需要输入连接名称来确认创建。主题创建后，会打开其标签页。

## 删除主题 {#delete-topics}

要删除一个主题，请打开该主题并点击[[ui:topics.actions.deleteTopic]]。输入主题名称进行确认，然后点击[[ui:topics.delete.confirm]]。主题标签页随即关闭。

一次删除多个主题：

1. 在主题列表中点击[[ui:topics.list.select]]，名称旁会出现复选框。
2. 选中要删除的主题。内部主题无法选中。
3. 点击删除按钮。按钮上会显示已选主题的数量。
4. 检查名称列表，输入主题数量进行确认，然后点击[[ui:topics.delete.confirm]]。

[[ui:topics.list.cancelSelect]]会隐藏复选框。

在 prod 连接上，这两种删除方式都还会要求输入连接名称。

删除无法撤销，数据将会丢失。如果某些主题未被删除（例如由于权限不足），应用会列出这些主题及原因。其他主题会被删除。

## 主题页面 {#topic-details}

主题标签页显示分区数、副本因子和近似消息数。顶部按钮：

- [[ui:topics.details.refresh]]：重新加载主题数据。
- [[ui:topics.details.produce]]：以此主题打开[[ui:sidebar.producer]]。参见 [Producer](producer.md)。
- [[ui:topics.details.consume]]：为此主题启动消费者会话。此功能需要 Pro。参见[消费者会话](consumer-sessions.md)。

在[[ui:common.readOnly]]连接上，这两个按钮均被禁用。

第二行是会更改主题的操作。在只读连接上以及对于内部主题，该行会被隐藏。

- [[ui:topics.actions.addPartitions]]：输入新的总数，然后点击[[ui:topics.actions.addSubmit]]。分区数不能减少。增加分区后，具有相同键的消息可能会进入其他分区，因此按键的顺序会被打乱。
- [[ui:topics.actions.purge]]：删除所选分区中截至当前末尾的所有消息，主题本身保留。选择分区，输入主题名称，然后点击[[ui:topics.actions.purgeConfirm]]。只有当 `cleanup.policy` 包含 `delete` 时才能清空，否则该按钮被禁用并显示原因。对于 `compact,delete` 主题，清空也会删除各键的最新值。
- [[ui:topics.actions.deleteTopic]]：参见[删除主题](topics.md#delete-topics)。

在 prod 连接上，以上每项操作都还会要求输入连接名称。

下方的标签页：

- [[ui:topics.details.tabMessages]]：浏览消息。参见[浏览消息](browse.md)。
- [[ui:topics.details.tabPartitions]]：列出每个分区的表格，包括[[ui:topics.details.colLeader]]、[[ui:topics.details.colReplicas]]、`ISR`（同步副本）、起始（`Low`）和结束（`High`）偏移量以及消息数。没有 Leader 的分区显示“[[ui:topics.details.noLeader]]”。
- [[ui:topics.details.tabConfigs]]：主题参数。参见[主题配置](topics.md#topic-config)。
- [[ui:topics.details.tabGroups]]：已为此主题提交偏移量的消费者组，包括[[ui:topics.groups.colState]]、[[ui:topics.groups.colPartitions]]和[[ui:topics.groups.colLag]]。点击某个组即可打开。参见[消费者组](groups.md)。

## 主题配置 {#topic-config}

[[ui:topics.details.tabConfigs]]标签页列出所有主题参数，共三列：

- [[ui:common.config.colParam]]：参数名称。粗体名称表示已显式设置：在主题上或在 Broker 配置中。
- [[ui:common.config.colValue]]：当前值。敏感值显示为 `••••••`。
- [[ui:common.config.colSource]]：值的来源。例如，`DynamicTopic` 表示在此主题上设置，`StaticBroker` 和 `DynamicBroker` 来自 Broker 配置，`Default` 是 Kafka 默认值。

使用[[ui:common.config.searchParam]]可按名称查找参数，使用[[ui:common.config.onlyChanged]]可只显示粗体参数。只读参数带有锁形图标。名称旁的 `?` 图标会解释该参数。

## 修改配置 {#edit-config}

1. 在[[ui:topics.details.tabConfigs]]标签页中，点击[[ui:topics.details.edit]]。在[[ui:common.readOnly]]连接上，此按钮会被隐藏。
2. 在[[ui:common.config.colValue]]列中修改值。只读参数和敏感参数无法编辑。要移除主题自身的值，请点击[[ui:common.config.reset]]，此时将使用 Broker 的值。不允许空值。
3. 点击[[ui:common.config.review]]。应用会以“旧值 → 新值”的形式显示每项更改；[[ui:common.config.defaultValue]]表示该值被重置。
4. 点击[[ui:common.config.apply]]，或点击[[ui:common.config.back]]继续编辑。

应用之前，集群中不会发生任何更改。[[ui:common.config.discard]]会取消所有编辑。存在未保存的编辑时，[[ui:topics.details.closeEditor]]被禁用。在 prod 连接上，需要输入连接名称来确认更改。

只会发送你修改过的参数，主题的其他参数保持不变。Broker 会检查这些值，并以错误拒绝无效值。

## 主题参数 {#config-parameters}

Kafka 主题有几十个参数，配置表中的 `?` 图标会逐一解释。最常用的有以下几个：

- `cleanup.policy`：旧数据如何处理。`delete` 按时间或大小删除旧的日志段。`compact` 每个键只保留最新的值。`compact,delete` 两者兼有。
- `retention.ms`：消息保留多长时间，单位为毫秒。默认为 `604800000`（7 天）；`-1` 表示永久保留。当 `cleanup.policy` 包含 `delete` 时生效。
- `retention.bytes`：删除旧日志段之前每个分区的最大大小。默认值 `-1` 表示不限制大小。
- `segment.ms` 和 `segment.bytes`：Broker 何时开始新的日志段。保留和 Compaction 以整个日志段为单位进行，因此在其所在日志段关闭之前，消息的保留时间可能超过 `retention.ms`。
- `min.insync.replicas`：使用 `acks=all` 时，必须有多少个副本确认写入。副本因子为 3 时通常设为 2。
- `max.message.bytes`：Broker 为此主题接受的最大批次。默认约为 1 MB。生产者也必须允许相同的大小（`max.request.size`）。
- `compression.type`：Broker 如何存储消息。`producer` 表示保留生产者的压缩方式。
- `message.timestamp.type`：`CreateTime`（生产者设置的时间）或 `LogAppendTime`（Broker 写入消息的时间）。浏览消息时显示的就是这个时间。
- `delete.retention.ms`：删除标记（值为 null 的消息）在 Compaction 主题中保留多长时间。默认为 1 天。

有些参数只在较新的 Kafka 版本中存在，有些已被弃用，`?` 图标会在相关之处说明。分层存储相关参数（例如 `remote.storage.enable`）仅在集群支持时生效。
