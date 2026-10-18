# 浏览消息

在主题的[[ui:topics.details.tabMessages]]标签页中浏览消息。浏览不会加入消费者组，也不提交任何内容，因此不会改变集群。在[[ui:common.readOnly]]连接上也可以浏览。如需以组成员身份读取，请使用[消费者会话](consumer-sessions.md)。

## 开始读取 {#start-reading}

1. 从[[ui:sidebar.topics]]列表中打开一个主题，首先打开的是[[ui:topics.details.tabMessages]]标签页。
2. 在[[ui:browse.form.from]]中选择从哪里开始。参见[起始位置](browse.md#start-position)。
3. 可以设置格式和筛选条件。
4. 点击[[ui:browse.panel.read]]。

消息到达后会陆续出现在表格中。按钮旁的状态行显示已扫描的消息数、与筛选条件匹配的消息数以及表格中的消息数，还会显示状态：[[ui:browse.panel.reading]]、[[ui:browse.panel.finishLimit]]、[[ui:browse.panel.finishEnd]]或[[ui:browse.panel.finishCancelled]]。

不开启 `Live tail` 时，读取会在到达点击[[ui:browse.panel.read]]时的主题末尾处停止；如果更早达到[[ui:browse.form.limit]]，则提前停止。上限按与筛选条件匹配的消息计数，可设为 1 到 100000；值越大，读取越慢。

开启 `Live tail` 后，读取到达末尾时不会停止：新写入的消息会陆续添加进来。所有旧消息读取完毕后，状态显示为[[ui:browse.panel.waiting]]。表格最多保留上限数量的最新消息，并丢弃最旧的消息。

[[ui:browse.panel.stop]]会停止读取并保留表格。读取进行期间，[[ui:browse.panel.read]]按钮会变为[[ui:browse.panel.restart]]：它会以当前设置重新开始，并清空表格。如果与 Broker 的连接断开，应用会自动重新连接，并在状态行中显示。

来自不同分区的消息在表格中混合显示。Kafka 只保证单个分区内的顺序。`P` 列显示分区，`Offset` 显示在分区中的位置。选中 `UTC` 可按 UTC 而非本地时间显示时间。

## 起始位置 {#start-position}

[[ui:browse.form.startPosition]]设置每个分区中读取的起点：

- [[ui:browse.form.startLatest]]：最后 N 条消息。N 会平均分配到各分区，因此 100 条消息、4 个分区时，应用从每个分区最多取 25 条。
- [[ui:browse.form.startEarliest]]：从仍在存储中的最旧消息开始。
- [[ui:browse.form.startOffset]]：在每个分区中从同一偏移量开始。如果某个分区没有该偏移量，则从其开头或末尾开始读取。此模式最适合只有一个分区的情况。
- [[ui:browse.form.startTimestamp]]：从时间戳等于或晚于指定本地时间的第一条消息开始。在没有更新消息的分区中，从末尾开始读取。
- [[ui:browse.form.startLastMinutes]]：同上，但从当前时间往前推算。

显示[[ui:browse.form.partitionsPlaceholder]]的字段可将读取限定在部分分区。输入用逗号分隔的分区编号，例如 `0,2`。留空则读取所有分区。

## 筛选条件 {#filters}

筛选条件只显示匹配的消息。应用仍会从起始位置读取所有消息并逐条检查，因此筛选条件不会加快读取速度。所有已填写的字段都必须匹配。

- [[ui:browse.filters.keyContains]]和[[ui:browse.filters.valueContains]]：键或值中的文本。
- `regex`：将这些字段以及标头值视为正则表达式。
- `Aa`：区分大小写。默认不区分大小写。
- [[ui:browse.filters.header]]：消息带有名称完全一致的标头。[[ui:browse.filters.headerValue]]还会检查其值。
- `JSONPath`：针对 JSON 值的条件，例如 `$.order.status == "FAILED"`。不含 `==` 或 `!=` 时，只要求路径存在，例如 `$.error`。值不是有效 JSON 的消息不匹配。
- [[ui:browse.filters.timeFrom]]和[[ui:browse.filters.timeTo]]：消息时间戳在此范围内（本地时间），包含两端。

筛选条件检查的是解码后表格中显示的文本。如果值格式为 `hex`，值筛选会搜索十六进制文本。筛选条件在点击[[ui:browse.panel.read]]时生效；更改后请重新读取。

筛选条件不会移动起始位置。例如，[[ui:browse.form.startLatest]]加上筛选条件，只会在最后 N 条消息中查找匹配项。要搜索整个主题，请从[[ui:browse.form.startEarliest]]开始。

## 格式 {#formats}

[[ui:browse.form.format]]设置键和值的解码方式，可分别为[[ui:browse.form.key]]和[[ui:browse.form.value]]设置。只改变显示方式，不改变数据。

- `auto`：默认值。有效的 JSON 对象和数组显示为 JSON，其他 UTF-8 文本显示为字符串，二进制数据显示为十六进制。
- `string`：UTF-8 文本。不是有效 UTF-8 的字节会被替换为 `�`。
- `json`：在消息详情中带缩进显示 JSON。如果值不是有效 JSON，详情中会显示错误。
- `hex`：每个字节显示为两位十六进制数。
- `base64`：以 Base64 显示字节。

详情中会显示实际使用的格式，例如 `auto` 识别出 JSON 时显示 `json`。标头值显示为文本；二进制标头值显示为 `0x` 加十六进制。缺少的键或值显示为 `null`。

非常大的键和值只显示一部分，约为文本的前 1 MB，`hex` 和 `base64` 同样如此。此时消息详情中会显示警告。

## 消息详情 {#message-details}

点击某一行，可在表格旁打开消息详情。顶部是分区、偏移量和时间，下方是：

- [[ui:browse.details.value]]和[[ui:browse.details.key]]：完整文本，附带格式和以字节为单位的大小。JSON 带缩进显示。[[ui:browse.details.copy]]用于复制文本。
- [[ui:browse.table.colHeaders]]：标头名称和值的表格。

表格列包括 `P`、`Offset`、[[ui:browse.table.colTime]]、[[ui:browse.table.colKey]]、[[ui:browse.table.colValue]]、[[ui:browse.table.colHeaders]]（标头数量）和[[ui:browse.table.colSize]]（键和值的大小，以字节为单位）。时间是消息的时间戳。根据主题设置 `message.timestamp.type`，它由生产者或 Broker 设置。

## 复制消息 {#export-messages}

Glafira 不会将浏览到的消息保存到文件。要将消息从应用中取出，请复制它：

- [[ui:browse.details.copyJson]] 会将整条消息复制为 JSON：主题、分区、偏移量、时间戳、键、值、标头和大小。键和值以解码后的形式包含在内，并附带其格式。
- [[ui:browse.details.value]]或[[ui:browse.details.key]]旁的[[ui:browse.details.copy]]只复制该文本。

复制的文本使用读取时选择的格式。要精确复制二进制数据，请以 `hex` 或 `base64` 读取。非常大的键和值只按显示的部分复制，即使是 `hex` 和 `base64` 也是如此。参见[格式](browse.md#formats)。

## 在 Producer 中打开 {#open-in-producer}

[[ui:browse.details.resend]]会在同一连接的 [[ui:sidebar.producer]] 中打开该消息。表单会填入消息的主题、键、值、标头和格式，分区交由 Kafka 决定。消息文本中的占位符不会被替换：副本会按读取时的原样发送。

不会立即发送任何内容。请检查表单，必要时进行修改，然后从 Producer 发送。参见 [Producer](producer.md)。

在以下两种情况下，该按钮被禁用并显示原因：

- 键或值过大，只显示了一部分；
- 文本不是有效的 UTF-8，解码时有损失。请以 `hex` 或 `base64` 读取，以便发送精确副本。

在[[ui:common.readOnly]]连接上，该按钮会被隐藏。

## 将组重置到某条消息 {#reset-group-here}

“[[ui:browse.details.resetHere]]”用于准备偏移量重置，使消费者组从这条消息开始读取。

1. 在消息详情中，点击[[ui:browse.details.resetHere]]。
2. 在列表中选择一个组。列表中只列出已存在的组。
3. 点击[[ui:browse.details.goToReset]]。

组标签页会打开，并已填好重置表单：[[ui:groups.reset.strategy]]为[[ui:groups.reset.strategyOffset]]，偏移量为该消息的偏移量；[[ui:groups.reset.scope]]为[[ui:groups.reset.scopePartitions]]，主题和分区为该消息所在的主题和分区。此时尚未做任何更改：点击[[ui:groups.reset.preview]]查看新的偏移量，然后在该处应用重置。该组必须没有活动成员。参见[消费者组](groups.md)。

在[[ui:common.readOnly]]连接上，该按钮会被隐藏。
