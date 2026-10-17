# Browsing messages

You browse messages on the [[ui:topics.details.tabMessages]] tab of a topic. Browsing does not join a consumer group and commits nothing, so the cluster does not change. It also works on [[ui:common.readOnly]] connections. To read as a member of a group, use a [consumer session](consumer-sessions.md).

## Start reading {#start-reading}

1. Open a topic from the [[ui:sidebar.topics]] list. The [[ui:topics.details.tabMessages]] tab opens first.
2. Choose where to start in [[ui:browse.form.from]]. See [Start position](browse.md#start-position).
3. Optionally, set the formats and the filters.
4. Click [[ui:browse.panel.read]].

Messages appear in the table as they arrive. The line next to the buttons shows how many messages were scanned, how many matched the filters and how many are in the table. It also shows the state: [[ui:browse.panel.reading]], [[ui:browse.panel.finishLimit]], [[ui:browse.panel.finishEnd]] or [[ui:browse.panel.finishCancelled]].

Without `Live tail`, reading stops at the end of the topic as it was when you clicked [[ui:browse.panel.read]], or earlier when the [[ui:browse.form.limit]] is reached. The limit counts messages that match the filters. It can be from 1 to 100000; large values make reading slower.

With `Live tail` on, reading does not stop at the end: new messages are added as they are written. When all old messages are read, the state shows [[ui:browse.panel.waiting]]. The table keeps the last messages up to the limit and drops the oldest ones.

[[ui:browse.panel.stop]] stops reading and keeps the table. While reading runs, the [[ui:browse.panel.read]] button becomes [[ui:browse.panel.restart]]: it starts again with the current settings and clears the table. If the connection to the broker is lost, the app reconnects by itself and shows that in the state line.

Messages from different partitions are mixed in the table. Kafka keeps the order only within one partition. The `P` column shows the partition, and `Offset` shows the position in it. Select `UTC` to show times in UTC instead of local time.

## Start position {#start-position}

[[ui:browse.form.startPosition]] sets where reading begins in each partition:

- [[ui:browse.form.startLatest]] — the last N messages. N is split evenly between the partitions, so with 100 messages and 4 partitions the app takes up to 25 from each.
- [[ui:browse.form.startEarliest]] — from the oldest message that is still stored.
- [[ui:browse.form.startOffset]] — from the same offset in each partition. If a partition has no such offset, reading starts at its beginning or end. This mode is most useful with one partition.
- [[ui:browse.form.startTimestamp]] — from the first message with a timestamp at or after the given local time. In partitions without newer messages, reading starts at the end.
- [[ui:browse.form.startLastMinutes]] — the same, counted back from now.

The field with [[ui:browse.form.partitionsPlaceholder]] limits reading to some partitions. Enter partition numbers separated by commas, for example `0,2`. Leave it empty to read all partitions.

## Filters {#filters}

Filters show only matching messages. The app still reads all messages from the start position and checks each one, so filters do not make reading faster. All filled fields must match.

- [[ui:browse.filters.keyContains]] and [[ui:browse.filters.valueContains]] — text in the key or the value.
- `regex` — treat these fields, and the header value, as regular expressions.
- `Aa` — match case. By default, case does not matter.
- [[ui:browse.filters.header]] — the message has a header with this exact name. [[ui:browse.filters.headerValue]] also checks its value.
- `JSONPath` — a condition on a JSON value, for example `$.order.status == "FAILED"`. Without `==` or `!=`, the path must simply exist, for example `$.error`. Messages whose value is not valid JSON do not match.
- [[ui:browse.filters.timeFrom]] and [[ui:browse.filters.timeTo]] — the message timestamp is within this range, in local time. Both ends are included.

Filters check the text shown in the table, after decoding. If the value format is `hex`, the value filter searches the hex text. Filters apply when you click [[ui:browse.panel.read]]; after changing them, read again.

A filter does not move the start position. For example, [[ui:browse.form.startLatest]] with a filter finds matches only among the last N messages. To search the whole topic, start from [[ui:browse.form.startEarliest]].

## Formats {#formats}

[[ui:browse.form.format]] sets how keys and values are decoded, separately for the [[ui:browse.form.key]] and the [[ui:browse.form.value]]. Only the display changes, not the data.

- `auto` — the default. Valid JSON objects and arrays are shown as JSON, other UTF-8 text as a string, binary data as hex.
- `string` — text in UTF-8. Bytes that are not valid UTF-8 are replaced with `�`.
- `json` — JSON with indents in the message details. If the value is not valid JSON, the details show the error.
- `hex` — every byte as two hex digits.
- `base64` — the bytes in Base64.

The details show which format was actually used, for example `json` when `auto` detected JSON. Header values are shown as text; binary header values are shown as `0x` followed by hex. A missing key or value is shown as `null`.

Very large keys and values are shown only partly, about the first 1 MB of text. This also applies to `hex` and `base64`. The message details then show a warning.

## Message details {#message-details}

Click a row to open the message details next to the table. At the top are the partition, the offset and the time. Below:

- [[ui:browse.details.value]] and [[ui:browse.details.key]] — the full text, with the format and size in bytes. JSON is shown with indents. [[ui:browse.details.copy]] copies the text.
- [[ui:browse.table.colHeaders]] — a table of header names and values.

The table columns are `P`, `Offset`, [[ui:browse.table.colTime]], [[ui:browse.table.colKey]], [[ui:browse.table.colValue]], [[ui:browse.table.colHeaders]] (their number) and [[ui:browse.table.colSize]] (the key and value size in bytes). The time is the message timestamp. Depending on the topic setting `message.timestamp.type`, it is set by the producer or by the broker.

## Copy messages {#export-messages}

Glafira does not save browsed messages to a file. To take a message out of the app, copy it:

- [[ui:browse.details.copyJson]] copies the whole message as JSON: topic, partition, offset, timestamp, key, value, headers and size. Key and value are included as decoded, with their format.
- [[ui:browse.details.copy]] next to [[ui:browse.details.value]] or [[ui:browse.details.key]] copies only that text.

The copied text uses the format chosen for reading. To copy binary data exactly, read it as `hex` or `base64`. Very large keys and values are copied only partly, as shown, even in `hex` and `base64`. See [Formats](browse.md#formats).

## Open in Producer {#open-in-producer}

[[ui:browse.details.resend]] opens the message in the [[ui:sidebar.producer]] of the same connection. The form gets the topic, key, value, headers and formats of the message. The partition is left to Kafka. Placeholders in the message text are not replaced: the copy is sent as it was read.

Nothing is sent right away. Check the form, change it if needed, and send it from the Producer. See [Producer](producer.md).

The button is disabled in two cases, and the reason is shown:

- the key or value was shown only partly because it is too large;
- the text was decoded with losses because it is not valid UTF-8. Read it as `hex` or `base64` to send an exact copy.

On [[ui:common.readOnly]] connections, the button is hidden.

## Reset a group to a message {#reset-group-here}

[[ui:browse.details.resetHere]] prepares an offset reset so that a consumer group starts reading from this message.

1. In the message details, click [[ui:browse.details.resetHere]].
2. Choose a group in the list. Only existing groups are listed.
3. Click [[ui:browse.details.goToReset]].

The group tab opens with the reset form filled in. [[ui:groups.reset.strategy]] is [[ui:groups.reset.strategyOffset]] with the offset of the message. [[ui:groups.reset.scope]] is [[ui:groups.reset.scopePartitions]] with the topic and partition of the message. Nothing changes yet: click [[ui:groups.reset.preview]] to see the new offsets, then apply the reset there. The group must have no active members. See [Consumer groups](groups.md).

On [[ui:common.readOnly]] connections, the button is hidden.
