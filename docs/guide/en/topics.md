# Cluster and topics

This chapter covers the cluster overview, the topic list, creating and deleting topics, the topic screen and topic configuration. Under each connection in the [[ui:sidebar.connections]] panel there are links to [[ui:sidebar.cluster]] and [[ui:sidebar.topics]]. Each opens in its own tab.

## Cluster overview {#cluster-overview}

[[ui:cluster.overview.title]] shows the state of the cluster at the moment it was loaded. Click [[ui:cluster.overview.refresh]] to load it again.

- `Cluster ID` — the cluster identifier.
- [[ui:cluster.overview.controller]] — the ID of the broker that the cluster reports as the controller. In KRaft clusters, clients do not talk to the controllers directly: the cluster reports a random broker here, not the active controller, and it may change between refreshes.
- [[ui:cluster.overview.brokers]] — the number of brokers.
- [[ui:cluster.overview.topicsInternal]] — all topics, with the number of internal topics in brackets.
- [[ui:cluster.overview.partitions]] — the number of partitions in all topics.
- `Under-replicated` — partitions where not all replicas are in sync (in the ISR). Usually a broker is down or cannot keep up.
- `Offline` — partitions without a leader. Nobody can write to them or read from them.

A non-zero value in `Under-replicated` or `Offline` is highlighted.

Below is the broker table with `ID` and [[ui:cluster.overview.address]]. The broker from the [[ui:cluster.overview.controller]] field is marked [[ui:cluster.overview.controllerBadge]]. In KRaft clusters, this is the same random broker, not the active controller. Click a broker to see its configuration under the table. Broker configuration can only be viewed. Use [[ui:common.config.searchParam]] to find a parameter by name, and [[ui:common.config.onlyChanged]] to hide parameters with default values. A lock icon marks read-only parameters.

## Topic list {#topic-list}

[[ui:sidebar.topics]] lists the topics of the cluster. Click a row to open the topic in its own tab.

The columns are [[ui:topics.list.colName]], [[ui:topics.list.colPartitions]], `RF` (replication factor), [[ui:topics.list.colMessages]], `cleanup.policy` and [[ui:topics.list.colUnderRepl]] (the number of under-replicated partitions). The message count is approximate: it is the difference between the end and start offsets of the partitions. Compaction and transaction markers make the real number smaller. Message counts and `cleanup.policy` are loaded only for the rows you scroll to.

Toolbar controls:

- [[ui:topics.list.search]] — type part of a name to find topics; case does not matter. To search with a regular expression, put it between slashes, for example `/^orders\..*/`.
- [[ui:topics.list.internal]] — show internal topics, such as `__consumer_offsets`. They are marked [[ui:topics.list.internalBadge]].
- The auto-refresh list — [[ui:topics.list.autoRefreshOff]], [[ui:topics.list.every10]] or [[ui:topics.list.every30]].
- [[ui:topics.list.refresh]] — reload the list and the message counts now.

Next to the toolbar, the app shows how many topics match the search out of the total. On [[ui:common.readOnly]] connections, the [[ui:topics.list.create]] and [[ui:topics.list.select]] buttons are hidden.

## Create a topic {#create-topic}

1. In the topic list, click [[ui:topics.list.create]]. The [[ui:topics.create.title]] form opens above the list.
2. Enter a [[ui:topics.create.name]]. It can contain Latin letters, digits, `.`, `_` and `-`, up to 249 characters. The form warns if the name has both `.` and `_`: Kafka metric names of such topics may collide with other topics.
3. Set the number of [[ui:topics.create.partitions]]. Partitions are read in parallel, and order is guaranteed only within one partition. The number can be increased later, but never decreased.
4. Set the [[ui:topics.create.rf]] — how many copies of each partition the brokers keep. It cannot be more than the number of brokers, which the form shows under the field. The default is 3, or the number of brokers if there are fewer. `-1` uses the broker default.
5. Choose a [[ui:topics.create.preset]]:
   - [[ui:topics.create.presetDelete]] — old messages are deleted by time.
   - `compact` — only the latest value per key is kept.
   - `compact + delete` — both.
   - [[ui:topics.create.presetTest]] — messages are kept for one hour.
6. Optionally, add [[ui:topics.create.extraConfigs]], one per line, for example `retention.ms=86400000`. They override the preset. Lines that start with `#` are ignored.
7. Click [[ui:topics.create.submit]].

When the form is valid, it shows the equivalent `kafka-topics.sh` command. You can copy it, for example, to create the same topic in another cluster.

On a prod connection, you confirm the creation by typing the connection name. After the topic is created, its tab opens.

## Delete topics {#delete-topics}

To delete one topic, open it and click [[ui:topics.actions.deleteTopic]]. Type the topic name to confirm and click [[ui:topics.delete.confirm]]. The topic tab closes.

To delete several topics at once:

1. In the topic list, click [[ui:topics.list.select]]. Checkboxes appear next to the names.
2. Select the topics. Internal topics cannot be selected.
3. Click the delete button. It shows the number of selected topics.
4. Check the list of names, type their number to confirm and click [[ui:topics.delete.confirm]].

[[ui:topics.list.cancelSelect]] hides the checkboxes.

On a prod connection, both ways of deleting also ask you to type the connection name.

Deletion cannot be undone: the data is lost. If some topics are not deleted, for example because of missing rights, the app lists them with the reason. The other topics are deleted.

## Topic screen {#topic-details}

The topic tab shows the number of partitions, the replication factor and the approximate message count. The buttons at the top:

- [[ui:topics.details.refresh]] — reload the topic data.
- [[ui:topics.details.produce]] — open the [[ui:sidebar.producer]] with this topic. See [Producer](producer.md).
- [[ui:topics.details.consume]] — start a consumer session for this topic. See [Consumer sessions](consumer-sessions.md).

On [[ui:common.readOnly]] connections, both buttons are disabled.

The second row has actions that change the topic. It is hidden on read-only connections and for internal topics.

- [[ui:topics.actions.addPartitions]] — enter the new total and click [[ui:topics.actions.addSubmit]]. The number of partitions cannot be decreased. After an increase, messages with the same key may go to a different partition, so the order by key breaks.
- [[ui:topics.actions.purge]] — delete all messages up to the current end of the selected partitions. The topic itself stays. Select the partitions, type the topic name and click [[ui:topics.actions.purgeConfirm]]. Purge works only when `cleanup.policy` includes `delete`; otherwise the button is disabled and the reason is shown. For a `compact,delete` topic, purge also deletes the latest values of the keys.
- [[ui:topics.actions.deleteTopic]] — see [Delete topics](topics.md#delete-topics).

On a prod connection, each of these actions also asks for the connection name.

The tabs below:

- [[ui:topics.details.tabMessages]] — browse messages. See [Browsing messages](browse.md).
- [[ui:topics.details.tabPartitions]] — a table with each partition: [[ui:topics.details.colLeader]], [[ui:topics.details.colReplicas]], `ISR` (in-sync replicas), the start (`Low`) and end (`High`) offsets and the number of messages. A partition without a leader shows [[ui:topics.details.noLeader]].
- [[ui:topics.details.tabConfigs]] — topic parameters. See [Topic configuration](topics.md#topic-config).
- [[ui:topics.details.tabGroups]] — consumer groups that have committed offsets for this topic, with [[ui:topics.groups.colState]], [[ui:topics.groups.colPartitions]] and [[ui:topics.groups.colLag]]. Click a group to open it. See [Consumer groups](groups.md).

## Topic configuration {#topic-config}

The [[ui:topics.details.tabConfigs]] tab lists all topic parameters with three columns:

- [[ui:common.config.colParam]] — the parameter name. Names in bold are set explicitly: on the topic or in the broker configuration.
- [[ui:common.config.colValue]] — the current value. Sensitive values are hidden as `••••••`.
- [[ui:common.config.colSource]] — where the value comes from. For example, `DynamicTopic` is set on this topic, `StaticBroker` and `DynamicBroker` come from the broker configuration, and `Default` is the Kafka default.

Use [[ui:common.config.searchParam]] to find a parameter by name, and [[ui:common.config.onlyChanged]] to show only the parameters in bold. A lock icon marks read-only parameters. The `?` icon next to a name explains the parameter.

## Change the configuration {#edit-config}

1. On the [[ui:topics.details.tabConfigs]] tab, click [[ui:topics.details.edit]]. On [[ui:common.readOnly]] connections, this button is hidden.
2. Change values in the [[ui:common.config.colValue]] column. Read-only and sensitive parameters cannot be edited. To remove the topic's own value, click [[ui:common.config.reset]]: the broker value then applies. An empty value is not allowed.
3. Click [[ui:common.config.review]]. The app shows each change as old value → new value; [[ui:common.config.defaultValue]] means the value is reset.
4. Click [[ui:common.config.apply]], or [[ui:common.config.back]] to continue editing.

Nothing changes in the cluster until you apply. [[ui:common.config.discard]] cancels all edits. While there are unsaved edits, [[ui:topics.details.closeEditor]] is disabled. On a prod connection, you confirm the changes by typing the connection name.

Only the parameters you changed are sent. Other topic parameters stay as they are. The broker checks the values and rejects invalid ones with an error.

## Topic parameters {#config-parameters}

Kafka topics have dozens of parameters. The `?` icons in the configuration table explain each of them. Most often you need these:

- `cleanup.policy` — what happens to old data. `delete` removes old segments by time or size. `compact` keeps only the latest value per key. `compact,delete` does both.
- `retention.ms` — how long messages are kept, in milliseconds. The default is `604800000` (7 days); `-1` keeps them forever. Works when `cleanup.policy` includes `delete`.
- `retention.bytes` — the maximum size of each partition before old segments are deleted. The default `-1` means no size limit.
- `segment.ms` and `segment.bytes` — when the broker starts a new log segment. Retention and compaction work on whole segments, so a message may stay longer than `retention.ms` until its segment is closed.
- `min.insync.replicas` — how many replicas must confirm a write with `acks=all`. Usually 2 with replication factor 3.
- `max.message.bytes` — the largest batch the broker accepts for this topic. The default is about 1 MB. Producers must allow the same size (`max.request.size`).
- `compression.type` — how the broker stores messages. `producer` keeps the producer's compression.
- `message.timestamp.type` — `CreateTime` (the time set by the producer) or `LogAppendTime` (the time the broker wrote the message). This is the time shown when you browse messages.
- `delete.retention.ms` — how long delete markers (messages with a null value) stay in a compacted topic. The default is 1 day.

Some parameters exist only in newer Kafka versions, and some are deprecated. The `?` icon says so where it matters. Parameters for tiered storage, such as `remote.storage.enable`, work only if the cluster supports it.
