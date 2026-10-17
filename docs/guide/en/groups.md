# Consumer groups

A consumer group is a set of consumers that share the reading of topics. Kafka gives each partition to one member of the group and stores, for each partition, the offset the group has committed. Applications continue reading from that offset after a restart.

Open [[ui:sidebar.groups]] under the connection in the [[ui:sidebar.connections]] panel. The groups of one topic are also on the [[ui:topics.details.tabGroups]] tab of the topic. See [Topic screen](topics.md#topic-details).

## Group list {#group-list}

The list shows all consumer groups of the cluster. The columns:

- [[ui:groups.list.colGroup]] — the group ID (`group.id`).
- [[ui:groups.list.colState]] — the state reported by Kafka, for example `Stable` (members are reading), `Empty` (no members), `PreparingRebalance` or `CompletingRebalance` (partitions are being redistributed) and `Dead` (the group no longer exists).
- [[ui:groups.list.colMembers]] — the number of active members.
- `Assignor` — the strategy used to distribute partitions, for example `range`.
- [[ui:groups.list.colLag]] — the total lag of the group. See [Lag](groups.md#lag).
- [[ui:groups.list.colTopics]] — the topics the group has committed offsets for.

Type in [[ui:groups.list.search]] to show the groups whose ID contains the text; case does not matter. The state list next to it shows only groups in one state; [[ui:groups.list.allStates]] removes this filter. The counter shows how many groups are shown and how many there are in total.

Members, assignor and lag are loaded only for the rows you see, while you scroll. Until they arrive, the cell shows `…`. If they cannot be loaded, the cell shows `—`; point at it to see the reason.

[[ui:groups.list.refresh]] reloads the list and the data in it. If some brokers did not respond, a warning says the list may be incomplete.

## Lag {#lag}

Lag is how many messages the group has not read yet. For each partition, it is the end of the log minus the committed offset. Lag is never shown below zero.

Lag depends only on committed offsets. An active consumer may have read further than it has committed, so its real lag can be smaller. When the group commits again, the lag drops.

A partition without a committed offset shows [[ui:groups.lag.noCommit]] and does not count in the total. The total lag of a group shows `—` when the group has no commits at all ([[ui:groups.lag.noCommits]]), or when the offsets of some partition could not be read ([[ui:groups.lag.unknown]]).

Lag counts offsets, not messages. In topics with compaction or transactions, some offsets hold no visible messages, so the group may have fewer messages left to read than the lag shows.

The values are a snapshot at the moment of loading. To follow the lag, open the group and turn on auto-refresh.

## Group details {#group-details}

Click a group in the list to open its tab. At the top, a summary shows the state, the number of members and the assignor.

- [[ui:groups.details.autoRefresh]] — [[ui:groups.details.refreshOff]], [[ui:groups.details.refresh5]] or [[ui:groups.details.refresh10]].
- [[ui:groups.details.refresh]] — reload now.
- [[ui:groups.details.join]] — start a [consumer session](consumer-sessions.md) in this group. The session form gets the group ID and the topics the group has commits for. This needs Pro, and the button is disabled on [[ui:common.readOnly]] connections.

[[ui:groups.details.partitions]] lists the partitions the group has commits for, and the partitions assigned to its members. The total lag of the group is shown above the table. The columns are [[ui:groups.details.colTopic]], [[ui:groups.details.colPartition]], `Committed` (the committed offset), `Log end` (the end offset of the partition), [[ui:groups.details.colLag]] and [[ui:groups.details.colMember]] (the client ID of the member that reads the partition).

[[ui:groups.details.members]] lists the active members: `Client ID`, [[ui:groups.details.colHost]], `Member ID` and [[ui:groups.details.colAssignment]] (the partitions of the member as `topic:partition`). A group without members shows [[ui:groups.details.noMembers]].

The second row has [[ui:groups.details.resetOffsets]] and [[ui:groups.details.deleteGroup]]. It is hidden on read-only connections.

## Reset offsets {#reset-offsets}

Resetting offsets moves the committed offsets of a group. The group then rereads messages or skips them. Offsets can be reset only for a group without active members, in the `Empty` state. Otherwise the members would overwrite the new offsets with their own commits. Stop the applications of the group first.

1. On the group tab, click [[ui:groups.details.resetOffsets]]. While the group has members, the button is disabled and the reason is shown.
2. Choose the [[ui:groups.reset.strategy]]. See [Reset strategies](groups.md#reset-strategies).
3. Choose the [[ui:groups.reset.scope]]. See [Reset scope](groups.md#reset-scope).
4. Click [[ui:groups.reset.preview]].
5. Check the table and click the apply button. It shows the number of partitions that will change.

The preview changes nothing. For each partition it shows the topic, the partition, [[ui:groups.reset.colCurrent]] (the committed offset), [[ui:groups.reset.colNew]] (the target offset), and `Low / High` (the first and end offsets of the partition). A partition for which no target can be computed is marked [[ui:groups.reset.noTarget]], with the reason. Such partitions are skipped.

Only the plan shown in the preview is applied. Any change in the form hides the preview, and you need to click [[ui:groups.reset.preview]] again. On a prod connection, applying asks for the connection name; on other connections, it applies at once.

Before applying, the app checks again that the group has no members. After applying, [[ui:groups.reset.result]] lists each partition with its new offset or an error.

You can also prepare a reset from a message: see [Reset a group to a message](browse.md#reset-group-here). [[ui:common.close]] closes the reset panel.

## Reset strategies {#reset-strategies}

[[ui:groups.reset.strategy]] sets the target offset for each partition in the scope:

- [[ui:groups.reset.strategyEarliest]] — the first offset that is still stored. The group rereads everything the topic keeps.
- [[ui:groups.reset.strategyLatest]] — the end of the partition. The group skips everything accumulated and reads only new messages.
- [[ui:groups.reset.strategyTimestamp]] — the first message written at or after [[ui:groups.reset.timestamp]]. In partitions without newer messages, the target is the end.
- [[ui:groups.reset.strategyOffset]] — the given [[ui:groups.reset.offset]], an integer of 0 or more, in every partition. If a partition does not have this offset, the target is its first or end offset.
- [[ui:groups.reset.strategyShift]] — moves the committed offset by [[ui:groups.reset.shift]]. A negative number rereads N messages back, a positive one skips N messages. The result stays within the first and end offsets. Partitions without a committed offset cannot be shifted and are skipped.

The time is your local time, with seconds. Kafka finds the offset by message timestamps.

## Reset scope {#reset-scope}

[[ui:groups.reset.scope]] sets which partitions are reset. Other partitions keep their offsets.

- [[ui:groups.reset.scopeAll]] — all partitions the group has committed offsets for. If the group has no commits yet, choose topics instead.
- [[ui:groups.reset.scopeTopics]] — all partitions of the listed topics, including partitions without commits. Separate the names with commas, spaces or line breaks. Use this to make a group start reading a new topic from a chosen place.
- [[ui:groups.reset.scopePartitions]] — only the listed partitions. Enter them in [[ui:groups.reset.partitionsLabel]] as `topic:partition`, separated by commas or spaces, for example `orders:0, orders:3`.

## Delete a group {#delete-group}

Deleting a group removes all its committed offsets. Only a group without members, in the `Empty` state, can be deleted.

1. On the group tab, click [[ui:groups.details.deleteGroup]]. While the group has members, the button is disabled.
2. Type the group ID to confirm. On a prod connection, also type the connection name.
3. Click [[ui:common.delete]].

After deleting, the group tab closes. If an application with this `group.id` starts again, Kafka creates the group anew. Without committed offsets, the application starts where its `auto.offset.reset` setting says: from the beginning or from the end.

On [[ui:common.readOnly]] connections, the button is hidden.
