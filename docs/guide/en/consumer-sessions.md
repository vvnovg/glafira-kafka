# Consumer sessions

A consumer session reads topics as a real member of a consumer group.

## What a session is {#what-is}

[Browsing](browse.md) reads messages without joining a group and changes nothing in the cluster. A session is different: it joins a group with the given `group.id`, gets partitions from Kafka and can commit offsets. Use it to read what a group would read, to move the offsets of a group by committing, or to watch how a group rebalances.

A session affects the cluster. When it joins or leaves, Kafka redistributes the partitions of the group, and the other members go through a rebalance. Commits change the offsets of the group, which its applications will use.

Sessions are not available on [[ui:common.readOnly]] connections: the tab shows [[ui:consumer.readOnlyBlocked]]. If a connection becomes read-only while a session runs, the session leaves the group.

## Start a session {#start-session}

Open a session in one of three ways:

- [[ui:sidebar.consumer]] under the connection in the [[ui:sidebar.connections]] panel — an empty form;
- [[ui:topics.details.consume]] on a topic tab — the topic is filled in;
- [[ui:groups.details.join]] on a group tab — the group and the topics it has commits for are filled in.

Each time, a new tab opens, so several sessions can run at once.

In [[ui:consumer.form.groupTopics]]:

- `group.id` — the group to join. If you do not enter one, the app suggests a new name such as `glafira-kafka-anna-1a2b3c`. A new group has no commits.
- [[ui:consumer.form.topicsLabel]] — topic names separated by commas or spaces. Or select [[ui:consumer.form.useRegex]] and enter a [[ui:consumer.form.patternLabel]], for example `orders\..*`. The expression is matched from the start of the name: `^` is added if it is missing. Topics that match and are created later are added within a few minutes (on the metadata refresh); this causes a rebalance.
- `auto.offset.reset` — where to start in partitions for which the group has no committed offset: [[ui:consumer.form.resetLatest]] (the default) or [[ui:consumer.form.resetEarliest]]. Where the group has a committed offset, reading continues from it.
- [[ui:consumer.form.autoCommit]] — commit read offsets automatically. Off by default. See [Commits](consumer-sessions.md#commits).
- `isolation.level` — `read_committed` (the default) shows only messages of committed transactions. `read_uncommitted` also shows messages of open and aborted transactions.

In [[ui:consumer.form.formatLegend]], choose the [[ui:consumer.form.keyFormat]] and the [[ui:consumer.form.valueFormat]], as in browsing. See [Formats](browse.md#formats). [[ui:consumer.form.buffer]] sets how many messages the table keeps, from 1 to 100000; the default is 5000.

The filters work as in browsing. See [Filters](browse.md#filters).

Click [[ui:consumer.actions.connect]]. On a prod connection, the app asks you to type the connection name and confirm with [[ui:consumer.dialog.confirm]]. While the session runs, the form cannot be changed.

## Reading {#reading}

The state line next to the buttons shows [[ui:consumer.status.connecting]], then [[ui:consumer.status.inGroup]]. It also shows how many messages were read, how many matched the filters and how many are in the table. If the connection to the broker is lost, the session reconnects by itself and shows this in the state line.

Messages appear in the table as they arrive, and the session keeps reading new messages until you stop it. When the table is full, the oldest messages are removed. Click a row to see the message details, as in browsing. See [Message details](browse.md#message-details). Select `UTC` to show times in UTC.

The session reads only the partitions Kafka has assigned to it. If the group has other members, they read the other partitions. If the group has more members than partitions, the session may get no partitions at all.

[[ui:consumer.disconnect]] leaves the group, and the other members get its partitions. Closing the session tab also leaves the group. When the session has ended, the state line shows [[ui:consumer.status.disconnected]].

If a topic does not exist, the session stops with an error and shows the names to check.

## Commits {#commits}

A commit saves an offset for the group. The next member that reads the partition, or the same application after a restart, starts from that offset.

With [[ui:consumer.form.autoCommit]] on, the session commits read offsets in the background every few seconds, and once more when it leaves the group. Messages hidden by filters count as read.

Without auto-commit, the session commits nothing by itself. Use the buttons:

- [[ui:consumer.actions.commitSelected]] — commits the offset after the selected message, in its partition only. The group will continue from the next message. If the selected message is older than the current commit, the offset moves back.
- [[ui:consumer.actions.commitAll]] — commits the current position in every assigned partition. This includes messages hidden by filters and messages already removed from the table.

After a commit, a line shows the committed offsets as `topic:partition → offset`. On a prod connection, each commit asks for the connection name.

If the session does not answer in time, the app warns that it is unknown whether the commit was applied. Check the offsets on the group tab. See [Group details](groups.md#group-details).

## Pause partitions {#pause-partitions}

The panel to the right of the table lists the partitions assigned to the session as `topic:partition`. Click [[ui:consumer.assignment.pause]] next to a partition to stop reading it, and [[ui:consumer.assignment.resume]] to continue. A paused partition is marked [[ui:consumer.assignment.paused]].

Pausing does not leave the group, so there is no rebalance. The partition stays assigned to the session, and other members do not read it while it is paused. Pausing changes nothing in the cluster and needs no confirmation.

After a rebalance, newly assigned partitions start unpaused. When the session leaves the group, all pauses end.

## Rebalance log {#rebalance-log}

[[ui:consumer.assignment.logTitle]] under the partition list records every change of the assignment, newest first. Each entry has a time and one of three kinds: partitions assigned to the session, partitions revoked from it, or a rebalance error with its text.

A rebalance happens when a member joins or leaves the group, when the subscribed topics change, or when a member stops responding in time. To watch it, start a second session with the same `group.id` in another tab: the partitions are split between the two sessions, and both logs show it.

The log keeps the last 200 entries. Before the first rebalance, it shows [[ui:consumer.assignment.logEmpty]].

## Limits {#limits}

- Sessions are not available on [[ui:common.readOnly]] connections.
- A session cannot start from a chosen offset. To set the start, reset the offsets of the group first: see [Reset offsets](groups.md#reset-offsets). The reset needs a group without members, so disconnect sessions of this group before resetting.
- The table keeps at most 100000 messages. Older messages are removed, but they still count as read.
- Very large keys and values are shown only partly, as in browsing.
- Do not join the group of a running application unless you mean to. The session takes partitions from the application, and the application does not process them while the session holds them. Commits from the session, including auto-commits, move the offsets of the application.
