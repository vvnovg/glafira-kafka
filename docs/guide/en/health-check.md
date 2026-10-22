# Cluster check and notes

Glafira checks a cluster with a set of built-in rules and shows the problems it finds next to the objects: in the connection tree, in the topic and group lists, and on the topic and group screens. An LLM assistant can add its own notes to these findings. The check only reads the cluster and changes nothing in it.

## Running a check {#run}

Open [[ui:sidebar.cluster]] or [[ui:sidebar.findings]] for a connection and click [[ui:health.check]]. A check takes up to 45 seconds: Glafira reads the brokers, partitions, topic configs (up to 2000 topics) and consumer groups (up to 500). If there are more topics or groups than that, something cannot be read, or the 45 seconds run out, the result is marked [[ui:health.partial]], and the [[ui:health.ruleNames.check.partial]] finding shows what was not read.

Glafira does not poll the cluster on its own: a check runs only when you click the button or an LLM assistant asks for it. The result is kept until the next check.

## What is checked {#rules}

- Brokers and partitions: no controller, offline and under-replicated partitions, a single-broker cluster, leader skew, leaders not on the preferred replica.
- Topic durability: replication factor 1, `min.insync.replicas` above the replication factor, `min.insync.replicas` equal to the replication factor, `min.insync.replicas` equal to 1, unclean leader election.
- Broker configs: automatic topic creation, default replication factor 1, default `min.insync.replicas` 1, important settings that differ between brokers.
- Consumer groups: empty groups with lag, rebalancing groups, members without partitions, lag that grew since the previous check.

## Severity and environment {#severity}

Each finding has a severity: [[ui:health.severity.critical]], [[ui:health.severity.warning]] or [[ui:health.severity.info]]. It depends on the environment of the connection. For example, replication factor 1 is critical on prod, a warning on stage, and is not shown on dev, test and local. The environment is set in the connection form.

## The Findings tab {#tab}

On the [[ui:sidebar.findings]] tab, findings are grouped by rule; each one explains the problem and what to do. Filter them by [[ui:health.filterSeverity]], [[ui:health.filterSource]] and [[ui:health.filterKind]], or search by name. The object name opens its screen.

A badge with a number in the connection tree and in the topic and group lists shows how many findings and open LLM notes with the severity [[ui:health.severity.critical]] or [[ui:health.severity.warning]] concern the object. Point at the badge to see their titles.

## Hiding findings {#hide}

If a finding is expected for a specific object, for example a test topic with replication factor 1 on prod, click [[ui:health.hide]]. A hidden finding is not counted in badges and stays hidden after later checks. To see hidden findings, turn on "Show hidden"; there you can click [[ui:health.restore]].

## LLM notes {#notes}

An LLM assistant connected over MCP (see [LLM assistants (MCP)](llm-mcp.md#how-it-works)) can leave a note about the cluster, a broker, a topic or a group. Notes appear in [[ui:health.notes]] and on the object screens, signed with the client name. The text is shown as is, without formatting or links.

- [[ui:health.resolve]] and [[ui:health.dismiss]] close a note; closed notes are kept in a collapsed section.
- [[ui:common.delete]] removes a note, and [[ui:health.deleteAll]] removes every note of the connection.
- A note is removed automatically 7 days after it was created. A connection can have at most 200 open notes.

## LLM proposals {#proposals}

Besides notes, an LLM assistant can propose a change: topic configs, a new topic, more partitions or an offset reset of an empty consumer group. A proposal appears in [[ui:health.proposals.title]] on the [[ui:sidebar.findings]] tab and on the topic or group screen, with the assistant's rationale, a description of the change and the client name. Nothing is applied by itself, and deleting or purging cannot be proposed at all.

- [[ui:health.proposals.openForm]] opens the object's tab with the usual form already filled in: the config editor with its before/after review, the create-topic form, the partitions dialog or the offset reset with a preview. Edit the values if you like and confirm as usual; on a prod connection Glafira asks for the connection name. After a successful change the proposal is marked as applied (with edits, if you changed something), and the assistant sees that in `list_proposals`.
- [[ui:health.proposals.reject]] closes a proposal; you may add a reason, and the assistant will see it. [[ui:health.reopen]] brings a rejected proposal back.
- On a read-only connection the proposal is kept, but [[ui:health.proposals.openForm]] is unavailable until you clear read-only in the connection form; the assistant is told about that too.
- [[ui:common.delete]] removes a proposal, [[ui:health.proposals.deleteAll]] removes every proposal of the connection. A proposal is removed automatically 7 days after it was created; a connection can have at most 50 open proposals.

## With an LLM assistant {#with-llm}

The assistant runs the same check with the `health_check` tool and sees the same findings, including hidden ones, which are marked. If the last check is less than 5 minutes old, the assistant gets its result without querying the cluster again. Ask, for example: "Check the health of the cluster and leave notes on the most important problems."
