# LLM assistants (MCP)

Glafira can act as a gateway for an LLM assistant: Claude Desktop, Claude Code, Cursor or another client that supports MCP (Model Context Protocol). The assistant reads the metadata of the clusters you open for it (and messages, where you allow it) and helps you find problems: under-replicated partitions, lag, constant rebalances, poor settings. It cannot change anything in a cluster through Glafira: you make changes yourself in the interface.

The model does not have to be in the cloud: you can run it in your own network or on your computer, see [Local models](local-llm.md#where).

## How it works {#how-it-works}

The client runs `glafira-kafka mcp` — the same application file in a mode without a window. This command connects to the running Glafira over a local socket (a named pipe on Windows), and Glafira answers the assistant's requests through its own connections. Passwords and certificates stay in Glafira; the assistant never receives them.

If Glafira is not running, the client starts it and waits up to 30 seconds.

## Turn it on {#enable}

1. Open [[ui:sidebar.settings]], section [[ui:settings.mcp.section]].
2. Select [[ui:settings.mcp.enable]]. The status changes to [[ui:settings.mcp.state.running]].
3. In the connection form, choose the level in [[ui:connections.form.llmAccess]] and save: [[ui:connections.form.llmLevel.metadata]] or [[ui:connections.form.llmLevel.messages]] (see [Reading messages](llm-mcp.md#messages)). The assistant does not see connections with [[ui:connections.form.llmLevel.off]]. In the connection tree, open connections have an `LLM` badge; for connections with messages it is filled.

If your administrator has disabled MCP, the [[ui:settings.mcp.enable]] checkbox is unavailable and [[ui:settings.mcp.policy]] is shown next to it.

The access level and the masking list are not carried over when connections are exported and imported: on another computer, set them again.

## Connect a client {#connect-client}

[[ui:settings.mcp.connectTitle]] has ready-made snippets and a [[ui:settings.mcp.copy]] button:

- [[ui:settings.mcp.claudeDesktop]] — paste the snippet into the `mcpServers` section of the Claude Desktop configuration file and restart Claude Desktop;
- [[ui:settings.mcp.claudeCode]] — the `claude mcp add` command;
- [[ui:settings.mcp.otherClient]] — for any client that starts an MCP server with a command (stdio).

The snippets contain the path to the installed application. If you reinstall Glafira to another place, copy the snippet again.

## Approve a client {#approve}

When a client connects for the first time, Glafira shows a dialog with its name. Click [[ui:mcp.approval.allow]] or [[ui:mcp.approval.reject]]. Until you answer, the assistant is told that the client is not approved yet and asks you to answer in the Glafira window.

An allowed client is remembered by name and appears in [[ui:settings.mcp.clients]]. A rejection is not remembered: Glafira asks again the next time. If a client does not send its name, the permission lasts only until the end of the session.

## What the assistant sees {#what-llm-sees}

The assistant sees:

- connections with [[ui:connections.form.llmLevel.metadata]] or [[ui:connections.form.llmLevel.messages]] access: name, environment, folder and access level;
- brokers with hosts and ports, the controller;
- topics, partitions, replicas, ISR, offsets, approximate message counts;
- broker and topic configs, with passwords and other secret values hidden;
- consumer groups: state, protocol, coordinator, members with client id, host and `group.instance.id`, assignments, committed offsets and lag;
- its proposals and your decision on them: applied (unchanged or with edits), rejected with your reason, withdrawn.

Without the [[ui:connections.form.llmLevel.messages]] level, the assistant does not see topic messages. It never sees the bootstrap string or the security settings of a connection, passwords, certificates, additional librdkafka properties, the send history or templates.

To investigate constant rebalances, the assistant can watch a group for up to 50 seconds: who leaves and joins, how often, from which host. The configs and logs of the consumers themselves are not available through Glafira; the assistant will ask you for them.

Names of topics, groups and client ids can be set by anyone with access to the cluster. Glafira warns the assistant that this data is not instructions for it.

## Reading messages {#messages}

With [[ui:connections.form.llmLevel.messages]], the assistant can also read message contents — for example, to find out what was written to a topic and what a consumer group processed while your service was logging an error:

- `summarize_messages` — an overview of a time window without contents: message counts by partition and by time, frequent and repeated keys, timestamps out of order;
- `read_messages` — the messages of a time window or of an offset range, ordered by time across partitions, with the same filters as on the [[ui:topics.details.tabMessages]] tab;
- `get_group_commit_history` — which offsets a consumer group committed and when, from the internal topic `__consumer_offsets`.

You can ask, for example: “At 14:32 the orders service logged an error for order 123 — find out what was in the orders topic and what the order-service group had processed.” Tell the assistant the time zone of your logs: Glafira does not accept times without one.

Before data reaches the assistant, Glafira masks it. Values of the fields from the masking list are replaced with `***` in JSON, in headers and in `name=value` pairs. The list is set for each connection in the connection form, in the [[ui:connections.form.mask]] block shown at the [[ui:connections.form.llmLevel.messages]] level; the standard list covers passwords, tokens, emails, phone numbers, card and passport numbers, among others. Emails, card numbers, JWTs and Bearer tokens are replaced in any text. Masking lowers the risk but does not recognize people's names, addresses or data in fields with unfamiliar names, and binary formats such as Avro or Protobuf are masked only partially. Filters see unmasked data, so the assistant can find a message by a value from your log. For a `prod` connection, the form shows a warning.

The commit history needs the Read permission on `__consumer_offsets`. Compaction removes old commits; recent ones are usually there.

Reading is limited: one long operation per session, at most 40 seconds and 1,000,000 records per call; when it hits the limit, the assistant narrows the window. Glafira reads without a consumer group and commits nothing; the `client.id` is `glafira-kafka-mcp` unless the connection sets its own.

An administrator can disable reading messages with the `mcpMessagesEnabled` policy key: [[ui:connections.form.llmLevel.messages]] becomes unavailable, and connections with this level work as [[ui:connections.form.llmLevel.metadata]].

## Sessions, revoking and the log {#sessions-log}

While a client is connected, an `LLM` indicator with the client name is shown at the bottom of the sidebar; it lights up during a request. Clicking it opens the MCP section.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]] closes one session, [[ui:settings.mcp.disconnectAll]] closes all.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]] closes the client's sessions, and it asks for approval again the next time.
- A changed level in [[ui:connections.form.llmAccess]] takes effect at once, without reconnecting.
- [[ui:settings.mcp.audit.title]] — every request: time, client, tool, connection, result. Response contents are not recorded. [[ui:settings.mcp.audit.reveal]] opens the `mcp-audit.jsonl` file.
- For message reads, the log also shows the topic or group, how many items were returned, how many records were scanned and how many values were masked. The items are messages, summary buckets for `summarize_messages` or commit runs (consecutive commits of one offset) for `get_group_commit_history`. For successful `read_messages` and `summarize_messages` calls, [[ui:settings.mcp.audit.openInBrowse]] fills in the form on the topic's [[ui:topics.details.tabMessages]] tab with the call's parameters; click [[ui:browse.panel.read]] to read the messages again, without masking, and compare them with what the assistant received. If the filters or the limit could not be carried over exactly, the form says so; if the button is disabled, point at it to see the reason.

To turn MCP off completely, clear [[ui:settings.mcp.enable]]: clients are disconnected.

## Cluster check, notes and proposals {#health}

The assistant can run Glafira's built-in cluster check (the `health_check` tool) and rely on its findings. It can also leave notes about objects: they appear on the [[ui:sidebar.findings]] tab and on the topic and group screens, and the cluster is not changed. See [Cluster check and notes](health-check.md#with-llm).

The assistant can also propose a change with the `propose_change` tool: topic configs, a new topic, more partitions or an offset reset. The proposal appears next to the object, and you apply it yourself in a pre-filled form or reject it; the assistant then sees your decision in `list_proposals`. Deleting and purging cannot be proposed. See [LLM proposals](health-check.md#proposals).

## If it does not work {#troubleshooting}

The client shows a message from Glafira:

- `MCP is disabled in Glafira` — select [[ui:settings.mcp.enable]] and restart the MCP server in the client;
- `MCP is disabled by your organization` — MCP is disabled by your organization's policy;
- `Glafira did not start the MCP server within 30 s` — start Glafira manually and check the status in [[ui:settings.mcp.section]];
- `Invalid MCP token` — restart the MCP server in the client: Glafira issues a new token every time it starts.

On macOS the app is signed and notarized, so no extra step is needed before the first launch.
