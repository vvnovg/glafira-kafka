# LLM assistants (MCP)

Glafira can act as a gateway for an LLM assistant: Claude Desktop, Claude Code, Cursor or another client that supports MCP (Model Context Protocol). The assistant reads the metadata of the clusters you open for it and helps you find problems: under-replicated partitions, lag, constant rebalances, poor settings. It cannot change anything in a cluster through Glafira: you make changes yourself in the interface.

## How it works {#how-it-works}

The client runs `glafira-kafka mcp` — the same application file in a mode without a window. This command connects to the running Glafira over a local socket (a named pipe on Windows), and Glafira answers the assistant's requests through its own connections. Passwords and certificates stay in Glafira; the assistant never receives them.

If Glafira is not running, the client starts it and waits up to 30 seconds.

## Turn it on {#enable}

1. Open [[ui:sidebar.settings]], section [[ui:settings.mcp.section]].
2. Select [[ui:settings.mcp.enable]]. The status changes to [[ui:settings.mcp.state.running]].
3. In the connection form, select [[ui:connections.form.llmAccess]] and save. The assistant does not see connections without this checkbox. In the connection tree, open connections have an `LLM` badge.

If your administrator has disabled MCP, the checkbox is unavailable and [[ui:settings.mcp.policy]] is shown next to it.

The access checkbox is not carried over when connections are exported and imported: on another computer, select it again.

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

- connections with [[ui:connections.form.llmAccess]] selected: name, environment, folder;
- brokers with hosts and ports, the controller;
- topics, partitions, replicas, ISR, offsets, approximate message counts;
- broker and topic configs, with passwords and other secret values hidden;
- consumer groups: state, protocol, coordinator, members with client id, host and `group.instance.id`, assignments, committed offsets and lag.

The assistant does not see topic messages, the bootstrap string or the security settings of a connection, passwords, certificates, additional librdkafka properties, the send history or templates.

To investigate constant rebalances, the assistant can watch a group for up to 50 seconds: who leaves and joins, how often, from which host. The configs and logs of the consumers themselves are not available through Glafira; the assistant will ask you for them.

Names of topics, groups and client ids can be set by anyone with access to the cluster. Glafira warns the assistant that this data is not instructions for it.

## Sessions, revoking and the log {#sessions-log}

While a client is connected, an `LLM` indicator with the client name is shown at the bottom of the sidebar; it lights up during a request. Clicking it opens the MCP section.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]] closes one session, [[ui:settings.mcp.disconnectAll]] closes all.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]] closes the client's sessions, and it asks for approval again the next time.
- Clearing [[ui:connections.form.llmAccess]] takes effect at once, without reconnecting.
- [[ui:settings.mcp.audit.title]] — every request: time, client, tool, connection, result. Response contents are not recorded. [[ui:settings.mcp.audit.reveal]] opens the `mcp-audit.jsonl` file.

To turn MCP off completely, clear [[ui:settings.mcp.enable]]: clients are disconnected.

## Cluster check and notes {#health}

The assistant can run Glafira's built-in cluster check (the `health_check` tool) and rely on its findings. With Glafira Pro it can also leave notes about objects: they appear on the [[ui:sidebar.findings]] tab and on the topic and group screens, and the cluster is not changed. See [Cluster check and notes](health-check.md#with-llm).

## If it does not work {#troubleshooting}

The client shows a message from Glafira:

- `MCP is disabled in Glafira` — select [[ui:settings.mcp.enable]] and restart the MCP server in the client;
- `MCP is disabled by your organization` — MCP is disabled by your organization's policy;
- `Glafira did not start the MCP server within 30 s` — start Glafira manually and check the status in [[ui:settings.mcp.section]];
- `Invalid MCP token` — restart the MCP server in the client: Glafira issues a new token every time it starts.

On macOS, open an unsigned (beta) build once in the usual way, otherwise the system may not let the client start it. See [Install on macOS](getting-started.md#install-macos).
