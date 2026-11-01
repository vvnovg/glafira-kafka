# Changelog

Versions in the other interface languages are in [docs/changelog](docs/changelog/). The section of a version is shown in About as the update notes, in the interface language.

## 0.5.3
- TLS errors: when the broker closes the connection during the TLS handshake, the message no longer claims that the port has no TLS. It says that the port may have no TLS or that the client certificate was not accepted, which is the usual case with mTLS.
- MCP works better with local models (Qwen, gpt-oss and others):
  - tool parameter schemas no longer contain `$ref` references, which some servers (vLLM with the `qwen3_coder` parser) passed to tools as strings;
  - a nested object sent as a JSON string is accepted, and `null` values and empty keys are ignored; if the arguments still do not fit, the error shows the expected arguments;
  - when a time window has no messages, `read_messages` suggests checking the time zone and widening the window; tool descriptions explain how to pass a time with a time zone and how to filter by one exact key.
- “Settings” → “LLM assistants (MCP)”:
  - “Response size limit”: 100 KB (default), 48 KB or 24 KB — choose less for a local model with a small context;
  - “Compact tools” for each approved client: no tools for notes and change proposals, which saves about 1,000 tokens of context.
- Guide: a new chapter, “Local models”: where the model can run, results of Qwen3.6 and Qwen3.8 on a Mac, hardware, Ollama, setting up Qwen Code and Claude Code, and how to check the result.

## 0.5.2
- More thorough masking of messages read by an LLM assistant:
  - in text, the values of cookies and `Authorization` headers (including `Proxy-Authorization`) are masked to the end of the line, so all cookies, Digest parameters and AWS signatures are hidden;
  - in `name=value` pairs, names in any alphabet are recognized, for example a field name from the connection’s own list written in Cyrillic;
  - card numbers glued to letters (`id4111…`) or written in unusual groupings are found, as are negative and fractional card numbers in JSON.
- A topic without the Describe permission: the “Messages” tab and the `read_messages`, `summarize_messages` and `get_group_commit_history` tools report “Not authorized to perform the operation” instead of “not found”.
- `get_group_commit_history`: if the commit history does not fit into 100 KB even without commit runs, the assistant gets a “response too large” error with advice to narrow the request instead of an empty history. `summarize_messages` uses less memory with large keys.
- Connection form:
  - when two connection forms were open, the LLM access level selected in one of them was not shown, although it was saved correctly; now each form shows its own;
  - screen readers announce the warning that the message contents of a production cluster will be sent to the LLM provider;
  - if the MCP status could not be loaded, a note explains why “Metadata and messages” cannot be selected;
  - a masking list hidden at the “Off” and “Metadata only” levels no longer blocks saving.
- `LLM` badges in the connection tree: screen readers read them in full, for example “LLM access: metadata and messages”, instead of “LLM”; the text has higher contrast.
- MCP log: the “Open in Messages” button explains that it is unavailable when the list of connections could not be loaded.

## 0.5.1
- Update notes in “About” → “Updates” are shown in the interface language (in English if there is no translation) and as formatted text instead of raw Markdown. This applies to updates offered to version 0.5.1 and later; earlier versions show the notes as plain English text.
- The changelog is now in English (`CHANGELOG.md`); its versions in the other interface languages are in `docs/changelog/`.

## 0.5.0
- Reading messages by an LLM assistant. A connection has three levels of LLM access: “Off”, “Metadata only”, “Metadata and messages”. At the third level, the assistant can use MCP to:
  - read messages within a time window or by offsets, with the same filters as on the “Messages” tab (`read_messages`);
  - get an overview of a window without the content: volume by partition and over time, frequent keys, out-of-order timestamps (`summarize_messages`);
  - see which offsets a consumer group committed and when, from `__consumer_offsets` (`get_group_commit_history`).
- Times are passed only with a time zone. A single call runs for at most 40 seconds, scans at most a million records and returns at most 100 KB.
- Masking before data reaches the assistant:
  - values of fields from a list of names — the standard one or the connection’s own — are replaced with `***`;
  - emails, card numbers, JWTs and authorization tokens are masked in any text;
  - filters work on unmasked data, so a message can be found by a value from a log.
- The access level and the masking list are not exported or imported.
- The `mcpMessagesEnabled` policy key forbids reading messages: the “Metadata and messages” level then works as “Metadata only”.
- MCP log: for message reads, it shows the topic or group, how much was returned out of what was scanned, and how many values were masked. The “Open in Messages” button fills in the form on the “Messages” screen with the same window and filters; if they cannot be transferred exactly, the form says so.
- MCP: clients of the MCP 2026-07-28 protocol are supported (for example, Claude Code 2.1.310): the client name and approval come right at connection. A pending session has an “Approve” button. All tools are explicitly marked as read-only (`idempotentHint`).
- The `glafira-mcp-catalog` catalog server for MCP registries (Glama): a Docker image, the real list of tools, and calls answer that the desktop app is required. The README is in English (Russian — `README.ru.md`).
- Offset reset proposed by an LLM: group membership is checked again before writing. If the broker refused because members joined the group in the meantime, a clear “group is not empty” error is shown, and the reset form immediately shows the current state of the group.
- Interface:
  - placeholders in empty fields are in italics, so they are not mistaken for entered values;
  - the start position and partition fields on the “Messages” screen have tooltips on hover;
  - the environment and LLM badges in the connection tree show the regular arrow pointer.
- Guide: the “Reading messages” section in the “LLM assistants (MCP)” chapter, in 12 languages.

## 0.4.0
- LLM proposals: an assistant can use MCP to propose a change — topic configs, creating a topic, adding partitions or resetting group offsets (the `propose_change`, `list_proposals`, `withdraw_proposal` tools). A proposal is validated against the cluster, stored next to the notes and shown in the “LLM proposals” section of the “Findings” tab and in the topic and group blocks. Nothing is applied by itself: the “Open in form” button opens a filled-in Glafira form, and confirmation works as for a regular change (on prod, typing the connection name). After success, the status “applied” or “applied with edits” is set automatically; with edits, the card shows what was actually applied, and the assistant sees it in `list_proposals`. A proposal can be rejected with a reason, reopened or deleted; an assistant can withdraw its own. Read-only connections receive proposals without the option to apply them. The limit is 50 open proposals per connection; they are kept for 7 days.
- Cluster check: a single “Checking…” state on all tabs of a connection; a “checked as PROD” label if the connection’s environment was changed after the check; a new “Controller unknown” rule (warning) instead of “No controller” when leaders are alive; check files for imported connections with non-standard IDs.
- “Findings” tab: recently closed notes and proposals are at the top; the topic configuration editor highlights changed lines.
- Guide: the “LLM proposals” section in the “Cluster check and notes” chapter, an updated chapter about assistants; installation on macOS is described for signed and notarized builds (the quarantine steps are removed).
- Texts: the “Purge is not possible” explanation names the allowed `cleanup.policy` values in quotes.

## 0.3.0
- All features are free: consumer sessions, prod connections and LLM notes are available without a key; the “License” screen and license keys are removed.
- The source code is open under the Apache License 2.0. “Support the project” is at the bottom of the left panel, in About and in the Help menu (macOS); About has a “License” row.
- Policy: the `licenseKey` key is no longer used and is ignored.
- Guide: the “License and updates” chapter became “Updates and diagnostics”.
- The interface in 12 languages: en, ru, de, fr, es, it, pl, pt-BR, zh-CN, zh-TW, ja, ko. Choose in “Settings” → “Interface language”; for administrators — the `language` policy key.
- “?” hints in the connection, topic, message browsing, Producer, offset reset and consumer session forms, and for every topic configuration parameter.
- Built-in guide: the “Help” tab with search, the F1 key, the Help menu on macOS. The guide is also published in the releases repository (`guide/`).
- Connection folders: nested folders in the left panel, drag and drop, a tree filter. Export of connections to a file without passwords and import with a preview.
- MCP gateway for LLM assistants: the “LLM assistants (MCP)” section in “Settings”, approval of every client, the “LLM access (metadata only)” checkbox of a connection, read-only tools (connections, topics, configuration, consumer groups and group watching), the `mcp-audit.jsonl` call log. The `mcpEnabled` policy key.
- Cluster check: the “Findings” tab of a connection — rules for brokers, topics, configuration and consumer groups with severity by environment, hiding findings, badges in the tree and on object screens. LLM assistant notes via MCP (`health_check`, `annotate`, `list_annotations`, `remove_annotation`).
- Consumer group description: the coordinator, the protocol and members’ `group.instance.id`.
- Producer: the key is not set by default (null) rather than an empty string.
- About: a short description of the app; a new app icon.
- Rolling back to 0.2.x keeps connections, but the folder layout is lost the first time the older version writes `connections.json`.

## 0.2.0
- Client certificates (mTLS): PEM, PKCS#12, JKS; passwords in the system secret store.
- Free / Pro licenses: consumer sessions and the prod mode are in Pro.
- Enterprise policy (MDM, group policies, /etc): updates, mirror address, organization key.
- Auto-update, logging to a file, diagnostics archive, About.
- Installers: macOS (.dmg, .pkg), Windows (.msi), Linux (.deb, .rpm, .AppImage).
