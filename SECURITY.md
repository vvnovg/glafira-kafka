# Security policy

## Supported versions

Only the latest release on https://github.com/vvnovg/glafira-kafka-releases/releases receives fixes.

## Reporting a vulnerability

Please do not open a public issue. Use GitHub's private vulnerability reporting: on the repository page open
the **Security** tab and click **Report a vulnerability**. Describe the problem, the app version and the steps
to reproduce it.

You will get a reply within 7 days. When a fix is released, the report is published as a security advisory with
credit to the reporter, unless you ask otherwise.

## What matters most

Glafira connects to Kafka clusters with the user's credentials and exposes read-only metadata to LLM assistants
over MCP. Reports about credential handling (system secret store, logs, diagnostics archive), the administrator
policy, the MCP gateway and the updater are especially welcome.
