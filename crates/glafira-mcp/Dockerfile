# Catalog build of the Glafira MCP server for MCP registries (Glama and others).
# It speaks MCP over stdio and lists the real tools, but has no Kafka connections:
# every tool call answers `desktop_app_required` with a link to the installers.
# Real use: install Glafira for Apache Kafka and run `glafira-kafka mcp`.
#
# Build from the repository root:
#   docker build -f crates/glafira-mcp/Dockerfile -t glafira-mcp-catalog .
FROM rust:1.99-slim-bookworm AS build
WORKDIR /src
COPY crates/glafira-mcp/ ./
RUN cargo build --release --locked --bin glafira-mcp-catalog

FROM debian:bookworm-slim
COPY --from=build /src/target/release/glafira-mcp-catalog /usr/local/bin/glafira-mcp-catalog
USER nobody
ENTRYPOINT ["/usr/local/bin/glafira-mcp-catalog"]
