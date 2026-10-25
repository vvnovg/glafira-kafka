//! MCP-шлюз Glafira (спец. `docs/superpowers/specs/2026-10-18-glafira-mcp-gateway-g1-design.md`).
//!
//! Крейт ничего не знает о Kafka: данные он получает через трейт [`Backend`], который
//! реализует приложение, и умеет только читать — изменяющих операций в [`ReadOp`] нет.
//! Замечания LLM (G2, [`health`]) и предложения изменений (G3, [`proposals`]) пишутся в Glafira,
//! а не в кластер: предложение применяет человек формой приложения.

pub mod audit;
pub mod catalog;
pub mod commits;
pub mod endpoint;
pub mod handshake;
pub mod health;
pub mod limits;
pub mod mask;
pub mod messages;
pub mod model;
pub mod observe;
pub mod proposals;
pub mod rules;
pub mod server;
pub mod summary;
pub mod topn;

pub use audit::{AuditEvent, AuditLog};
pub use endpoint::{BoxStream, Endpoint};
pub use model::{
    Backend, BackendError, BoxFuture, ClientIdent, ReadOp, ReadOutput, SessionAccess, SessionView,
};
pub use server::{GatewayOptions, McpServerHandle, SessionSnapshot, serve_stream, start};
