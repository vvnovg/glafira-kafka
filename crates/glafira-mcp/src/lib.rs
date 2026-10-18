//! MCP-шлюз Glafira (спец. `docs/superpowers/specs/2026-10-18-glafira-mcp-gateway-g1-design.md`).
//!
//! Крейт ничего не знает о Kafka: данные он получает через трейт `Backend`, который
//! реализует приложение, и умеет только читать — изменяющих операций в `ReadOp` нет.

pub mod endpoint;
pub mod handshake;

pub use endpoint::{BoxStream, Endpoint};
