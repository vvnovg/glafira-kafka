//! MCP-шлюз для LLM, этап G1 «только чтение» (спец.
//! `docs/superpowers/specs/2026-10-18-glafira-mcp-gateway-g1-design.md`). Протокол, сессии,
//! инструменты и лимиты — в крейте `glafira-mcp`; здесь — связь с приложением.

pub mod approvals;
pub mod backend;
pub mod files;
