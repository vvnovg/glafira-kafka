//! Проверка кластера и замечания (спец. G2): снимок кластера, правила, находки.
//! Кластер здесь только читается — изменяющих вызовов Kafka нет и быть не должно.

pub mod collect;
pub mod kafka_source;
pub mod model;
pub mod rules;
pub mod service;
pub mod snapshot;
pub mod store;
pub mod texts;
