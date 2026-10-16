use serde::Serialize;

use crate::browse::format::DecodedData;
use crate::error::AppError;
use crate::i18n::LocalizedText;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HeaderView {
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowsedMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<DecodedData>,
    pub value: Option<DecodedData>,
    pub headers: Vec<HeaderView>,
    pub size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FinishReason {
    Limit,
    EndOfTopic,
    Cancelled,
}

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum BrowseEvent {
    Messages {
        messages: Vec<BrowsedMessage>,
    },
    Progress {
        scanned: u64,
        matched: u64,
    },
    /// Только в live-режиме: все партиции дочитаны до конца, дальше идут новые сообщения.
    ReachedEnd,
    /// Временная потеря связи с брокером: librdkafka переподключается (спец. §8).
    Reconnecting {
        message: LocalizedText,
    },
    /// Связь восстановлена после Reconnecting.
    Recovered,
    Finished {
        scanned: u64,
        matched: u64,
        reason: FinishReason,
    },
    Error {
        error: AppError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_serialize_with_type_tag() {
        let v = serde_json::to_value(BrowseEvent::Finished {
            scanned: 3,
            matched: 1,
            reason: FinishReason::EndOfTopic,
        })
        .unwrap();
        assert_eq!(
            v,
            serde_json::json!({"type": "finished", "scanned": 3, "matched": 1, "reason": "endOfTopic"})
        );
        assert_eq!(
            serde_json::to_value(BrowseEvent::ReachedEnd).unwrap(),
            serde_json::json!({"type": "reachedEnd"})
        );
        assert_eq!(
            serde_json::to_value(BrowseEvent::Reconnecting {
                message: LocalizedText::raw("down")
            })
            .unwrap(),
            serde_json::json!({
                "type": "reconnecting",
                "message": {"key": "errors.generic", "params": {"message": "down"}, "text": "down"}
            })
        );
        assert_eq!(
            serde_json::to_value(BrowseEvent::Recovered).unwrap(),
            serde_json::json!({"type": "recovered"})
        );
    }
}
