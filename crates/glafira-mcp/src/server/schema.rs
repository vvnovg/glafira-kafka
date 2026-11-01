//! Схемы параметров без `$ref`: часть разборщиков вызовов у локальных моделей (vLLM `qwen3_coder`)
//! не разрешает ссылки на `$defs` и передаёт вложенный объект строкой JSON.

use std::sync::Arc;

use rmcp::model::{JsonObject, Tool};
use serde_json::Value;

const DEFS_PREFIX: &str = "#/$defs/";

/// Подставляет определения из `$defs` на место ссылок. Соседние со ссылкой ключи (`description`,
/// `default`) важнее ключей определения. Рекурсивное определение остаётся ссылкой, и тогда
/// `$defs` сохраняется.
pub(crate) fn inline_refs(schema: &JsonObject) -> JsonObject {
    let defs = match schema.get("$defs") {
        Some(Value::Object(defs)) => defs.clone(),
        _ => return schema.clone(),
    };
    let mut kept_ref = false;
    let mut out = JsonObject::new();
    for (k, v) in schema {
        if k != "$defs" {
            out.insert(k.clone(), inline(v, &defs, &mut Vec::new(), &mut kept_ref));
        }
    }
    if kept_ref {
        out.insert("$defs".into(), Value::Object(defs));
    }
    out
}

pub(crate) fn inline_tool(mut tool: Tool) -> Tool {
    tool.input_schema = Arc::new(inline_refs(&tool.input_schema));
    tool
}

fn inline(v: &Value, defs: &JsonObject, stack: &mut Vec<String>, kept: &mut bool) -> Value {
    match v {
        Value::Array(items) => {
            Value::Array(items.iter().map(|i| inline(i, defs, stack, kept)).collect())
        }
        Value::Object(obj) => {
            let target = obj
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|r| r.strip_prefix(DEFS_PREFIX))
                .filter(|name| !stack.iter().any(|s| s == name))
                .and_then(|name| Some((name.to_owned(), defs.get(name)?.as_object()?)));
            let Some((name, def)) = target else {
                *kept |= obj.contains_key("$ref");
                return Value::Object(
                    obj.iter()
                        .map(|(k, v)| (k.clone(), inline(v, defs, stack, kept)))
                        .collect(),
                );
            };
            stack.push(name);
            let mut out: JsonObject = def
                .iter()
                .map(|(k, v)| (k.clone(), inline(v, defs, stack, kept)))
                .collect();
            for (k, v) in obj.iter().filter(|(k, _)| *k != "$ref") {
                out.insert(k.clone(), inline(v, defs, stack, kept));
            }
            stack.pop();
            Value::Object(out)
        }
        _ => v.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn obj(v: Value) -> JsonObject {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn refs_are_replaced_and_defs_dropped() {
        let s = obj(json!({
            "type": "object",
            "properties": {
                "from": {"$ref": "#/$defs/Time", "description": "Start"},
                "to": {"anyOf": [{"$ref": "#/$defs/Time"}, {"type": "null"}]},
                "pair": {"$ref": "#/$defs/Pair"}
            },
            "$defs": {
                "Time": {"type": "string", "description": "A time"},
                "Pair": {"type": "object", "properties": {"a": {"$ref": "#/$defs/Time"}}}
            }
        }));
        let out = Value::Object(inline_refs(&s));
        assert_eq!(
            out["properties"]["from"],
            json!({"type": "string", "description": "Start"})
        );
        assert_eq!(
            out["properties"]["to"]["anyOf"][0],
            json!({"type": "string", "description": "A time"})
        );
        assert_eq!(
            out["properties"]["pair"]["properties"]["a"]["type"],
            "string"
        );
        assert!(out.get("$defs").is_none());
        assert!(!out.to_string().contains("$ref"));
    }

    #[test]
    fn recursive_ref_stays_with_its_defs() {
        let s = obj(json!({
            "properties": {"node": {"$ref": "#/$defs/Node"}},
            "$defs": {"Node": {"type": "object", "properties": {"next": {"$ref": "#/$defs/Node"}}}}
        }));
        let out = Value::Object(inline_refs(&s));
        assert_eq!(
            out["properties"]["node"]["properties"]["next"],
            json!({"$ref": "#/$defs/Node"})
        );
        assert!(out["$defs"]["Node"].is_object());
    }

    #[test]
    fn schema_without_defs_is_unchanged() {
        let s = obj(json!({"type": "object", "properties": {"a": {"type": "string"}}}));
        assert_eq!(inline_refs(&s), s);
    }
}
