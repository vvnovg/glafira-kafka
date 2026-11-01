//! Схемы параметров для слабых клиентов. Часть разборщиков вызовов у локальных моделей (vLLM
//! `qwen3_coder`) не разрешает ссылки на `$defs` и передаёт вложенный объект строкой JSON, другие
//! модели шлют `null` и пустые ключи. Здесь — схемы без `$ref`, терпимый разбор аргументов и
//! подсказка о форме аргументов при ошибке разбора.

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

/// Приводит аргументы к схеме, не меняя смысла: строка с JSON-объектом или массивом там, где
/// строка не допускается, становится значением; пустой ключ убирается, `null` — только там, где
/// схема его не допускает (такое поле — то же, что отсутствующее, у обязательного поля ошибка
/// разбора остаётся). Допустимый `null` остаётся: у аргумента с тремя состояниями он значит
/// «убрать значение», а не «не задано».
pub(crate) fn normalize_args(args: &mut JsonObject, schema: &JsonObject) {
    for (key, value) in args.iter_mut() {
        if let Some(prop) = property(schema, key) {
            normalize(value, prop);
        }
    }
    args.retain(|k, v| {
        !k.is_empty() && !(v.is_null() && !property(schema, k).is_some_and(allows_null))
    });
}

fn normalize(value: &mut Value, schema: &JsonObject) {
    if let Value::String(text) = value
        && !allows_string(schema)
        && let Ok(parsed @ (Value::Object(_) | Value::Array(_))) = serde_json::from_str(text)
    {
        *value = parsed;
    }
    match value {
        Value::Object(obj) => normalize_args(obj, schema),
        Value::Array(items) => {
            if let Some(Value::Object(item)) = schema.get("items") {
                items.iter_mut().for_each(|i| normalize(i, item));
            }
        }
        _ => {}
    }
}

/// Схема поля объекта: в `properties` самой схемы или одной из ветвей `anyOf`/`oneOf`/`allOf`.
fn property<'a>(schema: &'a JsonObject, key: &str) -> Option<&'a JsonObject> {
    if let Some(p) = schema
        .get("properties")
        .and_then(|p| p.get(key))
        .and_then(Value::as_object)
    {
        return Some(p);
    }
    branches(schema).find_map(|b| property(b, key))
}

fn branches(schema: &JsonObject) -> impl Iterator<Item = &JsonObject> {
    ["anyOf", "oneOf", "allOf"]
        .into_iter()
        .filter_map(|k| schema.get(k).and_then(Value::as_array))
        .flatten()
        .filter_map(Value::as_object)
}

/// Схема без типа (и без ветвей) допускает что угодно, в том числе строку.
fn allows_string(schema: &JsonObject) -> bool {
    allows(schema, "string", true)
}

/// `null` допустим, только если схема называет его явно: тип `null` или ветвь с ним.
fn allows_null(schema: &JsonObject) -> bool {
    allows(schema, "null", false)
}

/// Допускает ли схема тип `ty`; `untyped` — ответ для схемы без типа и без ветвей.
fn allows(schema: &JsonObject, ty: &str, untyped: bool) -> bool {
    match schema.get("type") {
        Some(Value::String(t)) => t == ty,
        Some(Value::Array(ts)) => ts.iter().any(|t| t == ty),
        _ => {
            let mut any = branches(schema).peekable();
            if any.peek().is_none() {
                untyped
            } else {
                any.any(|b| allows(b, ty, untyped))
            }
        }
    }
}

/// Набросок аргументов по схеме для подсказки: `{"topic": "string", "limit?": "integer"}`.
pub(crate) fn shape(schema: &JsonObject) -> Value {
    if let Some(Value::Object(props)) = schema.get("properties") {
        let required: Vec<&str> = schema
            .get("required")
            .and_then(Value::as_array)
            .map(|r| r.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let fields = props.iter().filter_map(|(k, v)| {
            let name = if required.contains(&k.as_str()) {
                k.clone()
            } else {
                format!("{k}?")
            };
            Some((name, shape(v.as_object()?)))
        });
        return Value::Object(fields.collect());
    }
    if let Some(Value::Array(values)) = schema.get("enum") {
        let names: Vec<String> = values
            .iter()
            .map(|v| v.as_str().map_or(v.to_string(), str::to_owned))
            .collect();
        return Value::String(names.join("|"));
    }
    if let Some(c) = schema.get("const") {
        return c.clone();
    }
    let alternatives: Vec<Value> = branches(schema)
        .filter(|b| b.get("type").and_then(Value::as_str) != Some("null"))
        .map(shape)
        .collect();
    match alternatives.len() {
        0 => {}
        1 => return alternatives.into_iter().next().unwrap_or_default(),
        _ if alternatives.iter().all(Value::is_string) => {
            let names: Vec<&str> = alternatives.iter().filter_map(Value::as_str).collect();
            return Value::String(names.join(" | "));
        }
        _ => return serde_json::json!({ "oneOf": alternatives }),
    }
    match schema.get("type") {
        Some(Value::String(t)) if t == "array" => {
            let item = schema
                .get("items")
                .and_then(Value::as_object)
                .map(shape)
                .unwrap_or_default();
            Value::Array(vec![item])
        }
        Some(Value::String(t)) => Value::String(t.clone()),
        Some(Value::Array(ts)) => {
            let names: Vec<&str> = ts
                .iter()
                .filter_map(Value::as_str)
                .filter(|t| *t != "null")
                .collect();
            Value::String(names.join(" | "))
        }
        _ => Value::String("any".into()),
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

    #[test]
    fn json_string_becomes_an_object_where_a_string_is_not_allowed() {
        let schema = obj(json!({
            "properties": {
                "target": {"type": "object", "properties": {"kind": {"type": "string"}}},
                "text": {"type": "string"},
                "from": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
                "topics": {"type": "array", "items": {"type": "string"}}
            }
        }));
        let mut args = obj(json!({
            "target": "{\"kind\": \"topic\", \"extra\": null}",
            "text": "{\"stays\": \"a string\"}",
            "from": "[1]",
            "topics": "[\"a\", \"b\"]",
            "": "",
            "limit": null
        }));
        normalize_args(&mut args, &schema);
        assert_eq!(
            Value::Object(args),
            json!({
                "target": {"kind": "topic"},
                "text": "{\"stays\": \"a string\"}",
                "from": "[1]",
                "topics": ["a", "b"]
            })
        );
    }

    #[test]
    fn nested_strings_in_one_of_branches_are_parsed() {
        let schema = obj(json!({
            "properties": {"change": {"oneOf": [
                {"type": "object", "properties": {"kind": {"const": "a"}, "scope": {"type": "object"}}},
                {"type": "object", "properties": {"kind": {"const": "b"}}}
            ]}}
        }));
        let mut args = obj(json!({"change": {"kind": "a", "scope": "{\"all\": true}"}}));
        normalize_args(&mut args, &schema);
        assert_eq!(args["change"]["scope"], json!({"all": true}));
    }

    #[test]
    fn shape_sketches_required_optional_and_alternatives() {
        let schema = obj(json!({
            "type": "object",
            "required": ["topic"],
            "properties": {
                "topic": {"type": "string"},
                "limit": {"type": ["integer", "null"]},
                "severity": {"type": "string", "enum": ["info", "warning"]},
                "keys": {"type": "array", "items": {"type": "string"}},
                "from": {"anyOf": [{"type": "string"}, {"type": "integer"}, {"type": "null"}]}
            }
        }));
        assert_eq!(
            shape(&schema),
            json!({
                "topic": "string",
                "limit?": "integer",
                "severity?": "info|warning",
                "keys?": ["string"],
                "from?": "string | integer"
            })
        );
    }
    #[test]
    fn null_is_kept_where_the_schema_allows_it() {
        let schema = obj(json!({
            "properties": {
                "limit": {"type": "integer"},
                "cpu": {"type": ["string", "null"]},
                "memory": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                "change": {"type": "object", "properties": {
                    "value": {"oneOf": [{"type": "string"}, {"anyOf": [{"type": "null"}]}]},
                    "note": {"type": "string"}
                }}
            }
        }));
        let mut args = obj(json!({
            "limit": null,
            "cpu": null,
            "memory": null,
            "change": {"value": null, "note": null},
            "unknown": null,
            "": ""
        }));
        normalize_args(&mut args, &schema);
        assert_eq!(
            Value::Object(args),
            json!({"cpu": null, "memory": null, "change": {"value": null}})
        );
    }
}
