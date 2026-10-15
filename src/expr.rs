use chrono::Utc;
use dashmap::DashMap;
use serde_json::{json, Number, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Context passed to the Expression Evaluator containing upstream outputs and runtime metadata
pub struct ExprContext<'a> {
    pub execution_id: &'a str,
    pub node_outputs: Option<&'a DashMap<u32, Arc<Value>>>,
    pub node_index: Option<&'a HashMap<String, u32>>,
}

impl<'a> ExprContext<'a> {
    pub fn new(
        execution_id: &'a str,
        node_outputs: Option<&'a DashMap<u32, Arc<Value>>>,
        node_index: Option<&'a HashMap<String, u32>>,
    ) -> Self {
        Self {
            execution_id,
            node_outputs,
            node_index,
        }
    }

    pub fn empty(execution_id: &'a str) -> Self {
        Self {
            execution_id,
            node_outputs: None,
            node_index: None,
        }
    }
}

/// Recursively traverses a node's parameters and evaluates all expression strings.
pub fn resolve_parameters(params: &Value, item_json: &Value, ctx: &ExprContext) -> Value {
    if !has_potential_expression(params) {
        return params.clone();
    }

    match params {
        Value::String(s) => resolve_expression_string(s, item_json, ctx),
        Value::Array(arr) => {
            let mut resolved = Vec::with_capacity(arr.len());
            for item in arr {
                resolved.push(resolve_parameters(item, item_json, ctx));
            }
            Value::Array(resolved)
        }
        Value::Object(map) => {
            let mut resolved = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                resolved.insert(k.clone(), resolve_parameters(v, item_json, ctx));
            }
            Value::Object(resolved)
        }
        _ => params.clone(),
    }
}

/// Fast check whether a JSON value might contain expressions (avoids recursion overhead in hyperscale benchmarks)
pub fn has_potential_expression(val: &Value) -> bool {
    match val {
        Value::String(s) => s.starts_with('=') || s.contains("{{"),
        Value::Array(arr) => arr.iter().any(has_potential_expression),
        Value::Object(map) => map.values().any(has_potential_expression),
        _ => false,
    }
}

/// Evaluates a single string that may contain `={{ ... }}` or inline `{{ ... }}` interpolations.
pub fn resolve_expression_string(input_str: &str, item_json: &Value, ctx: &ExprContext) -> Value {
    let trimmed = input_str.trim();

    // Case 1: Pure single expression starting with "={{" and ending with "}}"
    if trimmed.starts_with("={{") && trimmed.ends_with("}}") {
        let inner = &trimmed[3..trimmed.len() - 2].trim();
        // Ensure no other closing or opening blocks exist inside
        if !inner.contains("{{") && !inner.contains("}}") {
            return evaluate_raw_expression(inner, item_json, ctx);
        }
    }

    // Case 2: Pure expression starting with "=" but wrapped in single "{{ ... }}"
    if trimmed.starts_with("{{") && trimmed.ends_with("}}") {
        let inner = &trimmed[2..trimmed.len() - 2].trim();
        if !inner.contains("{{") && !inner.contains("}}") {
            return evaluate_raw_expression(inner, item_json, ctx);
        }
    }

    // Case 3: Starts with "=" directly followed by expression (n8n v1 syntax, e.g. "=$json.count * 2")
    if trimmed.starts_with('=') && !trimmed.contains("{{") {
        let inner = trimmed[1..].trim();
        if !inner.is_empty() {
            return evaluate_raw_expression(inner, item_json, ctx);
        }
    }

    // Case 4: Text with inline {{ ... }} blocks (String interpolation)
    if trimmed.contains("{{") && trimmed.contains("}}") {
        let mut result = String::new();
        let mut rest = if let Some(stripped) = trimmed.strip_prefix('=') {
            stripped
        } else {
            trimmed
        };

        while let Some(start_idx) = rest.find("{{") {
            result.push_str(&rest[..start_idx]);
            let after_open = &rest[start_idx + 2..];
            if let Some(end_idx) = after_open.find("}}") {
                let expr_str = after_open[..end_idx].trim();
                let evaluated = evaluate_raw_expression(expr_str, item_json, ctx);
                match evaluated {
                    Value::String(s) => result.push_str(&s),
                    Value::Null => {}
                    other => result.push_str(&other.to_string()),
                }
                rest = &after_open[end_idx + 2..];
            } else {
                result.push_str("{{");
                rest = after_open;
            }
        }
        result.push_str(rest);
        return Value::String(result);
    }

    // Default: return original string value
    Value::String(input_str.to_string())
}

/// Evaluates a raw expression without braces (e.g. `$json.price * $json.qty` or `$json.name.toUpperCase()`)
pub fn evaluate_raw_expression(expr: &str, item_json: &Value, ctx: &ExprContext) -> Value {
    let expr = expr.trim();
    if expr.is_empty() {
        return Value::Null;
    }

    // 0. Logical Boolean Operators (||, &&)
    if let Some((left, right)) = split_by_operator(expr, "||") {
        let l_bool = value_to_bool(&evaluate_raw_expression(left, item_json, ctx));
        if l_bool {
            return Value::Bool(true);
        }
        let r_bool = value_to_bool(&evaluate_raw_expression(right, item_json, ctx));
        return Value::Bool(r_bool);
    }
    if let Some((left, right)) = split_by_operator(expr, "&&") {
        let l_bool = value_to_bool(&evaluate_raw_expression(left, item_json, ctx));
        if !l_bool {
            return Value::Bool(false);
        }
        let r_bool = value_to_bool(&evaluate_raw_expression(right, item_json, ctx));
        return Value::Bool(r_bool);
    }

    // 1. Comparison Operators (==, !=, >=, <=, >, <)
    for op in ["==", "!=", ">=", "<=", ">", "<"] {
        if let Some((left, right)) = split_by_operator(expr, op) {
            let left_val = evaluate_raw_expression(left, item_json, ctx);
            let right_val = evaluate_raw_expression(right, item_json, ctx);
            return Value::Bool(compare_values(&left_val, &right_val, op));
        }
    }

    // 2. Arithmetic Addition and Subtraction (+, -)
    for op in ['+', '-'] {
        if let Some((left, right)) = split_by_arithmetic(expr, op) {
            let left_val = evaluate_raw_expression(left, item_json, ctx);
            let right_val = evaluate_raw_expression(right, item_json, ctx);
            if op == '+' {
                // If either is string, perform concatenation
                if left_val.is_string() || right_val.is_string() {
                    let s1 = value_to_string(&left_val);
                    let s2 = value_to_string(&right_val);
                    return Value::String(format!("{s1}{s2}"));
                }
                if let (Some(n1), Some(n2)) = (value_to_f64(&left_val), value_to_f64(&right_val)) {
                    let res = n1 + n2;
                    if res.fract() == 0.0 && res.is_finite() {
                        return json!(res as i64);
                    }
                    if let Some(num) = Number::from_f64(res) {
                        return Value::Number(num);
                    }
                }
            } else if let (Some(n1), Some(n2)) = (value_to_f64(&left_val), value_to_f64(&right_val))
            {
                let res = n1 - n2;
                if res.fract() == 0.0 && res.is_finite() {
                    return json!(res as i64);
                }
                if let Some(num) = Number::from_f64(res) {
                    return Value::Number(num);
                }
            }
        }
    }

    // 3. Arithmetic Multiplication and Division (*, /)
    for op in ['*', '/'] {
        if let Some((left, right)) = split_by_arithmetic(expr, op) {
            let left_val = evaluate_raw_expression(left, item_json, ctx);
            let right_val = evaluate_raw_expression(right, item_json, ctx);
            if let (Some(n1), Some(n2)) = (value_to_f64(&left_val), value_to_f64(&right_val)) {
                let res = if op == '*' {
                    n1 * n2
                } else if n2 != 0.0 {
                    n1 / n2
                } else {
                    0.0
                };
                if res.fract() == 0.0 && res.is_finite() {
                    return json!(res as i64);
                }
                if let Some(num) = Number::from_f64(res) {
                    return Value::Number(num);
                }
            }
        }
    }

    // 4. Primitive Literals
    if expr == "true" {
        return Value::Bool(true);
    }
    if expr == "false" {
        return Value::Bool(false);
    }
    if expr == "null" {
        return Value::Null;
    }
    if (expr.starts_with('"') && expr.ends_with('"'))
        || (expr.starts_with('\'') && expr.ends_with('\''))
    {
        if expr.len() >= 2 {
            return Value::String(expr[1..expr.len() - 1].to_string());
        }
        return Value::String(String::new());
    }
    if let Ok(i) = expr.parse::<i64>() {
        return Value::Number(Number::from(i));
    }
    if let Ok(f) = expr.parse::<f64>() {
        if let Some(num) = Number::from_f64(f) {
            return Value::Number(num);
        }
    }

    // 5. Built-in system variables ($now, $today, $execution.id, $env)
    if expr == "$now" {
        return Value::String(Utc::now().to_rfc3339());
    }
    if expr == "$today" {
        return Value::String(Utc::now().format("%Y-%m-%d").to_string());
    }
    if expr == "$execution.id" {
        return Value::String(ctx.execution_id.to_string());
    }
    if let Some(var_name) = expr.strip_prefix("$env.") {
        let val = std::env::var(var_name).unwrap_or_default();
        return Value::String(val);
    }
    if let Some(var_name) = expr.strip_prefix("$vars.") {
        let val = crate::engine::get_variable(var_name.trim()).unwrap_or_default();
        return Value::String(val);
    }
    if let Some(inner) = expr.strip_prefix("$vars[").and_then(|s| s.strip_suffix(']')) {
        let clean_key = inner.trim().trim_matches(|c| c == '\'' || c == '"');
        let val = crate::engine::get_variable(clean_key).unwrap_or_default();
        return Value::String(val);
    }

    // 6. Upstream Node Access ($node["NodeName"].json.prop or $('NodeName').item.json.prop)
    if expr.starts_with("$node[") || expr.starts_with("$('") || expr.starts_with("$(\"") {
        return evaluate_upstream_node_path(expr, ctx);
    }

    // 7. Input item access ($json.path or chained transformations)
    let effective_item = if let Some(inner) = item_json.get("json") {
        inner
    } else {
        item_json
    };

    if expr == "$json" {
        return effective_item.clone();
    }
    if let Some(path) = expr.strip_prefix("$json.") {
        let val = evaluate_path_with_methods(effective_item, path);
        if val.is_null() && effective_item != item_json {
            return evaluate_path_with_methods(item_json, path);
        }
        return val;
    }
    if let Some(path) = expr.strip_prefix("$json[") {
        if let Some(closing_bracket) = path.find(']') {
            let key_part = &path[..closing_bracket];
            let clean_key = key_part.trim().trim_matches(|c| c == '\'' || c == '"');
            let after = path[closing_bracket + 1..].trim_start_matches('.');
            let val = if let Some(obj) = effective_item.as_object() {
                obj.get(clean_key).cloned().unwrap_or(Value::Null)
            } else if let Some(obj) = item_json.as_object() {
                obj.get(clean_key).cloned().unwrap_or(Value::Null)
            } else {
                Value::Null
            };
            if !after.is_empty() {
                return evaluate_path_with_methods(&val, after);
            }
            return val;
        }
        let val = evaluate_path_with_methods(effective_item, path);
        if val.is_null() && effective_item != item_json {
            return evaluate_path_with_methods(item_json, path);
        }
        return val;
    }

    // Fallback: evaluate path directly on item_json
    let val = evaluate_path_with_methods(effective_item, expr);
    if val.is_null() && effective_item != item_json {
        return evaluate_path_with_methods(item_json, expr);
    }
    val
}

/// Evaluates upstream node expressions like:
/// - `$node["NodeName"].json.price`
/// - `$('NodeName').item.json.price`
fn evaluate_upstream_node_path(expr: &str, ctx: &ExprContext) -> Value {
    let (node_name, remaining_path) = extract_node_name_and_path(expr);
    if node_name.is_empty() {
        return Value::Null;
    }

    let node_idx = match ctx.node_index.and_then(|idx_map| idx_map.get(&node_name)) {
        Some(&idx) => idx,
        None => return Value::Null,
    };

    let node_output_arc = match ctx.node_outputs.and_then(|outs| outs.get(&node_idx)) {
        Some(val) => Arc::clone(&*val),
        None => return Value::Null,
    };

    // Extract the json data payload from the node's output
    let payload = match &*node_output_arc {
        Value::Array(items) if !items.is_empty() => {
            if let Some(first_obj) = items[0].get("json") {
                first_obj
            } else {
                &items[0]
            }
        }
        Value::Object(obj) => {
            if let Some(inner) = obj.get("json") {
                inner
            } else {
                &*node_output_arc
            }
        }
        other => other,
    };

    // Clean remaining path: strip prefixes like ".json", "['json']", ".item.json"
    let clean_path = remaining_path
        .trim_start_matches(".item")
        .trim_start_matches("['item']")
        .trim_start_matches(".json")
        .trim_start_matches("['json']")
        .trim_start_matches('.');

    if clean_path.is_empty() {
        payload.clone()
    } else {
        evaluate_path_with_methods(payload, clean_path)
    }
}

/// Helper to parse node name and remaining path from `$node["..."]` or `$('...')`
fn extract_node_name_and_path(expr: &str) -> (String, String) {
    if let Some(rest) = expr.strip_prefix("$node[") {
        let quote_char = rest.chars().next().unwrap_or('"');
        if quote_char == '"' || quote_char == '\'' {
            let inner = &rest[1..];
            if let Some(closing_idx) = inner.find(quote_char) {
                let name = inner[..closing_idx].to_string();
                let after = &inner[closing_idx + 1..];
                let after_bracket = after.trim_start_matches(']');
                return (name, after_bracket.to_string());
            }
        }
    } else if expr.starts_with("$('") || expr.starts_with("$(\"") {
        let quote_char = expr.chars().nth(2).unwrap_or('\'');
        let inner = &expr[3..];
        if let Some(closing_idx) = inner.find(quote_char) {
            let name = inner[..closing_idx].to_string();
            let after = &inner[closing_idx + 1..];
            let after_paren = after.trim_start_matches(')');
            return (name, after_paren.to_string());
        }
    }
    (String::new(), String::new())
}

/// Navigates a nested path (e.g. `user.address.city` or `items[0].name`) and evaluates transformations
fn evaluate_path_with_methods(root: &Value, path: &str) -> Value {
    let mut current = root.clone();
    let parts = split_path_segments(path);

    for part in parts {
        let part_trimmed = part.trim();
        if part_trimmed.is_empty() {
            continue;
        }

        // Method calls
        if part_trimmed == "toUpperCase()" {
            if let Some(s) = current.as_str() {
                current = Value::String(s.to_uppercase());
            }
            continue;
        }
        if part_trimmed == "toLowerCase()" {
            if let Some(s) = current.as_str() {
                current = Value::String(s.to_lowercase());
            }
            continue;
        }
        if part_trimmed == "trim()" {
            if let Some(s) = current.as_str() {
                current = Value::String(s.trim().to_string());
            }
            continue;
        }
        if part_trimmed == "length" {
            if let Some(s) = current.as_str() {
                current = json!(s.len());
            } else if let Some(arr) = current.as_array() {
                current = json!(arr.len());
            }
            continue;
        }

        // Array index access like `items[0]`
        if let Some((field, idx)) = parse_array_index(part_trimmed) {
            if !field.is_empty() {
                current = current.get(&field).cloned().unwrap_or(Value::Null);
            }
            if let Some(arr) = current.as_array() {
                current = arr.get(idx).cloned().unwrap_or(Value::Null);
            } else {
                current = Value::Null;
            }
            continue;
        }

        // Standard object field access
        if let Some(obj) = current.as_object() {
            current = obj.get(part_trimmed).cloned().unwrap_or(Value::Null);
        } else {
            return Value::Null;
        }
    }

    current
}

/// Parses path segments taking dots into account, excluding method parens
fn split_path_segments(path: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();

    let mut in_parens = false;
    for ch in path.chars() {
        if ch == '(' {
            in_parens = true;
            current.push(ch);
        } else if ch == ')' {
            in_parens = false;
            current.push(ch);
        } else if ch == '.' && !in_parens {
            if !current.is_empty() {
                segments.push(current.clone());
                current.clear();
            }
        } else {
            current.push(ch);
        }
    }

    if !current.is_empty() {
        segments.push(current);
    }

    segments
}

/// Parses array index syntax such as `items[0]` into `("items", 0)`
fn parse_array_index(part: &str) -> Option<(String, usize)> {
    if let Some(open) = part.find('[') {
        if let Some(close) = part.find(']') {
            if close > open {
                let field = part[..open].to_string();
                let idx_str = &part[open + 1..close];
                if let Ok(idx) = idx_str.parse::<usize>() {
                    return Some((field, idx));
                }
            }
        }
    }
    None
}

/// Splits expression by a comparison operator (outside quotes)
fn split_by_operator<'a>(expr: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let bytes = expr.as_bytes();
    let op_bytes = op.as_bytes();

    for i in 0..bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if b == b'"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote
            && !in_double_quote
            && i + op_bytes.len() <= bytes.len()
            && &bytes[i..i + op_bytes.len()] == op_bytes
        {
            // Ensure not preceded or followed by '=' if op is '>' or '<'
            if (op == ">" || op == "<") && (i + 1 < bytes.len() && bytes[i + 1] == b'=') {
                continue;
            }
            let left = expr[..i].trim();
            let right = expr[i + op_bytes.len()..].trim();
            return Some((left, right));
        }
    }
    None
}

/// Splits expression by an arithmetic operator (+, -, *, /) respecting left-associativity
fn split_by_arithmetic(expr: &str, target_op: char) -> Option<(&str, &str)> {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let bytes = expr.as_bytes();
    let mut last_split_idx = None;

    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if b == b'"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote && !in_double_quote && b == target_op as u8 {
            // Avoid interpreting leading negative sign as binary operator
            if target_op == '-' {
                let prev_non_space = expr[..i].trim_end();
                if prev_non_space.is_empty() {
                    continue;
                }
                let last_ch = prev_non_space.chars().last().unwrap_or(' ');
                if matches!(last_ch, '+' | '-' | '*' | '/' | '(' | '=') {
                    continue;
                }
            }
            let left = expr[..i].trim();
            let right = expr[i + 1..].trim();
            if !left.is_empty() && !right.is_empty() {
                last_split_idx = Some(i);
            }
        }
    }

    if let Some(i) = last_split_idx {
        let left = expr[..i].trim();
        let right = expr[i + 1..].trim();
        return Some((left, right));
    }
    None
}

/// Compares two JSON values according to the operator
pub fn compare_values(left: &Value, right: &Value, op: &str) -> bool {
    match op {
        "==" => left == right,
        "!=" => left != right,
        ">" => match (value_to_f64(left), value_to_f64(right)) {
            (Some(n1), Some(n2)) => n1 > n2,
            _ => value_to_string(left) > value_to_string(right),
        },
        "<" => match (value_to_f64(left), value_to_f64(right)) {
            (Some(n1), Some(n2)) => n1 < n2,
            _ => value_to_string(left) < value_to_string(right),
        },
        ">=" => match (value_to_f64(left), value_to_f64(right)) {
            (Some(n1), Some(n2)) => n1 >= n2,
            _ => value_to_string(left) >= value_to_string(right),
        },
        "<=" => match (value_to_f64(left), value_to_f64(right)) {
            (Some(n1), Some(n2)) => n1 <= n2,
            _ => value_to_string(left) <= value_to_string(right),
        },
        _ => false,
    }
}

fn value_to_f64(val: &Value) -> Option<f64> {
    match val {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn value_to_string(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn value_to_bool(val: &Value) -> bool {
    match val {
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
        Value::String(s) => !s.is_empty() && s != "false",
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expression_arithmetic_and_string() {
        let item = json!({
            "price": 100,
            "qty": 3,
            "name": "rust"
        });
        let ctx = ExprContext::empty("exec-123");

        let res_num = resolve_expression_string("={{ $json.price * $json.qty }}", &item, &ctx);
        assert_eq!(res_num, json!(300));

        let res_str = resolve_expression_string("={{ $json.name.toUpperCase() }}", &item, &ctx);
        assert_eq!(res_str, json!("RUST"));

        let res_interp = resolve_expression_string(
            "Order for {{ $json.name }} total: {{ $json.price }}",
            &item,
            &ctx,
        );
        assert_eq!(res_interp, json!("Order for rust total: 100"));

        // Left-associativity tests
        let res_sub = resolve_expression_string("={{ 10 - 3 - 2 }}", &item, &ctx);
        assert_eq!(res_sub, json!(5));

        let res_div = resolve_expression_string("={{ 20 / 2 / 2 }}", &item, &ctx);
        assert_eq!(res_div, json!(5));
    }

    #[test]
    fn test_expression_comparison() {
        let item = json!({ "status": "active", "count": 10 });
        let ctx = ExprContext::empty("exec-123");

        let res_eq = resolve_expression_string("={{ $json.status == \"active\" }}", &item, &ctx);
        assert_eq!(res_eq, json!(true));

        let res_gt = resolve_expression_string("={{ $json.count > 5 }}", &item, &ctx);
        assert_eq!(res_gt, json!(true));

        // Logical boolean operators tests
        let res_and = resolve_expression_string(
            "={{ $json.count > 5 && $json.status == \"active\" }}",
            &item,
            &ctx,
        );
        if res_and != json!(true) {
            panic!("FAIL_AND: res_and was {:?}", res_and);
        }

        let res_or = resolve_expression_string(
            "={{ $json.count < 5 || $json.status == \"active\" }}",
            &item,
            &ctx,
        );
        if res_or != json!(true) {
            panic!("FAIL_OR: res_or was {:?}", res_or);
        }
    }
}
