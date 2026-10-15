use crate::expr::{self, ExprContext};
use anyhow::{Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Flat Graph Arena Definitions (Zero-Copy & Flat Indexed)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeKind {
    ManualTrigger = 0,
    ScheduleTrigger = 1,
    Webhook = 2,
    Set = 3,
    If = 4,
    Switch = 5,
    Filter = 6,
    Limit = 7,
    Sort = 8,
    Code = 9,
    Merge = 10,
    HttpRequest = 11,
    Unknown = 255,
}

impl NodeKind {
    pub fn from_type_str(s: &str) -> Self {
        match s {
            "n8n-nodes-base.manualTrigger" | "manualTrigger" => NodeKind::ManualTrigger,
            "n8n-nodes-base.scheduleTrigger" | "scheduleTrigger" => NodeKind::ScheduleTrigger,
            "n8n-nodes-base.webhook" | "webhook" => NodeKind::Webhook,
            "n8n-nodes-base.set" | "set" => NodeKind::Set,
            "n8n-nodes-base.if" | "if" => NodeKind::If,
            "n8n-nodes-base.switch" | "switch" => NodeKind::Switch,
            "n8n-nodes-base.filter" | "filter" => NodeKind::Filter,
            "n8n-nodes-base.limit" | "limit" => NodeKind::Limit,
            "n8n-nodes-base.sort" | "sort" => NodeKind::Sort,
            "n8n-nodes-base.code" | "code" => NodeKind::Code,
            "n8n-nodes-base.merge" | "merge" => NodeKind::Merge,
            "n8n-nodes-base.httpRequest" | "httpRequest" => NodeKind::HttpRequest,
            _ => NodeKind::Unknown,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArenaNode {
    pub id: String,
    pub name: String,
    pub node_type: String,
    pub parameters: Value,
    pub has_expressions: bool,
    pub kind: NodeKind,
}

impl ArenaNode {
    pub fn new(
        id: String,
        name: String,
        node_type: String,
        parameters: Value,
        has_expressions: bool,
    ) -> Self {
        let kind = NodeKind::from_type_str(&node_type);
        Self {
            id,
            name,
            node_type,
            parameters,
            has_expressions,
            kind,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeTarget {
    pub to_idx: u32,
    pub output_port: u16,
}

pub struct CompiledGraph {
    pub nodes: Vec<ArenaNode>,
    pub edges: Vec<Vec<EdgeTarget>>,
    pub in_degree: Vec<u32>,
    pub roots: Vec<u32>,
    pub node_index: HashMap<String, u32>,
}

impl CompiledGraph {
    /// Compiles standard n8n JSON `nodes` and `connections` into a flat indexed Arena graph
    pub fn compile(nodes_json: &Value, connections_json: &Value) -> Result<Self> {
        let raw_nodes = nodes_json
            .as_array()
            .context("Workflow nodes must be a JSON array")?;

        let total = raw_nodes.len();
        let mut nodes = Vec::with_capacity(total);
        let mut node_index = HashMap::with_capacity(total);

        for (idx, n) in raw_nodes.iter().enumerate() {
            let id = n
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = n
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or(&id)
                .to_string();
            let node_type = n
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("n8n-nodes-base.manualTrigger")
                .to_string();
            let parameters = n.get("parameters").cloned().unwrap_or_else(|| json!({}));
            let has_expressions = crate::expr::has_potential_expression(&parameters);

            node_index.insert(name.clone(), idx as u32);
            if !id.is_empty() && id != name {
                node_index.insert(id.clone(), idx as u32);
            }

            let kind = NodeKind::from_type_str(&node_type);
            nodes.push(ArenaNode {
                id,
                name,
                node_type,
                parameters,
                has_expressions,
                kind,
            });
        }

        let mut edges: Vec<Vec<EdgeTarget>> = vec![Vec::new(); total];
        let mut in_degree = vec![0u32; total];

        if let Some(conn_obj) = connections_json.as_object() {
            for (source_name, outputs) in conn_obj {
                if let Some(&from_idx) = node_index.get(source_name) {
                    if let Some(main_outputs) = outputs.get("main").and_then(|v| v.as_array()) {
                        for (port_idx, output_list) in main_outputs.iter().enumerate() {
                            if let Some(targets) = output_list.as_array() {
                                for target_conn in targets {
                                    if let Some(target_name) =
                                        target_conn.get("node").and_then(|v| v.as_str())
                                    {
                                        if let Some(&to_idx) = node_index.get(target_name) {
                                            let edge_target = EdgeTarget {
                                                to_idx,
                                                output_port: port_idx as u16,
                                            };
                                            if !edges[from_idx as usize].iter().any(|e| {
                                                e.to_idx == to_idx
                                                    && e.output_port == edge_target.output_port
                                            }) {
                                                edges[from_idx as usize].push(edge_target);
                                                in_degree[to_idx as usize] += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut roots = Vec::new();
        for (i, &deg) in in_degree.iter().enumerate() {
            if deg == 0 {
                roots.push(i as u32);
            }
        }

        Ok(Self {
            nodes,
            edges,
            in_degree,
            roots,
            node_index,
        })
    }

    /// Fast programmatic builder for testing hyper-scale topologies (e.g. 1,000,000 nodes)
    pub fn programmatic(mut nodes: Vec<ArenaNode>, edges: Vec<Vec<u32>>) -> Self {
        for node in &mut nodes {
            if node.kind == NodeKind::Unknown {
                node.kind = NodeKind::from_type_str(&node.node_type);
            }
        }
        let total = nodes.len();
        let mut in_degree = vec![0u32; total];
        let mut converted_edges: Vec<Vec<EdgeTarget>> = Vec::with_capacity(total);

        for targets in edges {
            let mut node_targets = Vec::with_capacity(targets.len());
            for to in targets {
                if (to as usize) < total {
                    in_degree[to as usize] += 1;
                    node_targets.push(EdgeTarget {
                        to_idx: to,
                        output_port: 0,
                    });
                }
            }
            converted_edges.push(node_targets);
        }

        let mut roots = Vec::new();
        for (i, &deg) in in_degree.iter().enumerate() {
            if deg == 0 {
                roots.push(i as u32);
            }
        }

        Self {
            nodes,
            edges: converted_edges,
            in_degree,
            roots,
            node_index: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Execution Event Callback (Real-time Push to UI)
// ---------------------------------------------------------------------------

pub trait ExecutionNotifier: Send + Sync {
    fn notify_started(&self, execution_id: &str, workflow_id: Option<&str>);
    fn notify_node_before(&self, execution_id: &str, node_name: &str);
    fn notify_node_after(&self, execution_id: &str, node_name: &str, output: &Value);
    fn notify_finished(&self, execution_id: &str, status: &str, duration_ms: u64);
}

// ---------------------------------------------------------------------------
// Multi-Runner Work-Stealing Pool Engine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub execution_id: String,
    pub status: String,
    pub executed_nodes: u32,
    pub duration_ms: u64,
    pub final_output: Value,
    pub payload: Value,
}

#[derive(Clone)]
pub struct HyperScaleEngine {
    rayon_pool: Arc<ThreadPool>,
}

impl HyperScaleEngine {
    pub fn new(threads: Option<usize>) -> Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads.unwrap_or_else(num_cpus_available))
            .thread_name(|i| format!("n8n-runner-{i}"))
            .build()
            .context("Failed to build Rayon thread pool for HyperScaleEngine")?;

        Ok(Self {
            rayon_pool: Arc::new(pool),
        })
    }

    /// Execute a compiled DAG with zero-copy Arc payload passing and Rayon work-stealing wave scheduling
    pub async fn execute(
        &self,
        execution_id: &str,
        workflow_id: Option<&str>,
        graph: &CompiledGraph,
        initial_input: Arc<Value>,
        notifier: Option<Arc<dyn ExecutionNotifier>>,
        cancel_token: Option<Arc<AtomicBool>>,
    ) -> Result<ExecutionResult> {
        let start_time = Instant::now();
        let total_nodes = graph.nodes.len();

        if let Some(ref n) = notifier {
            n.notify_started(execution_id, workflow_id);
        }

        if total_nodes == 0 {
            let duration_ms = start_time.elapsed().as_millis() as u64;
            if let Some(ref n) = notifier {
                n.notify_finished(execution_id, "success", duration_ms);
            }
            return Ok(ExecutionResult {
                execution_id: execution_id.to_string(),
                status: "success".to_string(),
                executed_nodes: 0,
                duration_ms,
                final_output: (*initial_input).clone(),
                payload: json!({ "mode": "empty", "status": "success", "executedNodes": 0 }),
            });
        }

        // Initialize atomic in-degrees
        // Initialize atomic in-degrees
        let remaining_in_degree: Vec<AtomicU32> =
            graph.in_degree.iter().map(|&d| AtomicU32::new(d)).collect();

        let executed_count = AtomicU32::new(0);
        let outputs = DashMap::<u32, Arc<Value>>::new();
        let cancel_flag = cancel_token.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
        let timed_out = AtomicBool::new(false);
        let timeout_duration = std::time::Duration::from_secs(60);

        // Hyper-scale throttling flag (for > 1000 nodes, throttle UI websocket messages)
        let is_hyperscale = total_nodes > 1000;
        let is_compact_logging = total_nodes > 5000;

        let last_output_holder = std::sync::Mutex::new(Arc::clone(&initial_input));

        // Pre-allocate thread-safe accumulation buffers ONLY for nodes with multi-input fan-in (in_degree > 1)
        let multi_inputs = DashMap::<u32, std::sync::Mutex<Vec<Arc<Value>>>>::new();
        for (idx, &degree) in graph.in_degree.iter().enumerate() {
            if degree > 1 {
                multi_inputs.insert(
                    idx as u32,
                    std::sync::Mutex::new(Vec::with_capacity(degree as usize)),
                );
            }
        }

        let ctx = TraversalContext {
            graph,
            remaining_in_degree: &remaining_in_degree,
            outputs: &outputs,
            multi_inputs: &multi_inputs,
            executed_count: &executed_count,
            cancel_flag: &cancel_flag,
            timed_out: &timed_out,
            start_time,
            timeout_duration,
            notifier: notifier.as_deref(),
            execution_id,
            last_output_holder: &last_output_holder,
            is_hyperscale,
            is_compact_logging,
            total_nodes,
        };

        // Execute DAG using Rayon Work-Stealing Scope
        self.rayon_pool.install(|| {
            rayon::scope(|s| {
                for &root_idx in &graph.roots {
                    let initial_input = Arc::clone(&initial_input);
                    let ctx_ref = &ctx;

                    s.spawn(move |s| {
                        traverse_node(s, ctx_ref, root_idx, initial_input);
                    });
                }
            });
        });

        let duration_ms = start_time.elapsed().as_millis() as u64;
        let final_count = executed_count.load(Ordering::Relaxed);
        let status = if cancel_flag.load(Ordering::Relaxed) {
            info!(
                execution_id = %execution_id,
                "Execution cancelled via cancellation token"
            );
            "canceled"
        } else if timed_out.load(Ordering::Relaxed) {
            warn!(
                execution_id = %execution_id,
                "Execution timed out by watchdog"
            );
            "timeout"
        } else {
            "success"
        };

        if let Some(ref n) = notifier {
            n.notify_finished(execution_id, status, duration_ms);
        }

        // Determine final output
        let last_output = last_output_holder
            .into_inner()
            .unwrap_or_else(|e| e.into_inner());

        // Construct storage payload: Compact vs Full mode
        let payload = if is_compact_logging {
            json!({
                "mode": "hyperScaleCompact",
                "status": status,
                "totalNodes": total_nodes,
                "executedNodes": final_count,
                "durationMs": duration_ms,
                "lastNodeOutput": *last_output
            })
        } else {
            let now_ms = Utc::now().timestamp_millis();
            let start_time_ms = now_ms.saturating_sub(duration_ms as i64);
            let node_status = if status == "success" {
                "success"
            } else {
                "error"
            };

            let mut run_data = serde_json::Map::new();
            for (idx, node) in graph.nodes.iter().enumerate() {
                if let Some(out) = outputs.get(&(idx as u32)) {
                    let main_items: Vec<Value> = if let Some(arr) = out.value().as_array() {
                        arr.iter().map(|item| json!({ "json": item })).collect()
                    } else {
                        vec![json!({ "json": *out.value() })]
                    };
                    run_data.insert(
                        node.name.clone(),
                        json!([
                            {
                                "startTime": start_time_ms,
                                "executionTime": duration_ms.max(1),
                                "executionStatus": node_status,
                                "source": [],
                                "data": {
                                    "main": [ main_items ]
                                }
                            }
                        ]),
                    );
                }
            }
            json!({
                "resultData": {
                    "runData": run_data
                }
            })
        };

        debug!(
            execution_id = %execution_id,
            executed = final_count,
            total = total_nodes,
            duration_ms = duration_ms,
            "Execution completed"
        );

        Ok(ExecutionResult {
            execution_id: execution_id.to_string(),
            status: status.to_string(),
            executed_nodes: final_count,
            duration_ms,
            final_output: (*last_output).clone(),
            payload,
        })
    }
}

struct TraversalContext<'a> {
    graph: &'a CompiledGraph,
    remaining_in_degree: &'a [AtomicU32],
    outputs: &'a DashMap<u32, Arc<Value>>,
    multi_inputs: &'a DashMap<u32, std::sync::Mutex<Vec<Arc<Value>>>>,
    executed_count: &'a AtomicU32,
    cancel_flag: &'a AtomicBool,
    timed_out: &'a AtomicBool,
    start_time: Instant,
    timeout_duration: std::time::Duration,
    notifier: Option<&'a dyn ExecutionNotifier>,
    execution_id: &'a str,
    last_output_holder: &'a std::sync::Mutex<Arc<Value>>,
    is_hyperscale: bool,
    is_compact_logging: bool,
    total_nodes: usize,
}

fn prepare_target_input(
    ctx: &TraversalContext,
    target_idx: u32,
    single_fallback: &Arc<Value>,
) -> Arc<Value> {
    if ctx.graph.in_degree[target_idx as usize] > 1 {
        if let Some(entry) = ctx.multi_inputs.get(&target_idx) {
            let items_guard = entry.value().lock().unwrap_or_else(|e| e.into_inner());
            let mut flat_items = Vec::with_capacity(items_guard.len());
            for item in items_guard.iter() {
                if let Some(arr) = item.as_array() {
                    flat_items.extend(arr.iter().cloned());
                } else {
                    flat_items.push((**item).clone());
                }
            }
            Arc::new(Value::Array(flat_items))
        } else {
            Arc::clone(single_fallback)
        }
    } else {
        Arc::clone(single_fallback)
    }
}

fn traverse_node<'scope>(
    s: &rayon::Scope<'scope>,
    ctx: &'scope TraversalContext<'scope>,
    current_idx: u32,
    current_input: Arc<Value>,
) {
    let mut local_executed: u32 = 0;
    traverse_node_inner(s, ctx, current_idx, current_input, &mut local_executed);
    if local_executed > 0 {
        ctx.executed_count.fetch_add(local_executed, Ordering::Relaxed);
    }
}

fn traverse_node_inner<'scope>(
    s: &rayon::Scope<'scope>,
    ctx: &'scope TraversalContext<'scope>,
    mut current_idx: u32,
    mut current_input: Arc<Value>,
    local_executed: &mut u32,
) {
    let expr_ctx = ExprContext::new(
        ctx.execution_id,
        Some(ctx.outputs),
        Some(&ctx.graph.node_index),
    );

    loop {
        if (*local_executed == 0)
            && (ctx.cancel_flag.load(Ordering::Relaxed)
                || ctx.timed_out.load(Ordering::Relaxed)
                || ctx.start_time.elapsed() > ctx.timeout_duration)
        {
            if !ctx.cancel_flag.load(Ordering::Relaxed) {
                ctx.timed_out.store(true, Ordering::Relaxed);
            }
            return;
        }

        let node = &ctx.graph.nodes[current_idx as usize];

        // UI notification before node execution
        if let Some(n) = ctx.notifier {
            if !ctx.is_hyperscale || current_idx.is_multiple_of(1000) {
                n.notify_node_before(ctx.execution_id, &node.name);
            }
        }

        // Execute node transformation with zero-copy Arc<Value> & conditional port routing
        let (result_payload, active_port) =
            execute_node_logic(node, current_input, &expr_ctx, ctx.is_hyperscale);

        // Store output for UI runData ONLY when not compact logging (saves massive allocations in hyperscale)
        if !ctx.is_compact_logging {
            ctx.outputs.insert(current_idx, Arc::clone(&result_payload));
        }

        if current_idx == (ctx.total_nodes - 1) as u32 {
            if let Ok(mut guard) = ctx.last_output_holder.lock() {
                *guard = Arc::clone(&result_payload);
            }
        }

        // UI notification after node execution
        if let Some(n) = ctx.notifier {
            if !ctx.is_hyperscale || current_idx.is_multiple_of(1000) {
                n.notify_node_after(ctx.execution_id, &node.name, &result_payload);
            }
        }

        *local_executed += 1;
        if *local_executed >= 1024 {
            ctx.executed_count.fetch_add(*local_executed, Ordering::Relaxed);
            *local_executed = 0;
        }

        let targets = &ctx.graph.edges[current_idx as usize];
        if targets.is_empty() {
            if let Ok(mut guard) = ctx.last_output_holder.lock() {
                *guard = Arc::clone(&result_payload);
            }
            return;
        }

        // Fast-path: exactly 1 downstream edge (zero heap allocation!)
        if targets.len() == 1 {
            let edge = targets[0];
            let is_edge_active = match active_port {
                Some(p) => edge.output_port == p,
                None => true,
            };

            if is_edge_active {
                let target_idx = edge.to_idx;
                if ctx.graph.in_degree[target_idx as usize] > 1 {
                    if let Some(entry) = ctx.multi_inputs.get(&target_idx) {
                        if let Ok(mut lock) = entry.value().lock() {
                            lock.push(Arc::clone(&result_payload));
                        }
                    }
                }

                if ctx.remaining_in_degree[target_idx as usize].fetch_sub(1, Ordering::AcqRel) == 1 {
                    current_idx = target_idx;
                    current_input = prepare_target_input(ctx, current_idx, &result_payload);
                    continue;
                }
            } else {
                prune_inactive_branch(ctx, edge.to_idx);
            }

            return;
        }

        // Multi-edge routing (IF branches, fan-out, etc.)
        let mut ready_targets = Vec::new();
        for edge in targets {
            let target_idx = edge.to_idx;
            let port = edge.output_port;

            let is_edge_active = match active_port {
                Some(p) => port == p,
                None => true,
            };

            if is_edge_active {
                if ctx.graph.in_degree[target_idx as usize] > 1 {
                    if let Some(entry) = ctx.multi_inputs.get(&target_idx) {
                        if let Ok(mut lock) = entry.value().lock() {
                            lock.push(Arc::clone(&result_payload));
                        }
                    }
                }

                if ctx.remaining_in_degree[target_idx as usize].fetch_sub(1, Ordering::AcqRel) == 1 {
                    ready_targets.push(target_idx);
                }
            } else {
                prune_inactive_branch(ctx, target_idx);
            }
        }

        if ready_targets.is_empty() {
            return;
        }

        if ready_targets.len() == 1 {
            current_idx = ready_targets[0];
            current_input = prepare_target_input(ctx, current_idx, &result_payload);
            continue;
        }

        for &target_idx in &ready_targets[1..] {
            let target_input = prepare_target_input(ctx, target_idx, &result_payload);
            s.spawn(move |s| {
                traverse_node(s, ctx, target_idx, target_input);
            });
        }

        current_idx = ready_targets[0];
        current_input = prepare_target_input(ctx, current_idx, &result_payload);
    }
}

/// Recursively prunes dead branches when a conditional node (IF/Switch) skips an output port
fn prune_inactive_branch(ctx: &TraversalContext, target_idx: u32) {
    let prev = ctx.remaining_in_degree[target_idx as usize].fetch_sub(1, Ordering::AcqRel);
    if prev == 1 {
        // Target is now fully unreached, propagate pruning to downstream children
        for child_edge in &ctx.graph.edges[target_idx as usize] {
            prune_inactive_branch(ctx, child_edge.to_idx);
        }
    }
}

/// Helper to cleanly extract n8n Set node parameters into top-level json properties
fn apply_set_parameters(target_obj: &mut serde_json::Map<String, Value>, params: &Value) {
    if let Some(params_obj) = params.as_object() {
        if params_obj.is_empty() {
            return;
        }

        // 1. Format n8n v1/v2: "values": { "string": [...], "number": [...], "boolean": [...] }
        if let Some(values_obj) = params_obj.get("values").and_then(|v| v.as_object()) {
            for type_arr in values_obj.values() {
                if let Some(arr) = type_arr.as_array() {
                    for item in arr {
                        if let (Some(k), Some(v)) =
                            (item.get("name").and_then(|n| n.as_str()), item.get("value"))
                        {
                            target_obj.insert(k.to_string(), v.clone());
                        }
                    }
                }
            }
        }

        // 2. Format n8n Edit Fields v3.4: "assignments": { "assignments": [ { "name": k, "value": v, ... } ] }
        if let Some(assign_arr) = params_obj
            .get("assignments")
            .and_then(|a| a.get("assignments"))
            .and_then(|arr| arr.as_array())
        {
            for item in assign_arr {
                if let (Some(k), Some(v)) =
                    (item.get("name").and_then(|n| n.as_str()), item.get("value"))
                {
                    target_obj.insert(k.to_string(), v.clone());
                }
            }
        }

        // 3. Parameter key-value langsung lainnya (selain internal fields)
        for (k, v) in params_obj {
            if k != "values" && k != "assignments" && k != "options" && k != "keepOnlySet" {
                target_obj.insert(k.clone(), v.clone());
            }
        }
    }
}

/// Evaluates if conditions for IF and Filter nodes
fn evaluate_if_condition(
    params: &Value,
    item_json: &Value,
    ctx: &ExprContext,
    has_expressions: bool,
) -> bool {
    // 1. Simple format: value1 vs value2
    if let (Some(v1), Some(v2)) = (params.get("value1"), params.get("value2")) {
        let op = params
            .get("operation")
            .and_then(|o| o.as_str())
            .unwrap_or("equal");
        if !has_expressions {
            return check_op(v1, v2, op);
        }
        let left = expr::resolve_parameters(v1, item_json, ctx);
        let right = expr::resolve_parameters(v2, item_json, ctx);
        return check_op(&left, &right, op);
    }

    // 2. n8n conditions object (v1 & v2 formats)
    if let Some(conds) = params.get("conditions") {
        if let Some(arr) = conds.get("conditions").and_then(|c| c.as_array()) {
            if arr.is_empty() {
                return true;
            }
            for c in arr {
                let left = c
                    .get("leftValue")
                    .map(|v| expr::resolve_parameters(v, item_json, ctx))
                    .unwrap_or(Value::Null);
                let right = c
                    .get("rightValue")
                    .map(|v| expr::resolve_parameters(v, item_json, ctx))
                    .unwrap_or(Value::Null);
                let op = c
                    .get("operator")
                    .and_then(|o| o.get("operation"))
                    .and_then(|op| op.as_str())
                    .or_else(|| c.get("operation").and_then(|op| op.as_str()))
                    .unwrap_or("equals");
                if !check_op(&left, &right, op) {
                    return false;
                }
            }
            return true;
        }

        // v1 type groups: boolean, string, number
        let mut evaluated_any = false;
        for (_type_name, list) in conds.as_object().into_iter().flatten() {
            if let Some(items) = list.as_array() {
                for item in items {
                    evaluated_any = true;
                    let left = item
                        .get("value1")
                        .map(|v| expr::resolve_parameters(v, item_json, ctx))
                        .unwrap_or(Value::Null);
                    let right = item
                        .get("value2")
                        .map(|v| expr::resolve_parameters(v, item_json, ctx))
                        .unwrap_or(Value::Null);
                    let op = item
                        .get("operation")
                        .and_then(|o| o.as_str())
                        .unwrap_or("equal");
                    if !check_op(&left, &right, op) {
                        return false;
                    }
                }
            }
        }
        if evaluated_any {
            return true;
        }
    }

    // 3. Fallback boolean parameter
    if let Some(b) = params.get("boolean") {
        let resolved = expr::resolve_parameters(b, item_json, ctx);
        return resolved.as_bool().unwrap_or(false);
    }

    true
}

fn check_op(left: &Value, right: &Value, op: &str) -> bool {
    match op {
        "equal" | "equals" | "==" => left == right,
        "notEqual" | "notEquals" | "!=" => left != right,
        "larger" | ">" => expr::compare_values(left, right, ">"),
        "largerEqual" | ">=" => expr::compare_values(left, right, ">="),
        "smaller" | "<" => expr::compare_values(left, right, "<"),
        "smallerEqual" | "<=" => expr::compare_values(left, right, "<="),
        "contains" => {
            let l_str = left
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| left.to_string());
            let r_str = right
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| right.to_string());
            l_str.contains(&r_str)
        }
        "isEmpty" => match left {
            Value::Null => true,
            Value::String(s) => s.is_empty(),
            Value::Array(a) => a.is_empty(),
            Value::Object(o) => o.is_empty(),
            _ => false,
        },
        "isNotEmpty" => match left {
            Value::Null => false,
            Value::String(s) => !s.is_empty(),
            Value::Array(a) => !a.is_empty(),
            Value::Object(o) => !o.is_empty(),
            _ => true,
        },
        _ => left == right,
    }
}

fn is_set_idempotent(obj: &serde_json::Map<String, Value>, params: &Value) -> bool {
    if let Some(params_obj) = params.as_object() {
        if params_obj.is_empty() {
            return true;
        }
        if params_obj.contains_key("values") || params_obj.contains_key("assignments") {
            return false;
        }
        let mut checked_any = false;
        for (k, v) in params_obj {
            if k != "options" && k != "keepOnlySet" {
                checked_any = true;
                if obj.get(k) != Some(v) {
                    return false;
                }
            }
        }
        checked_any
    } else {
        false
    }
}

fn evaluate_switch_rule(
    params: &Value,
    item_json: &Value,
    ctx: &ExprContext,
    has_expressions: bool,
) -> u16 {
    if let Some(rules) = params
        .get("rules")
        .and_then(|r| r.get("rules"))
        .and_then(|r| r.as_array())
    {
        for (idx, rule) in rules.iter().enumerate() {
            if let Some(conditions) = rule.get("conditions") {
                if evaluate_if_condition(conditions, item_json, ctx, has_expressions) {
                    return idx as u16;
                }
            }
        }
    }
    0
}

/// Node execution logic: executes transformations with zero-copy input & expression evaluation
fn execute_node_logic(
    node: &ArenaNode,
    input: Arc<Value>,
    expr_ctx: &ExprContext,
    is_hyperscale: bool,
) -> (Arc<Value>, Option<u16>) {
    let resolved_holder;
    let params_ref = if !is_hyperscale && node.has_expressions {
        resolved_holder = expr::resolve_parameters(&node.parameters, &input, expr_ctx);
        &resolved_holder
    } else {
        &node.parameters
    };

    match node.kind {
        NodeKind::ManualTrigger => {
            let payload = if input.is_null() || input.as_object().is_some_and(|o| o.is_empty()) {
                Arc::new(json!({
                    "timestamp": Utc::now().to_rfc3339(),
                    "triggeredBy": "manualTest"
                }))
            } else {
                input
            };
            (payload, None)
        }
        NodeKind::ScheduleTrigger => (
            Arc::new(json!({
                "timestamp": Utc::now().to_rfc3339(),
                "triggeredBy": "schedule"
            })),
            None,
        ),
        NodeKind::Webhook => {
            // Webhook passthrough payload
            (input, None)
        }
        NodeKind::Merge => {
            // Full multi-branch fan-in array preservation
            let payload = if input.is_array() {
                input
            } else {
                Arc::new(json!([*input]))
            };
            (payload, None)
        }
        NodeKind::Set => {
            let payload = if let Some(arr) = input.as_array() {
                let mut new_arr = Vec::with_capacity(arr.len());
                for elem in arr {
                    let mut merged = elem.clone();
                    if let Some(obj) = merged.as_object_mut() {
                        if !is_hyperscale && node.has_expressions {
                            let item_resolved =
                                expr::resolve_parameters(&node.parameters, elem, expr_ctx);
                            apply_set_parameters(obj, &item_resolved);
                        } else {
                            apply_set_parameters(obj, params_ref);
                        }
                    }
                    new_arr.push(merged);
                }
                Arc::new(Value::Array(new_arr))
            } else if let Some(obj) = input.as_object() {
                if is_set_idempotent(obj, params_ref) {
                    Arc::clone(&input)
                } else {
                    let mut merged = (*input).clone();
                    if let Some(obj_mut) = merged.as_object_mut() {
                        apply_set_parameters(obj_mut, params_ref);
                    }
                    Arc::new(merged)
                }
            } else {
                let mut merged = (*input).clone();
                if let Some(obj) = merged.as_object_mut() {
                    apply_set_parameters(obj, params_ref);
                }
                Arc::new(merged)
            };
            (payload, None)
        }
        NodeKind::If => {
            let is_true = evaluate_if_condition(params_ref, &input, expr_ctx, node.has_expressions);
            let port = if is_true { 0 } else { 1 };
            (input, Some(port))
        }
        NodeKind::Switch => {
            let port = evaluate_switch_rule(params_ref, &input, expr_ctx, node.has_expressions);
            (input, Some(port))
        }
        NodeKind::Filter => {
            let payload = if let Some(arr) = input.as_array() {
                let mut filtered = Vec::new();
                for item in arr {
                    if evaluate_if_condition(params_ref, item, expr_ctx, node.has_expressions) {
                        filtered.push(item.clone());
                    }
                }
                Arc::new(Value::Array(filtered))
            } else if evaluate_if_condition(params_ref, &input, expr_ctx, node.has_expressions) {
                input
            } else {
                Arc::new(Value::Array(vec![]))
            };
            (payload, None)
        }
        NodeKind::Limit => {
            let max_items = params_ref
                .get("maxItems")
                .and_then(|v| v.as_u64())
                .unwrap_or(10) as usize;
            let payload = if let Some(arr) = input.as_array() {
                let limited: Vec<Value> = arr.iter().take(max_items).cloned().collect();
                Arc::new(Value::Array(limited))
            } else {
                input
            };
            (payload, None)
        }
        NodeKind::Sort => {
            let field_name = params_ref
                .get("fieldName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let descending = params_ref
                .get("descending")
                .and_then(|v| v.as_bool())
                .or_else(|| {
                    params_ref
                        .get("order")
                        .and_then(|v| v.as_str())
                        .map(|s| s.eq_ignore_ascii_case("desc") || s.eq_ignore_ascii_case("descending"))
                })
                .unwrap_or(false);

            let payload = if let Some(arr) = input.as_array() {
                let mut sorted = arr.clone();
                if !field_name.is_empty() {
                    sorted.sort_by(|a, b| {
                        let v_a = a.get(field_name).unwrap_or(&Value::Null);
                        let v_b = b.get(field_name).unwrap_or(&Value::Null);
                        let ord = match (v_a, v_b) {
                            (Value::Number(n1), Value::Number(n2)) => {
                                let f1 = n1.as_f64().unwrap_or(0.0);
                                let f2 = n2.as_f64().unwrap_or(0.0);
                                f1.total_cmp(&f2)
                            }
                            (Value::String(s1), Value::String(s2)) => {
                                if let (Ok(f1), Ok(f2)) =
                                    (s1.trim().parse::<f64>(), s2.trim().parse::<f64>())
                                {
                                    f1.total_cmp(&f2)
                                } else {
                                    s1.cmp(s2)
                                }
                            }
                            _ => v_a.to_string().cmp(&v_b.to_string()),
                        };
                        if descending {
                            ord.reverse()
                        } else {
                            ord
                        }
                    });
                }
                Arc::new(Value::Array(sorted))
            } else {
                input
            };
            (payload, None)
        }
        NodeKind::Code => {
            if let Some(obj) = input.as_object() {
                if obj.get("codeExecuted") == Some(&json!(true)) {
                    return (Arc::clone(&input), None);
                }
            }
            let mut val = (*input).clone();
            if let Some(obj) = val.as_object_mut() {
                obj.insert("codeExecuted".to_string(), json!(true));
            }
            (Arc::new(val), None)
        }
        NodeKind::HttpRequest => {
            let url = params_ref
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or("https://api.example.com");
            let method = params_ref
                .get("method")
                .and_then(|v| v.as_str())
                .unwrap_or("GET");
            let headers = params_ref.get("headers");
            let body = params_ref.get("body").or_else(|| params_ref.get("data"));
            let res = execute_real_http_request(url, method, headers, body);
            (Arc::new(res), None)
        }
        _ => (input, None),
    }
}

fn execute_real_http_request(
    url: &str,
    method: &str,
    headers_val: Option<&Value>,
    body_val: Option<&Value>,
) -> Value {
    if url.is_empty() {
        return json!({
            "statusCode": 400,
            "url": url,
            "method": method,
            "error": "URL cannot be empty",
            "data": Value::Null
        });
    }

    let url_owned = url.to_string();
    let method_owned = method.to_string();
    let headers_owned = headers_val.cloned();
    let body_owned = body_val.cloned();

    let runner = move || {
        let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(r) => r,
            Err(e) => {
                return json!({
                    "statusCode": 500,
                    "url": url_owned,
                    "method": method_owned,
                    "error": format!("Runtime init error: {e}"),
                    "data": Value::Null
                });
            }
        };

        rt.block_on(async move {
            let client = match reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    return json!({
                        "statusCode": 500,
                        "url": url_owned,
                        "method": method_owned,
                        "error": format!("HTTP client initialization failed: {e}"),
                        "data": Value::Null
                    });
                }
            };

            let req_method = match method_owned.to_ascii_uppercase().as_str() {
                "POST" => reqwest::Method::POST,
                "PUT" => reqwest::Method::PUT,
                "DELETE" => reqwest::Method::DELETE,
                "PATCH" => reqwest::Method::PATCH,
                "HEAD" => reqwest::Method::HEAD,
                "OPTIONS" => reqwest::Method::OPTIONS,
                _ => reqwest::Method::GET,
            };

            let mut req = client.request(req_method, &url_owned);

            if let Some(h_obj) = headers_owned.as_ref().and_then(|h| h.as_object()) {
                for (k, v) in h_obj {
                    if let Some(v_str) = v.as_str() {
                        req = req.header(k.as_str(), v_str);
                    }
                }
            }

            if let Some(ref b) = body_owned {
                if !b.is_null() && b != &json!({}) {
                    req = req.json(b);
                }
            }

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let mut resp_headers = serde_json::Map::new();
                    for (k, v) in resp.headers() {
                        if let Ok(v_str) = v.to_str() {
                            resp_headers.insert(k.as_str().to_string(), Value::String(v_str.to_string()));
                        }
                    }
                    let body_json = match resp.json::<Value>().await {
                        Ok(j) => j,
                        Err(_) => Value::Null,
                    };
                    json!({
                        "statusCode": status,
                        "url": url_owned,
                        "method": method_owned,
                        "headers": Value::Object(resp_headers),
                        "data": body_json
                    })
                }
                Err(e) => {
                    json!({
                        "statusCode": 502,
                        "url": url_owned,
                        "method": method_owned,
                        "error": e.to_string(),
                        "data": Value::Null
                    })
                }
            }
        })
    };

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(runner).join().unwrap_or_else(|_| {
            json!({
                "statusCode": 500,
                "url": url,
                "method": method,
                "error": "Thread execution panicked",
                "data": Value::Null
            })
        })
    } else {
        runner()
    }
}

fn num_cpus_available() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

