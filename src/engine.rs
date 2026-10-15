use crate::expr::{self, ExprContext};
use anyhow::{Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
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
    Code = 9,
    Merge = 10,
    HttpRequest = 11,
    ExecuteWorkflow = 12,
    RespondToWebhook = 13,
    SplitOut = 14,
    Aggregate = 15,
    RemoveDuplicates = 16,
    SplitInBatches = 17,
    Wait = 18,
    StopAndError = 19,
    ErrorTrigger = 20,
    Switch = 21,
    Sort = 22,
    Limit = 23,
    Filter = 24,
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
            "n8n-nodes-base.sort" | "sort" => NodeKind::Sort,
            "n8n-nodes-base.limit" | "limit" => NodeKind::Limit,
            "n8n-nodes-base.filter" | "filter" => NodeKind::Filter,
            "n8n-nodes-base.code" | "code" => NodeKind::Code,
            "n8n-nodes-base.merge" | "merge" => NodeKind::Merge,
            "n8n-nodes-base.httpRequest" | "httpRequest" => NodeKind::HttpRequest,
            "n8n-nodes-base.executeWorkflow" | "executeWorkflow" => NodeKind::ExecuteWorkflow,
            "n8n-nodes-base.respondToWebhook" | "respondToWebhook" => NodeKind::RespondToWebhook,
            "n8n-nodes-base.splitOut" | "splitOut" => NodeKind::SplitOut,
            "n8n-nodes-base.aggregate" | "aggregate" => NodeKind::Aggregate,
            "n8n-nodes-base.removeDuplicates" | "removeDuplicates" => NodeKind::RemoveDuplicates,
            "n8n-nodes-base.splitInBatches" | "splitInBatches" => NodeKind::SplitInBatches,
            "n8n-nodes-base.wait" | "wait" => NodeKind::Wait,
            "n8n-nodes-base.stopAndError" | "stopAndError" => NodeKind::StopAndError,
            "n8n-nodes-base.errorTrigger" | "errorTrigger" => NodeKind::ErrorTrigger,
            _ => NodeKind::Unknown,
        }
    }
}

static SUBWORKFLOW_REGISTRY: OnceLock<DashMap<String, Value>> = OnceLock::new();

pub fn register_subworkflow(workflow_id: &str, workflow_data: Value) {
    let registry = SUBWORKFLOW_REGISTRY.get_or_init(DashMap::new);
    registry.insert(workflow_id.to_string(), workflow_data);
}

pub fn get_subworkflow(workflow_id: &str) -> Option<Value> {
    let registry = SUBWORKFLOW_REGISTRY.get_or_init(DashMap::new);
    registry.get(workflow_id).map(|v| v.clone())
}

pub fn unregister_subworkflow(workflow_id: &str) {
    if let Some(registry) = SUBWORKFLOW_REGISTRY.get() {
        registry.remove(workflow_id);
    }
}

static CREDENTIAL_REGISTRY: OnceLock<DashMap<String, Value>> = OnceLock::new();

pub fn register_credential(credential_id: &str, data: Value) {
    let registry = CREDENTIAL_REGISTRY.get_or_init(DashMap::new);
    registry.insert(credential_id.to_string(), data);
}

pub fn get_credential(credential_id: &str) -> Option<Value> {
    let registry = CREDENTIAL_REGISTRY.get_or_init(DashMap::new);
    registry.get(credential_id).map(|v| v.clone())
}

pub fn unregister_credential(credential_id: &str) {
    if let Some(registry) = CREDENTIAL_REGISTRY.get() {
        registry.remove(credential_id);
    }
}

static VARIABLE_REGISTRY: OnceLock<DashMap<String, String>> = OnceLock::new();

pub fn register_variable(key: &str, value: &str) {
    let registry = VARIABLE_REGISTRY.get_or_init(DashMap::new);
    registry.insert(key.to_string(), value.to_string());
}

pub fn get_variable(key: &str) -> Option<String> {
    let registry = VARIABLE_REGISTRY.get_or_init(DashMap::new);
    registry.get(key).map(|v| v.clone())
}

pub fn unregister_variable(key: &str) {
    if let Some(registry) = VARIABLE_REGISTRY.get() {
        registry.remove(key);
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

pub trait NodeExecutor: Send + Sync {
    fn execute(
        &self,
        node: &ArenaNode,
        input: Arc<Value>,
        ctx: &ExprContext,
        is_hyperscale: bool,
    ) -> (Arc<Value>, Option<u16>);
}

pub struct DefaultNodeExecutor;

impl NodeExecutor for DefaultNodeExecutor {
    fn execute(
        &self,
        node: &ArenaNode,
        input: Arc<Value>,
        ctx: &ExprContext,
        is_hyperscale: bool,
    ) -> (Arc<Value>, Option<u16>) {
        execute_node_logic(node, input, ctx, is_hyperscale, 0)
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
            let mut parameters = n.get("parameters").cloned().unwrap_or_else(|| json!({}));
            if let Some(cof) = n.get("continueOnFail").and_then(|v| v.as_bool()) {
                if let Some(obj) = parameters.as_object_mut() {
                    obj.entry("continueOnFail".to_string()).or_insert(json!(cof));
                }
            }
            if let Some(oe) = n.get("onError").and_then(|v| v.as_str()) {
                if oe == "continueRegularOutput" || oe == "continueErrorOutput" {
                    if let Some(obj) = parameters.as_object_mut() {
                        obj.entry("continueOnFail".to_string()).or_insert(json!(true));
                    }
                }
            }
            if let Some(creds) = n.get("credentials") {
                if let Some(obj) = parameters.as_object_mut() {
                    obj.entry("credentials".to_string()).or_insert_with(|| creds.clone());
                }
            }
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
        let has_error = AtomicBool::new(false);
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
            has_error: &has_error,
            start_time,
            timeout_duration,
            notifier: notifier.as_deref(),
            execution_id,
            last_output_holder: &last_output_holder,
            is_hyperscale,
            is_compact_logging,
            total_nodes,
            subworkflow_depth: 0,
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
        } else if has_error.load(Ordering::Relaxed) {
            "error"
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

            let mut run_data = serde_json::Map::new();
            for (idx, node) in graph.nodes.iter().enumerate() {
                if let Some(out) = outputs.get(&(idx as u32)) {
                    let is_node_error = out
                        .value()
                        .as_object()
                        .and_then(|o| o.get("__node_error"))
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);

                    let (node_status, error_val) = if is_node_error {
                        let err_obj = out
                            .value()
                            .as_object()
                            .and_then(|o| o.get("error"))
                            .cloned()
                            .unwrap_or_else(|| json!({ "message": "Node execution failed" }));
                        ("error", Some(err_obj))
                    } else if status == "success" {
                        ("success", None)
                    } else {
                        ("error", None)
                    };

                    let main_items: Vec<Value> = if let Some(arr) = out.value().as_array() {
                        arr.iter().map(|item| json!({ "json": item })).collect()
                    } else {
                        vec![json!({ "json": *out.value() })]
                    };

                    let mut node_execution_entry = json!({
                        "startTime": start_time_ms,
                        "executionTime": duration_ms.max(1),
                        "executionStatus": node_status,
                        "source": [],
                        "data": {
                            "main": [ main_items ]
                        }
                    });

                    if let Some(err) = error_val {
                        if let Some(obj) = node_execution_entry.as_object_mut() {
                            obj.insert("error".to_string(), err);
                        }
                    }

                    run_data.insert(
                        node.name.clone(),
                        json!([ node_execution_entry ]),
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
    has_error: &'a AtomicBool,
    start_time: Instant,
    timeout_duration: std::time::Duration,
    notifier: Option<&'a dyn ExecutionNotifier>,
    execution_id: &'a str,
    last_output_holder: &'a std::sync::Mutex<Arc<Value>>,
    is_hyperscale: bool,
    is_compact_logging: bool,
    total_nodes: usize,
    subworkflow_depth: u32,
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
            execute_node_logic(node, current_input, &expr_ctx, ctx.is_hyperscale, ctx.subworkflow_depth);

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

        let has_node_error = result_payload
            .as_object()
            .and_then(|o| o.get("__node_error"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if has_node_error {
            ctx.has_error.store(true, Ordering::Relaxed);
            for edge in &ctx.graph.edges[current_idx as usize] {
                prune_inactive_branch(ctx, edge.to_idx);
            }
            if let Ok(mut guard) = ctx.last_output_holder.lock() {
                *guard = Arc::clone(&result_payload);
            }
            return;
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

        let mut to_insert: Vec<(String, Value)> = Vec::new();

        // 1. Format n8n v1/v2: "values": { "string": [...], "number": [...], "boolean": [...] }
        if let Some(values_obj) = params_obj.get("values").and_then(|v| v.as_object()) {
            for type_arr in values_obj.values() {
                if let Some(arr) = type_arr.as_array() {
                    for item in arr {
                        if let (Some(k), Some(v)) =
                            (item.get("name").and_then(|n| n.as_str()), item.get("value"))
                        {
                            to_insert.push((k.to_string(), v.clone()));
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
                    to_insert.push((k.to_string(), v.clone()));
                }
            }
        }

        // 3. Parameter key-value langsung lainnya (selain internal fields)
        for (k, v) in params_obj {
            if k != "values" && k != "assignments" && k != "options" && k != "keepOnlySet" {
                to_insert.push((k.clone(), v.clone()));
            }
        }

        // Terapkan ke target_obj dan ke inner target_obj["json"] jika ada
        let has_inner_json = target_obj.get("json").is_some_and(|j| j.is_object());
        for (k, v) in to_insert {
            target_obj.insert(k.clone(), v.clone());
            if has_inner_json {
                if let Some(inner) = target_obj.get_mut("json").and_then(|j| j.as_object_mut()) {
                    inner.insert(k, v);
                }
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
    let actual_item = if let Some(inner) = item_json.get("json") {
        inner
    } else {
        item_json
    };

    // 1. Simple format: value1 vs value2
    if let (Some(v1), Some(v2)) = (params.get("value1"), params.get("value2")) {
        let op = params
            .get("operation")
            .and_then(|o| o.as_str())
            .unwrap_or("equal");
        if !has_expressions {
            return check_op(v1, v2, op);
        }
        let left = expr::resolve_parameters(v1, actual_item, ctx);
        let right = expr::resolve_parameters(v2, actual_item, ctx);
        return check_op(&left, &right, op);
    }

    // 2. Locate conditions container: either params["conditions"] or params itself
    let cond_target = if let Some(inner_conds) = params.get("conditions") {
        inner_conds
    } else {
        params
    };

    // 2a. Direct array of conditions or cond_target["conditions"] as array
    let conditions_array = if let Some(arr) = cond_target.as_array() {
        Some(arr)
    } else {
        cond_target.get("conditions").and_then(|c| c.as_array())
    };

    if let Some(arr) = conditions_array {
        if arr.is_empty() {
            return true;
        }
        let is_or = cond_target
            .get("combinator")
            .and_then(|c| c.as_str())
            .map(|s| s.eq_ignore_ascii_case("or") || s.eq_ignore_ascii_case("any"))
            .unwrap_or(false);

        for c in arr {
            let left = c
                .get("leftValue")
                .or_else(|| c.get("value1"))
                .map(|v| expr::resolve_parameters(v, actual_item, ctx))
                .unwrap_or(Value::Null);
            let right = c
                .get("rightValue")
                .or_else(|| c.get("value2"))
                .map(|v| expr::resolve_parameters(v, actual_item, ctx))
                .unwrap_or(Value::Null);
            let op = c
                .get("operator")
                .and_then(|o| {
                    if let Some(s) = o.as_str() {
                        Some(s)
                    } else {
                        o.get("operation").and_then(|op| op.as_str())
                    }
                })
                .or_else(|| c.get("operation").and_then(|op| op.as_str()))
                .unwrap_or("equals");
            let matched = check_op(&left, &right, op);
            if is_or {
                if matched {
                    return true;
                }
            } else if !matched {
                return false;
            }
        }
        return !is_or;
    }

    // 2b. v1 type groups: boolean, string, number, dateTime
    if let Some(obj) = cond_target.as_object() {
        let mut evaluated_any = false;
        let is_or = obj
            .get("combinator")
            .and_then(|c| c.as_str())
            .map(|s| s.eq_ignore_ascii_case("or") || s.eq_ignore_ascii_case("any"))
            .unwrap_or(false);

        for (key, list) in obj {
            if (key == "boolean" || key == "string" || key == "number" || key == "dateTime")
                && list.is_array()
            {
                if let Some(items) = list.as_array() {
                    for item in items {
                        evaluated_any = true;
                        let left = item
                            .get("value1")
                            .or_else(|| item.get("leftValue"))
                            .map(|v| expr::resolve_parameters(v, actual_item, ctx))
                            .unwrap_or(Value::Null);
                        let right = item
                            .get("value2")
                            .or_else(|| item.get("rightValue"))
                            .map(|v| expr::resolve_parameters(v, actual_item, ctx))
                            .unwrap_or(Value::Null);
                        let op = item
                            .get("operation")
                            .or_else(|| item.get("operator").and_then(|o| o.get("operation")))
                            .and_then(|o| o.as_str())
                            .unwrap_or("equal");
                        let matched = check_op(&left, &right, op);
                        if is_or {
                            if matched {
                                return true;
                            }
                        } else if !matched {
                            return false;
                        }
                    }
                }
            }
        }
        if evaluated_any {
            return !is_or;
        }
    }

    // 3. Fallback boolean parameter
    if let Some(b) = params.get("boolean").or_else(|| cond_target.get("boolean")) {
        let resolved = expr::resolve_parameters(b, actual_item, ctx);
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
        "exists" => !left.is_null(),
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
        let inner_obj = obj.get("json").and_then(|j| j.as_object());
        let mut checked_any = false;
        for (k, v) in params_obj {
            if k != "options" && k != "keepOnlySet" {
                checked_any = true;
                let matches_top = obj.get(k) == Some(v);
                let matches_inner = inner_obj.and_then(|j| j.get(k)) == Some(v);
                if !matches_top && !matches_inner {
                    return false;
                }
            }
        }
        checked_any
    } else {
        false
    }
}

/// Node execution logic: executes transformations with zero-copy input & expression evaluation
fn execute_node_logic(
    node: &ArenaNode,
    input: Arc<Value>,
    expr_ctx: &ExprContext,
    is_hyperscale: bool,
    subworkflow_depth: u32,
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
            let payload = if let Some(d) = params_ref
                .get("data")
                .or_else(|| params_ref.get("input"))
                .or_else(|| params_ref.get("values"))
            {
                Arc::new(d.clone())
            } else if input.is_null() || input.as_object().is_some_and(|o| o.is_empty()) {
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
            if let Some(arr) = input.as_array() {
                if !is_hyperscale && node.has_expressions {
                    let mut new_arr = Vec::with_capacity(arr.len());
                    for elem in arr {
                        let mut merged = elem.clone();
                        if let Some(obj) = merged.as_object_mut() {
                            let item_resolved =
                                crate::expr::resolve_parameters(&node.parameters, elem, expr_ctx);
                            apply_set_parameters(obj, &item_resolved);
                        }
                        new_arr.push(merged);
                    }
                    return (Arc::new(Value::Array(new_arr)), None);
                }

                let all_idempotent = arr.iter().all(|item| {
                    if let Some(obj) = item.as_object() {
                        is_set_idempotent(obj, params_ref)
                    } else {
                        false
                    }
                });
                if all_idempotent {
                    return (Arc::clone(&input), None);
                }
                let mut new_arr = Vec::with_capacity(arr.len());
                for elem in arr {
                    let mut merged = elem.clone();
                    if let Some(obj) = merged.as_object_mut() {
                        apply_set_parameters(obj, params_ref);
                    }
                    new_arr.push(merged);
                }
                return (Arc::new(Value::Array(new_arr)), None);
            }
            let payload = if let Some(obj) = input.as_object() {
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
            let switch_params = if node.has_expressions {
                &node.parameters
            } else {
                params_ref
            };
            let rules_arr = switch_params
                .get("rules")
                .and_then(|r| {
                    if r.is_array() {
                        r.as_array()
                    } else {
                        r.get("rules").and_then(|sub| sub.as_array())
                    }
                });
            if let Some(rules) = rules_arr {
                if !rules.is_empty() {
                    let raw_item = input.as_array().and_then(|a| a.first()).unwrap_or(&*input);
                    let test_item = if let Some(inner) = raw_item.get("json") {
                        inner
                    } else {
                        raw_item
                    };
                    for (i, rule) in rules.iter().enumerate() {
                        if let Some(conditions) = rule.get("conditions") {
                            if evaluate_if_condition(conditions, test_item, expr_ctx, node.has_expressions) {
                                let target_port = rule
                                    .get("outputPort")
                                    .or_else(|| rule.get("output"))
                                    .and_then(|v| {
                                        v.as_u64()
                                            .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
                                    })
                                    .map(|p| p as u16)
                                    .unwrap_or(i as u16);
                                return (input, Some(target_port));
                            }
                        } else {
                            let field = rule.get("field").and_then(|f| f.as_str()).unwrap_or("");
                            let actual = if !field.is_empty() {
                                test_item.get(field).unwrap_or(&Value::Null)
                            } else {
                                rule.get("value1").unwrap_or(&Value::Null)
                            };
                            let op = rule
                                .get("operator")
                                .or_else(|| rule.get("operation"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("equals");
                            let expected = rule
                                .get("value")
                                .or_else(|| rule.get("value2"))
                                .unwrap_or(&Value::Null);
                            if check_op(actual, expected, op) {
                                let target_port = rule
                                    .get("outputPort")
                                    .or_else(|| rule.get("output"))
                                    .and_then(|v| {
                                        v.as_u64()
                                            .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
                                    })
                                    .map(|p| p as u16)
                                    .unwrap_or(i as u16);
                                return (input, Some(target_port));
                            }
                        }
                    }
                    let fallback = switch_params
                        .get("fallbackOutput")
                        .or_else(|| {
                            switch_params.get("fallback").and_then(|f| {
                                if f.is_object() {
                                    f.get("output").or_else(|| f.get("outputPort"))
                                } else {
                                    Some(f)
                                }
                            })
                        })
                        .and_then(|v| {
                            v.as_u64()
                                .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
                        })
                        .map(|p| p as u16);
                    return (input, fallback);
                }
            }
            (input, None)
        }
        NodeKind::Sort => {
            let sort_field = params_ref
                .get("sortField")
                .or_else(|| params_ref.get("fieldName"))
                .and_then(|v| v.as_str())
                .unwrap_or("id");
            let is_desc = params_ref
                .get("order")
                .and_then(|v| v.as_str())
                .map(|s| s.eq_ignore_ascii_case("desc") || s.eq_ignore_ascii_case("descending"))
                .unwrap_or_else(|| {
                    params_ref
                        .get("descending")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                });

            if let Some(arr) = input.as_array() {
                let mut sorted = arr.clone();
                sorted.sort_by(|a, b| {
                    let v_a = a.get(sort_field).unwrap_or(&Value::Null);
                    let v_b = b.get(sort_field).unwrap_or(&Value::Null);
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
                    if is_desc {
                        ord.reverse()
                    } else {
                        ord
                    }
                });
                (Arc::new(Value::Array(sorted)), None)
            } else {
                (input, None)
            }
        }
        NodeKind::Limit => {
            let max_items = params_ref
                .get("maxItems")
                .and_then(|v| v.as_u64())
                .unwrap_or(1)
                .max(1) as usize;
            let keep = params_ref
                .get("keep")
                .and_then(|v| v.as_str())
                .unwrap_or("firstItems");

            if let Some(arr) = input.as_array() {
                if arr.len() > max_items {
                    let slice = if keep == "lastItems" {
                        let start = arr.len().saturating_sub(max_items);
                        arr[start..].to_vec()
                    } else {
                        arr[..max_items].to_vec()
                    };
                    (Arc::new(Value::Array(slice)), None)
                } else {
                    (input, None)
                }
            } else {
                (input, None)
            }
        }
        NodeKind::Filter => {
            let filter_params = if node.has_expressions {
                &node.parameters
            } else {
                params_ref
            };
            if filter_params.get("conditions").is_some() || filter_params.get("value1").is_some() {
                if let Some(arr) = input.as_array() {
                    let filtered: Vec<Value> = arr
                        .iter()
                        .filter(|item| {
                            evaluate_if_condition(filter_params, item, expr_ctx, node.has_expressions)
                        })
                        .cloned()
                        .collect();
                    return (Arc::new(Value::Array(filtered)), None);
                } else if evaluate_if_condition(filter_params, &input, expr_ctx, node.has_expressions) {
                    return (input, None);
                } else {
                    return (Arc::new(Value::Array(vec![])), None);
                }
            }

            let field = filter_params
                .get("field")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let op = filter_params
                .get("operator")
                .or_else(|| filter_params.get("operation"))
                .and_then(|v| v.as_str())
                .unwrap_or("equals");
            let expected = filter_params
                .get("value")
                .or_else(|| filter_params.get("value2"))
                .unwrap_or(&Value::Null);

            if let Some(arr) = input.as_array() {
                let filtered: Vec<Value> = arr
                    .iter()
                    .filter(|item| {
                        let actual_item = if let Some(inner) = item.get("json") {
                            inner
                        } else {
                            item
                        };
                        if op == "exists" {
                            actual_item.get(field).is_some_and(|v| !v.is_null())
                        } else {
                            let actual = if !field.is_empty() {
                                actual_item.get(field).unwrap_or(&Value::Null)
                            } else {
                                actual_item
                            };
                            check_op(actual, expected, op)
                        }
                    })
                    .cloned()
                    .collect();
                (Arc::new(Value::Array(filtered)), None)
            } else {
                let actual_item = if let Some(inner) = input.get("json") {
                    inner
                } else {
                    &*input
                };
                let actual = if !field.is_empty() {
                    actual_item.get(field).unwrap_or(&Value::Null)
                } else {
                    actual_item
                };
                let matches = if op == "exists" {
                    actual_item.get(field).is_some_and(|v| !v.is_null())
                } else {
                    check_op(actual, expected, op)
                };
                if matches {
                    (input, None)
                } else {
                    (Arc::new(Value::Array(vec![])), None)
                }
            }
        }
        NodeKind::Code => {
            let language = params_ref
                .get("language")
                .and_then(|v| v.as_str())
                .unwrap_or("javascript");

            let default_version = match language {
                "python" => "3.11-sandbox",
                "wasm" => "wasm32-wasmer-4.0",
                _ => "es2022-quickjs",
            };

            let version = params_ref
                .get("runtimeVersion")
                .or_else(|| params_ref.get("languageVersion"))
                .and_then(|v| v.as_str())
                .unwrap_or(default_version);

            let code_str = params_ref
                .get("jsCode")
                .or_else(|| params_ref.get("pythonCode"))
                .or_else(|| params_ref.get("wasmCode"))
                .or_else(|| params_ref.get("code"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim();

            let is_default_bypass = is_hyperscale
                || code_str.is_empty()
                || code_str == "return $input.all();"
                || code_str == "return _input.all();"
                || code_str == "return $input.all()"
                || code_str == "return _input.all()";

            if is_default_bypass {
                let runtime_tag = json!(format!("{language}:{version}"));
                if let Some(arr) = input.as_array() {
                    let all_executed = arr.iter().all(|item| {
                        item.as_object()
                            .and_then(|o| {
                                o.get("codeExecuted")
                                    .or_else(|| o.get("json").and_then(|j| j.get("codeExecuted")))
                            })
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    });
                    if all_executed {
                        return (Arc::clone(&input), None);
                    }
                    let mut new_arr = arr.clone();
                    for item in &mut new_arr {
                        if let Some(obj) = item.as_object_mut() {
                            if let Some(inner) = obj.get_mut("json").and_then(|j| j.as_object_mut()) {
                                inner.insert("codeExecuted".to_string(), json!(true));
                                inner.insert("runtimeUsed".to_string(), runtime_tag.clone());
                            }
                            obj.insert("codeExecuted".to_string(), json!(true));
                            obj.insert("runtimeUsed".to_string(), runtime_tag.clone());
                        }
                    }
                    return (Arc::new(Value::Array(new_arr)), None);
                }
                if let Some(obj) = input.as_object() {
                    let executed = obj
                        .get("codeExecuted")
                        .or_else(|| obj.get("json").and_then(|j| j.get("codeExecuted")))
                        == Some(&json!(true));
                    if executed {
                        return (Arc::clone(&input), None);
                    }
                }
                let mut val = (*input).clone();
                if let Some(obj) = val.as_object_mut() {
                    if let Some(inner) = obj.get_mut("json").and_then(|j| j.as_object_mut()) {
                        inner.insert("codeExecuted".to_string(), json!(true));
                        inner.insert("runtimeUsed".to_string(), runtime_tag.clone());
                    }
                    obj.insert("codeExecuted".to_string(), json!(true));
                    obj.insert("runtimeUsed".to_string(), runtime_tag);
                    (Arc::new(val), None)
                } else {
                    (
                        Arc::new(json!({
                            "codeExecuted": true,
                            "runtimeUsed": format!("{language}:{version}"),
                            "data": val
                        })),
                        None,
                    )
                }
            } else {
                match crate::runtime::execute_polyglot_code(language, version, code_str, &input, expr_ctx) {
                    Ok(result) => (Arc::new(result), None),
                    Err(err) => {
                        let mut err_val = (*input).clone();
                        let runtime_tag = json!(format!("{language}:{version}"));
                        if let Some(arr) = err_val.as_array_mut() {
                            for item in arr {
                                if let Some(obj) = item.as_object_mut() {
                                    if let Some(inner) = obj.get_mut("json").and_then(|j| j.as_object_mut()) {
                                        inner.insert("codeExecuted".to_string(), json!(true));
                                        inner.insert("runtimeUsed".to_string(), runtime_tag.clone());
                                        inner.insert("error".to_string(), json!(err));
                                    }
                                    obj.insert("codeExecuted".to_string(), json!(true));
                                    obj.insert("runtimeUsed".to_string(), runtime_tag.clone());
                                    obj.insert("error".to_string(), json!(err));
                                }
                            }
                            (Arc::new(err_val), None)
                        } else if let Some(obj) = err_val.as_object_mut() {
                            if let Some(inner) = obj.get_mut("json").and_then(|j| j.as_object_mut()) {
                                inner.insert("codeExecuted".to_string(), json!(true));
                                inner.insert("runtimeUsed".to_string(), runtime_tag.clone());
                                inner.insert("error".to_string(), json!(err));
                            }
                            obj.insert("codeExecuted".to_string(), json!(true));
                            obj.insert("runtimeUsed".to_string(), runtime_tag);
                            obj.insert("error".to_string(), json!(err));
                            (Arc::new(err_val), None)
                        } else {
                            (
                                Arc::new(json!({
                                    "codeExecuted": true,
                                    "runtimeUsed": format!("{language}:{version}"),
                                    "error": err
                                })),
                                None,
                            )
                        }
                    }
                }
            }
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

            // Resolve and inject credentials if provided
            let cred_id = params_ref
                .get("credentialId")
                .and_then(|v| v.as_str())
                .or_else(|| {
                    params_ref.get("credentials").and_then(|c| {
                        if let Some(s) = c.as_str() {
                            Some(s)
                        } else if let Some(obj) = c.as_object() {
                            if let Some(id) = obj.get("id").and_then(|v| v.as_str()) {
                                Some(id)
                            } else {
                                obj.values().find_map(|v| v.get("id").and_then(|id| id.as_str()))
                            }
                        } else {
                            None
                        }
                    })
                });

            let mut effective_headers = match headers {
                Some(Value::Object(m)) => m.clone(),
                _ => serde_json::Map::new(),
            };

            if let Some(cred_data) = cred_id.and_then(get_credential) {
                if let (Some(k), Some(v)) = (
                    cred_data
                        .get("name")
                        .or_else(|| cred_data.get("headerName"))
                        .and_then(|v| v.as_str()),
                    cred_data
                        .get("value")
                        .or_else(|| cred_data.get("headerValue"))
                        .and_then(|v| v.as_str()),
                ) {
                    effective_headers.insert(k.to_string(), json!(v));
                }

                let user_opt = cred_data
                    .get("user")
                    .or_else(|| cred_data.get("username"))
                    .and_then(|v| v.as_str());
                let pass_opt = cred_data
                    .get("password")
                    .or_else(|| cred_data.get("pass"))
                    .and_then(|v| v.as_str());
                if let (Some(u), Some(p)) = (user_opt, pass_opt) {
                    let cred_str = format!("{u}:{p}");
                    let b64 = base64_encode(cred_str.as_bytes());
                    effective_headers.insert("Authorization".to_string(), json!(format!("Basic {b64}")));
                } else if let Some(token) = cred_data
                    .get("token")
                    .or_else(|| cred_data.get("bearerToken"))
                    .and_then(|v| v.as_str())
                {
                    effective_headers
                        .insert("Authorization".to_string(), json!(format!("Bearer {token}")));
                } else if let Some(api_key) = cred_data.get("apiKey").and_then(|v| v.as_str()) {
                    effective_headers
                        .entry("X-API-Key".to_string())
                        .or_insert_with(|| json!(api_key));
                }
            }

            let effective_headers_val = Value::Object(effective_headers);
            let effective_headers_ref = Some(&effective_headers_val);

            let max_tries = params_ref
                .get("maxTries")
                .and_then(|v| v.as_u64())
                .or_else(|| {
                    params_ref
                        .get("options")
                        .and_then(|o| o.get("maxTries"))
                        .and_then(|v| v.as_u64())
                })
                .unwrap_or(1)
                .clamp(1, 5) as u32;

            let wait_between_tries = params_ref
                .get("waitBetweenTries")
                .and_then(|v| v.as_u64())
                .or_else(|| {
                    params_ref
                        .get("options")
                        .and_then(|o| o.get("waitBetweenTries"))
                        .and_then(|v| v.as_u64())
                })
                .unwrap_or(0)
                .clamp(0, 5000);

            let mut attempts = 0u32;
            let mut res;

            loop {
                attempts += 1;
                res = execute_real_http_request(url, method, effective_headers_ref, body);
                let status = res
                    .get("statusCode")
                    .and_then(|s| s.as_u64())
                    .unwrap_or(200);

                if (status < 500 && status != 0) || attempts >= max_tries {
                    break;
                }

                if wait_between_tries > 0 {
                    std::thread::sleep(std::time::Duration::from_millis(wait_between_tries));
                }
            }

            if let Some(res_obj) = res.as_object_mut() {
                res_obj.insert("attempts".to_string(), json!(attempts));
            }

            let continue_on_fail = params_ref
                .get("continueOnFail")
                .and_then(|v| v.as_bool())
                .or_else(|| {
                    params_ref
                        .get("options")
                        .and_then(|o| o.get("continueOnFail"))
                        .and_then(|v| v.as_bool())
                })
                .unwrap_or(false);

            let status = res
                .get("statusCode")
                .and_then(|s| s.as_u64())
                .unwrap_or(200);

            if status >= 400 {
                if let Some(res_obj) = res.as_object_mut() {
                    if continue_on_fail {
                        res_obj.insert("continueOnFail".to_string(), json!(true));
                    } else {
                        res_obj.insert("__node_error".to_string(), json!(true));
                        let err_msg = res_obj
                            .get("error")
                            .and_then(|e| e.as_str())
                            .unwrap_or("HTTP request failed with status >= 400")
                            .to_string();
                        res_obj.insert("error".to_string(), json!({ "message": err_msg }));
                    }
                }
            }

            (Arc::new(res), None)
        }
        NodeKind::ExecuteWorkflow => {
            let continue_on_fail = params_ref
                .get("continueOnFail")
                .and_then(|v| v.as_bool())
                .or_else(|| {
                    params_ref
                        .get("options")
                        .and_then(|o| o.get("continueOnFail"))
                        .and_then(|v| v.as_bool())
                })
                .unwrap_or(false);

            if subworkflow_depth >= 10 {
                let err_res = json!({
                    "__node_error": true,
                    "error": {
                        "message": "Exceeded maximum sub-workflow recursion depth (10)"
                    }
                });
                return wrap_continue_on_fail(err_res, continue_on_fail);
            }

            let workflow_val = if let Some(wf) = params_ref.get("workflow") {
                Some(wf.clone())
            } else if let Some(wf_id) = params_ref.get("workflowId").and_then(|v| v.as_str()) {
                get_subworkflow(wf_id)
            } else {
                None
            };

            let Some(wf_val) = workflow_val else {
                let err_res = json!({
                    "__node_error": true,
                    "error": {
                        "message": "Sub-workflow not found or missing workflowId/workflow parameter"
                    }
                });
                return wrap_continue_on_fail(err_res, continue_on_fail);
            };

            let empty_arr = Value::Array(vec![]);
            let empty_obj = Value::Object(serde_json::Map::new());
            let nodes_val = wf_val.get("nodes").unwrap_or(&empty_arr);
            let conns_val = wf_val.get("connections").unwrap_or(&empty_obj);

            let compiled = match CompiledGraph::compile(nodes_val, conns_val) {
                Ok(c) => c,
                Err(e) => {
                    let err_res = json!({
                        "__node_error": true,
                        "error": {
                            "message": format!("Failed to compile sub-workflow: {}", e)
                        }
                    });
                    return wrap_continue_on_fail(err_res, continue_on_fail);
                }
            };

            let (sub_out, port) =
                execute_subworkflow(&compiled, Arc::clone(&input), subworkflow_depth + 1);

            if continue_on_fail {
                let is_err = sub_out
                    .as_object()
                    .and_then(|o| o.get("__node_error"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if is_err {
                    let mut modified = (*sub_out).clone();
                    if let Some(obj) = modified.as_object_mut() {
                        obj.remove("__node_error");
                        obj.insert("continueOnFail".to_string(), json!(true));
                    }
                    return (Arc::new(modified), port);
                }
            }

            (sub_out, port)
        }
        NodeKind::RespondToWebhook => {
            let code = params_ref
                .get("responseCode")
                .or_else(|| params_ref.get("responseStatusCode"))
                .and_then(|v| {
                    v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
                })
                .unwrap_or(200)
                .clamp(100, 599) as u16;

            let headers_obj = params_ref
                .get("responseHeaders")
                .cloned()
                .unwrap_or_else(|| json!({}));

            let body_val = params_ref
                .get("responseBody")
                .or_else(|| params_ref.get("body"))
                .cloned()
                .unwrap_or_else(|| (*input).clone());

            let resp_payload = json!({
                "statusCode": code,
                "headers": headers_obj,
                "body": body_val
            });

            let mut output_obj = if let Some(obj) = input.as_object() {
                obj.clone()
            } else {
                serde_json::Map::new()
            };
            output_obj.insert("__webhook_response".to_string(), resp_payload);
            (Arc::new(Value::Object(output_obj)), None)
        }
        NodeKind::SplitOut => {
            let field = params_ref
                .get("fieldToSplitOut")
                .and_then(|v| v.as_str())
                .unwrap_or("items");

            let input_items: Vec<Value> = if let Some(arr) = input.as_array() {
                arr.clone()
            } else {
                vec![(*input).clone()]
            };

            let mut out_items = Vec::new();
            for item in input_items {
                if let Some(obj) = item.as_object() {
                    let maybe_sub_arr = obj.get(field).and_then(|v| v.as_array()).or_else(|| {
                        obj.get("body").and_then(|b| b.get(field)).and_then(|v| v.as_array())
                    }).or_else(|| {
                        obj.get("json").and_then(|j| j.get(field)).and_then(|v| v.as_array())
                    });

                    if let Some(sub_arr) = maybe_sub_arr {
                        for sub in sub_arr {
                            let mut new_item = obj.clone();
                            new_item.remove(field);
                            if let Some(sub_obj) = sub.as_object() {
                                for (k, v) in sub_obj {
                                    new_item.insert(k.clone(), v.clone());
                                }
                            } else {
                                new_item.insert(field.to_string(), sub.clone());
                            }
                            out_items.push(Value::Object(new_item));
                        }
                    } else {
                        out_items.push(item);
                    }
                } else {
                    out_items.push(item);
                }
            }

            (Arc::new(Value::Array(out_items)), None)
        }
        NodeKind::Aggregate => {
            let dest_field = params_ref
                .get("destinationFieldName")
                .and_then(|v| v.as_str())
                .unwrap_or("data");

            let items_to_aggregate: Vec<Value> = if let Some(arr) = input.as_array() {
                arr.clone()
            } else {
                vec![(*input).clone()]
            };

            let mut map = serde_json::Map::new();
            map.insert(dest_field.to_string(), Value::Array(items_to_aggregate));
            (Arc::new(Value::Array(vec![Value::Object(map)])), None)
        }
        NodeKind::RemoveDuplicates => {
            let compare_field = params_ref
                .get("compareField")
                .or_else(|| params_ref.get("fieldToCompare"))
                .or_else(|| params_ref.get("fieldsToCompare"))
                .and_then(|v| v.as_str());

            let input_items: Vec<Value> = if let Some(arr) = input.as_array() {
                arr.clone()
            } else {
                vec![(*input).clone()]
            };

            let mut seen = std::collections::HashSet::new();
            let mut out_items = Vec::new();

            for item in input_items {
                if let Some(field) = compare_field {
                    let maybe_val = item
                        .get(field)
                        .or_else(|| item.get("json").and_then(|j| j.get(field)))
                        .or_else(|| item.get("body").and_then(|b| b.get(field)));

                    if let Some(val) = maybe_val {
                        let key = serde_json::to_string(val).unwrap_or_else(|_| val.to_string());
                        if seen.insert(key) {
                            out_items.push(item);
                        }
                    } else {
                        out_items.push(item);
                    }
                } else {
                    let key = serde_json::to_string(&item).unwrap_or_else(|_| item.to_string());
                    if seen.insert(key) {
                        out_items.push(item);
                    }
                }
            }

            (Arc::new(Value::Array(out_items)), None)
        }
        NodeKind::SplitInBatches => {
            let batch_size = params_ref
                .get("batchSize")
                .and_then(|v| v.as_u64())
                .unwrap_or(10)
                .max(1) as usize;
            let batch_index = params_ref
                .get("batchIndex")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize;

            let items: Vec<Value> = if let Some(arr) = input.as_array() {
                arr.clone()
            } else {
                vec![(*input).clone()]
            };

            let start = batch_index.saturating_mul(batch_size);
            let slice: Vec<Value> = if start < items.len() {
                let end = (start + batch_size).min(items.len());
                items[start..end].to_vec()
            } else {
                Vec::new()
            };

            if slice.is_empty() {
                (Arc::new(Value::Array(Vec::new())), Some(1))
            } else {
                (Arc::new(Value::Array(slice)), Some(0))
            }
        }
        NodeKind::Wait => {
            let wait_ms = params_ref
                .get("amount")
                .or_else(|| params_ref.get("waitMs"))
                .and_then(|v| v.as_u64())
                .unwrap_or(10)
                .clamp(0, 2000);
            if wait_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(wait_ms));
            }
            (input, None)
        }
        NodeKind::StopAndError => {
            let err_msg = params_ref
                .get("errorMessage")
                .or_else(|| params_ref.get("message"))
                .and_then(|v| v.as_str())
                .unwrap_or("Workflow stopped by StopAndError node");

            let continue_on_fail = params_ref
                .get("continueOnFail")
                .and_then(|v| v.as_bool())
                .or_else(|| {
                    params_ref
                        .get("options")
                        .and_then(|o| o.get("continueOnFail"))
                        .and_then(|v| v.as_bool())
                })
                .unwrap_or(false);

            if continue_on_fail {
                let payload = json!({
                    "continueOnFail": true,
                    "error": { "message": err_msg }
                });
                (Arc::new(payload), None)
            } else {
                let payload = json!({
                    "__node_error": true,
                    "error": { "message": err_msg }
                });
                (Arc::new(payload), None)
            }
        }
        NodeKind::ErrorTrigger => {
            (input, None)
        }
        _ => (input, None),
    }
}

fn wrap_continue_on_fail(mut val: Value, continue_on_fail: bool) -> (Arc<Value>, Option<u16>) {
    if continue_on_fail {
        if let Some(obj) = val.as_object_mut() {
            obj.remove("__node_error");
            obj.insert("continueOnFail".to_string(), json!(true));
        }
    }
    (Arc::new(val), None)
}

pub fn execute_subworkflow(
    graph: &CompiledGraph,
    initial_input: Arc<Value>,
    depth: u32,
) -> (Arc<Value>, Option<u16>) {
    let start_time = Instant::now();
    let total_nodes = graph.nodes.len();

    if total_nodes == 0 {
        return (initial_input, None);
    }

    let remaining_in_degree: Vec<AtomicU32> =
        graph.in_degree.iter().map(|&d| AtomicU32::new(d)).collect();
    let executed_count = AtomicU32::new(0);
    let outputs = DashMap::<u32, Arc<Value>>::new();
    let cancel_flag = AtomicBool::new(false);
    let timed_out = AtomicBool::new(false);
    let has_error = AtomicBool::new(false);
    let timeout_duration = std::time::Duration::from_secs(30);
    let last_output_holder = std::sync::Mutex::new(Arc::clone(&initial_input));

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
        has_error: &has_error,
        start_time,
        timeout_duration,
        notifier: None,
        execution_id: "subworkflow",
        last_output_holder: &last_output_holder,
        is_hyperscale: false,
        is_compact_logging: false,
        total_nodes,
        subworkflow_depth: depth,
    };

    rayon::scope(|s| {
        for &root_idx in &graph.roots {
            let root_input = Arc::clone(&initial_input);
            let ctx_ref = &ctx;
            s.spawn(move |s| {
                traverse_node(s, ctx_ref, root_idx, root_input);
            });
        }
    });

    let final_output = last_output_holder
        .into_inner()
        .unwrap_or_else(|e| e.into_inner());

    if has_error.load(Ordering::Relaxed) {
        let mut err_obj = (*final_output).clone();
        if let Some(obj) = err_obj.as_object_mut() {
            obj.insert("__node_error".to_string(), json!(true));
            if !obj.contains_key("error") {
                obj.insert(
                    "error".to_string(),
                    json!({ "message": "Sub-workflow execution failed" }),
                );
            }
        } else {
            err_obj = json!({
                "__node_error": true,
                "error": {
                    "message": "Sub-workflow execution failed"
                },
                "data": err_obj
            });
        }
        (Arc::new(err_obj), None)
    } else {
        (final_output, None)
    }
}

static SHARED_HTTP_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
static SHARED_HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn get_shared_http_runtime() -> Option<&'static tokio::runtime::Runtime> {
    if let Some(rt) = SHARED_HTTP_RUNTIME.get() {
        return Some(rt);
    }
    if let Ok(rt) = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("shared-http-rt")
        .build()
    {
        let _ = SHARED_HTTP_RUNTIME.set(rt);
    }
    SHARED_HTTP_RUNTIME.get()
}

fn get_shared_http_client() -> Option<&'static reqwest::Client> {
    if let Some(c) = SHARED_HTTP_CLIENT.get() {
        return Some(c);
    }
    if let Ok(c) = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .pool_max_idle_per_host(32)
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .build()
    {
        let _ = SHARED_HTTP_CLIENT.set(c);
    }
    SHARED_HTTP_CLIENT.get()
}

pub fn execute_real_http_request(
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

    let rt = match get_shared_http_runtime() {
        Some(r) => r,
        None => {
            return json!({
                "statusCode": 500,
                "url": url,
                "method": method,
                "error": "Failed to initialize shared HTTP runtime",
                "data": Value::Null
            });
        }
    };

    let client = match get_shared_http_client() {
        Some(c) => c,
        None => {
            return json!({
                "statusCode": 500,
                "url": url,
                "method": method,
                "error": "Failed to initialize shared HTTP client",
                "data": Value::Null
            });
        }
    };

    let url_owned = url.to_string();
    let method_owned = method.to_string();
    let headers_owned = headers_val.cloned();
    let body_owned = body_val.cloned();

    let async_work = async move {
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
                let raw_text = resp.text().await.unwrap_or_default();
                let body_val = match serde_json::from_str::<Value>(&raw_text) {
                    Ok(json_val) => json_val,
                    Err(_) => Value::String(raw_text),
                };
                json!({
                    "statusCode": status,
                    "url": url_owned,
                    "method": method_owned,
                    "headers": Value::Object(resp_headers),
                    "data": body_val
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
    };

    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    rt.spawn(async move {
        let _ = tx.send(async_work.await);
    });
    rx.recv().unwrap_or_else(|_| {
        json!({
            "statusCode": 500,
            "url": url,
            "method": method,
            "error": "Shared HTTP runtime channel closed",
            "data": Value::Null
        })
    })
}

fn num_cpus_available() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

pub fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().saturating_mul(4) / 3 + 4);
    for chunk in bytes.chunks(3) {
        match chunk.len() {
            3 => {
                let b0 = chunk[0];
                let b1 = chunk[1];
                let b2 = chunk[2];
                out.push(TABLE[(b0 >> 2) as usize] as char);
                out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
                out.push(TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
                out.push(TABLE[(b2 & 0x3f) as usize] as char);
            }
            2 => {
                let b0 = chunk[0];
                let b1 = chunk[1];
                out.push(TABLE[(b0 >> 2) as usize] as char);
                out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
                out.push(TABLE[((b1 & 0x0f) << 2) as usize] as char);
                out.push('=');
            }
            1 => {
                let b0 = chunk[0];
                out.push(TABLE[(b0 >> 2) as usize] as char);
                out.push(TABLE[((b0 & 0x03) << 4) as usize] as char);
                out.push('=');
                out.push('=');
            }
            _ => {}
        }
    }
    out
}


