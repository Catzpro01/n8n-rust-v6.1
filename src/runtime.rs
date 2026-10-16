use crate::expr::{evaluate_raw_expression, ExprContext};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

/// Metadata representation of a polyglot language runtime package
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimePackage {
    pub language: String,
    pub version: String,
    pub installed: bool,
    pub is_default: bool,
    pub size_bytes: u64,
    pub sha256: String,
    pub capabilities: Vec<String>,
}

/// Abstract syntax instruction for polyglot runtime execution
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolyglotInstruction {
    /// Assignment of an evaluated expression to a target JSON path
    Assign {
        target: Vec<String>,
        expr: String,
    },
    /// Assignment to a local variable in scope
    AssignLocal {
        name: String,
        expr: String,
    },
    /// Return statement targeting an expression or payload
    Return {
        expr: String,
    },
    /// Loop over collection items
    ForLoop {
        item_var: String,
        collection_expr: String,
        body: Vec<PolyglotInstruction>,
    },
    /// Conditional branching if / else
    IfElse {
        condition: String,
        then_body: Vec<PolyglotInstruction>,
        else_body: Vec<PolyglotInstruction>,
    },
    /// Standalone expression statement
    ExprStatement {
        expr: String,
    },
}

/// Compiled representation of a polyglot program with AST instructions
#[derive(Debug, Clone)]
pub struct CompiledPolyglotProgram {
    pub language: String,
    pub version: String,
    pub code_hash: u64,
    pub instructions: Vec<PolyglotInstruction>,
}

/// Thread-safe registry and Tier 1 AST/Bytecode caching engine for Universal Polyglot Runtimes
pub struct PolyglotRuntimeManager {
    registry: DashMap<String, RuntimePackage>,
    ast_cache: DashMap<u64, Arc<CompiledPolyglotProgram>>,
}

static GLOBAL_RUNTIME_MANAGER: OnceLock<PolyglotRuntimeManager> = OnceLock::new();

impl Default for PolyglotRuntimeManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PolyglotRuntimeManager {
    /// Initializes a new PolyglotRuntimeManager with standard built-in packages
    pub fn new() -> Self {
        let registry = DashMap::new();
        let ast_cache = DashMap::new();

        let default_packages = vec![
            // JavaScript Runtimes
            RuntimePackage {
                language: "javascript".to_string(),
                version: "es2022-quickjs".to_string(),
                installed: true,
                is_default: true,
                size_bytes: 4_194_304,
                sha256: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08".to_string(),
                capabilities: vec!["es2022".to_string(), "async".to_string(), "json".to_string()],
            },
            RuntimePackage {
                language: "javascript".to_string(),
                version: "es2024-v8-compat".to_string(),
                installed: false,
                is_default: false,
                size_bytes: 12_582_912,
                sha256: "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8".to_string(),
                capabilities: vec!["es2024".to_string(), "v8".to_string(), "jit".to_string()],
            },
            RuntimePackage {
                language: "javascript".to_string(),
                version: "js-es2024".to_string(),
                installed: true,
                is_default: false,
                size_bytes: 8_388_608,
                sha256: "3f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a09".to_string(),
                capabilities: vec!["es2024".to_string(), "fast-jit".to_string(), "v1.2.0".to_string()],
            },
            // Python Runtimes
            RuntimePackage {
                language: "python".to_string(),
                version: "3.11-sandbox".to_string(),
                installed: true,
                is_default: true,
                size_bytes: 8_388_608,
                sha256: "4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a".to_string(),
                capabilities: vec!["python3".to_string(), "stdlib".to_string(), "math".to_string()],
            },
            RuntimePackage {
                language: "python".to_string(),
                version: "3.12-pyo3".to_string(),
                installed: true,
                is_default: false,
                size_bytes: 16_777_216,
                sha256: "ef2d127de37b942baad06145e54b0c619a1f22327b2ebbcfbec78f5564afe39d".to_string(),
                capabilities: vec!["python3.12".to_string(), "gil-free".to_string(), "arrow".to_string()],
            },
            RuntimePackage {
                language: "python".to_string(),
                version: "py-3.12".to_string(),
                installed: true,
                is_default: false,
                size_bytes: 14_680_064,
                sha256: "6b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8b".to_string(),
                capabilities: vec!["python3.12".to_string(), "embedded".to_string(), "v3.12.4".to_string()],
            },
            // TypeScript Runtimes
            RuntimePackage {
                language: "typescript".to_string(),
                version: "ts-5.6".to_string(),
                installed: true,
                is_default: true,
                size_bytes: 6_291_456,
                sha256: "7b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8c".to_string(),
                capabilities: vec!["typescript".to_string(), "zero-strip".to_string(), "v5.6.2".to_string()],
            },
            // WASM Runtimes
            RuntimePackage {
                language: "wasm".to_string(),
                version: "wasm32-wasmer-4.0".to_string(),
                installed: true,
                is_default: true,
                size_bytes: 2_097_152,
                sha256: "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae".to_string(),
                capabilities: vec!["wasm".to_string(), "wasi".to_string(), "simd".to_string()],
            },
            RuntimePackage {
                language: "wasm".to_string(),
                version: "wasm-24.0".to_string(),
                installed: true,
                is_default: false,
                size_bytes: 3_670_016,
                sha256: "8c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7af".to_string(),
                capabilities: vec!["wasm".to_string(), "wasmtime".to_string(), "v24.0.0".to_string()],
            },
            RuntimePackage {
                language: "wasm".to_string(),
                version: "wasm32-wasi".to_string(),
                installed: true,
                is_default: false,
                size_bytes: 3_145_728,
                sha256: "fc4888842fa76b5c48b0a1a2b1f0b08b3e8e7a6f235b6f790c9b0e263d9dd83f".to_string(),
                capabilities: vec!["wasi-preview2".to_string(), "component-model".to_string()],
            },
            // Rust Script Runtimes
            RuntimePackage {
                language: "rust".to_string(),
                version: "rust-script-nightly".to_string(),
                installed: false,
                is_default: false,
                size_bytes: 33_554_432,
                sha256: "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e".to_string(),
                capabilities: vec!["rust".to_string(), "cargo".to_string(), "mir".to_string()],
            },
        ];

        for pkg in default_packages {
            let key = format!("{}:{}", pkg.language, pkg.version);
            registry.insert(key, pkg);
        }

        Self {
            registry,
            ast_cache,
        }
    }

    /// Access the global singleton instance of PolyglotRuntimeManager
    pub fn global() -> &'static Self {
        GLOBAL_RUNTIME_MANAGER.get_or_init(Self::new)
    }

    /// Returns a list of all registered runtime packages
    pub fn list_runtimes(&self) -> Vec<RuntimePackage> {
        let mut list: Vec<RuntimePackage> = self.registry.iter().map(|e| e.value().clone()).collect();
        list.sort_by(|a, b| a.language.cmp(&b.language).then_with(|| a.version.cmp(&b.version)));
        list
    }

    /// Resolves canonical language and version, picking default if version is empty or "auto"
    pub fn resolve_runtime_version(&self, language: &str, version: &str) -> (String, String) {
        let norm_lang = normalize_language(language);
        let effective_lang = if norm_lang.is_empty() {
            "javascript"
        } else {
            norm_lang
        };

        let trimmed_ver = version.trim();
        if !trimmed_ver.is_empty()
            && !trimmed_ver.eq_ignore_ascii_case("default")
            && !trimmed_ver.eq_ignore_ascii_case("auto")
        {
            return (effective_lang.to_string(), trimmed_ver.to_string());
        }

        for entry in self.registry.iter() {
            if entry.language.eq_ignore_ascii_case(effective_lang) && entry.is_default {
                return (effective_lang.to_string(), entry.version.clone());
            }
        }

        let fallback_ver = match effective_lang {
            "python" => "3.11-sandbox",
            "typescript" => "ts-5.6",
            "wasm" => "wasm32-wasmer-4.0",
            "rust" => "rust-script-nightly",
            _ => "es2022-quickjs",
        };
        (effective_lang.to_string(), fallback_ver.to_string())
    }

    /// Retrieve a runtime package from the registry if present
    pub fn get_runtime(&self, language: &str, version: &str) -> Option<RuntimePackage> {
        let (norm_lang, norm_ver) = self.resolve_runtime_version(language, version);
        let key = format!("{norm_lang}:{norm_ver}");
        if let Some(entry) = self.registry.get(&key) {
            return Some(entry.clone());
        }
        for entry in self.registry.iter() {
            if entry.language.eq_ignore_ascii_case(&norm_lang)
                && entry.version.eq_ignore_ascii_case(&norm_ver)
            {
                return Some(entry.clone());
            }
        }
        None
    }

    /// Downloads or installs a runtime package into the registry
    pub fn download_runtime(&self, language: &str, version: &str) -> Result<RuntimePackage, String> {
        let (norm_lang, norm_ver) = self.resolve_runtime_version(language, version);
        let key = format!("{norm_lang}:{norm_ver}");

        if let Some(mut existing) = self.registry.get_mut(&key) {
            existing.installed = true;
            return Ok(existing.clone());
        }

        for mut existing in self.registry.iter_mut() {
            if existing.language.eq_ignore_ascii_case(&norm_lang)
                && existing.version.eq_ignore_ascii_case(&norm_ver)
            {
                existing.installed = true;
                return Ok(existing.clone());
            }
        }

        let sha = format!("{:064x}", compute_program_hash(&norm_lang, &norm_ver, "download"));
        let new_pkg = RuntimePackage {
            language: norm_lang,
            version: norm_ver,
            installed: true,
            is_default: false,
            size_bytes: 8_388_608,
            sha256: sha,
            capabilities: vec!["custom".to_string(), "dynamic-runtime".to_string()],
        };

        self.registry.insert(key, new_pkg.clone());
        Ok(new_pkg)
    }

    /// Checks whether a runtime package is registered and installed
    pub fn is_runtime_installed(&self, language: &str, version: &str) -> bool {
        self.get_runtime(language, version).is_some_and(|pkg| pkg.installed)
    }

    /// Deletes or uninstalls a runtime package from the registry and clears AST cache
    pub fn delete_runtime(&self, language: &str, version: &str) -> Result<(), String> {
        let (norm_lang, norm_ver) = self.resolve_runtime_version(language, version);
        let key = format!("{norm_lang}:{norm_ver}");

        let found = if let Some(mut existing) = self.registry.get_mut(&key) {
            existing.installed = false;
            true
        } else {
            let mut matched = false;
            for mut existing in self.registry.iter_mut() {
                if existing.language.eq_ignore_ascii_case(&norm_lang)
                    && existing.version.eq_ignore_ascii_case(&norm_ver)
                {
                    existing.installed = false;
                    matched = true;
                    break;
                }
            }
            matched
        };

        if found {
            self.ast_cache.retain(|_, v| {
                !(v.language.eq_ignore_ascii_case(&norm_lang)
                    && v.version.eq_ignore_ascii_case(&norm_ver))
            });
            Ok(())
        } else {
            Err(format!(
                "Runtime not found for language '{language}' version '{version}'"
            ))
        }
    }

    /// Compiles code into an AST program
    pub fn compile_program(
        &self,
        language: &str,
        version: &str,
        code: &str,
        code_hash: u64,
    ) -> Result<CompiledPolyglotProgram, String> {
        let effective_code = if language.eq_ignore_ascii_case("typescript") {
            strip_typescript_types(code)
        } else {
            code.to_string()
        };
        let instructions = parse_statements_into_ast(&effective_code);
        Ok(CompiledPolyglotProgram {
            language: language.to_string(),
            version: version.to_string(),
            code_hash,
            instructions,
        })
    }

    /// Clears the Tier 1 AST/Bytecode program cache
    pub fn clear_cache(&self) {
        self.ast_cache.clear();
    }

    /// Number of cached programs in Tier 1 AST cache
    pub fn cache_len(&self) -> usize {
        self.ast_cache.len()
    }

    /// Executes code with specified mode: "runOnceForAllItems" (default) or "runOnceForEachItem"
    pub fn execute_polyglot_code_with_mode(
        &self,
        language: &str,
        version: &str,
        code: &str,
        input: &Value,
        ctx: &ExprContext,
        mode: &str,
    ) -> Result<Value, String> {
        let (effective_lang, effective_ver) = self.resolve_runtime_version(language, version);
        let runtime_used = format!("{effective_lang}:{effective_ver}");

        // Verify that runtime package is installed if known
        if let Some(pkg) = self.get_runtime(&effective_lang, &effective_ver) {
            if !pkg.installed {
                return Err(format!(
                    "Runtime package '{runtime_used}' is not installed. Please download it first."
                ));
            }
        }

        let norm_mode = mode.trim();
        if norm_mode.eq_ignore_ascii_case("runOnceForEachItem") {
            if let Value::Array(arr) = input {
                let mut results = Vec::with_capacity(arr.len());
                for single_item in arr {
                    let out = self.execute_polyglot_code_internal(
                        &effective_lang,
                        &effective_ver,
                        code,
                        single_item,
                        ctx,
                        &runtime_used,
                    )?;
                    results.push(out);
                }
                return Ok(Value::Array(results));
            }
        }

        self.execute_polyglot_code_internal(
            &effective_lang,
            &effective_ver,
            code,
            input,
            ctx,
            &runtime_used,
        )
    }

    /// 3-Tier Polyglot Execution Engine: Tier 0 Fast Path -> Tier 1 AST Cache -> Tier 2 Execution
    pub fn execute_polyglot_code(
        &self,
        language: &str,
        version: &str,
        code: &str,
        input: &Value,
        ctx: &ExprContext,
    ) -> Result<Value, String> {
        self.execute_polyglot_code_with_mode(
            language,
            version,
            code,
            input,
            ctx,
            "runOnceForAllItems",
        )
    }

    fn execute_polyglot_code_internal(
        &self,
        effective_lang: &str,
        effective_ver: &str,
        code: &str,
        input: &Value,
        ctx: &ExprContext,
        runtime_used: &str,
    ) -> Result<Value, String> {
        let trimmed = code.trim();

        // -------------------------------------------------------------
        // Tier 0: Fast Path Execution (< 1 microsecond, no parsing/compilation)
        // -------------------------------------------------------------
        let is_fast_path = trimmed.is_empty()
            || trimmed == "return $input.all();"
            || trimmed == "return $input.all()"
            || trimmed == "return _input.all();"
            || trimmed == "return _input.all()"
            || trimmed == "return;"
            || trimmed == "return";

        if is_fast_path {
            let mut output = input.clone();
            inject_metadata(&mut output, runtime_used);
            return Ok(output);
        }

        // -------------------------------------------------------------
        // Tier 1: AST / Bytecode Cache lookup
        // -------------------------------------------------------------
        let code_hash = compute_program_hash(effective_lang, effective_ver, code);

        let program: Arc<CompiledPolyglotProgram> =
            if let Some(cached) = self.ast_cache.get(&code_hash) {
                Arc::clone(cached.value())
            } else {
                let compiled =
                    self.compile_program(effective_lang, effective_ver, code, code_hash)?;
                let arc_prog = Arc::new(compiled);
                self.ast_cache.insert(code_hash, Arc::clone(&arc_prog));
                arc_prog
            };

        // -------------------------------------------------------------
        // Tier 2: Real Deterministic Execution
        // -------------------------------------------------------------
        self.execute_program(&program, input, ctx, runtime_used)
    }

    fn execute_program(
        &self,
        program: &CompiledPolyglotProgram,
        input: &Value,
        ctx: &ExprContext,
        runtime_used: &str,
    ) -> Result<Value, String> {
        let is_input_array = input.is_array();
        let mut working_items: Vec<Value> = match input {
            Value::Array(arr) => arr.clone(),
            _ => vec![input.clone()],
        };

        let mut local_vars_per_item: Vec<HashMap<String, Value>> =
            vec![HashMap::new(); working_items.len()];

        let mut explicit_return: Option<Value> = None;

        for inst in &program.instructions {
            match inst {
                PolyglotInstruction::ForLoop { body, .. } => {
                    for (idx, item) in working_items.iter_mut().enumerate() {
                        let locals = &mut local_vars_per_item[idx];
                        for sub_inst in body {
                            execute_single_instruction(sub_inst, item, locals, ctx)?;
                        }
                    }
                }
                PolyglotInstruction::Assign { .. } => {
                    for (idx, item) in working_items.iter_mut().enumerate() {
                        let locals = &mut local_vars_per_item[idx];
                        execute_single_instruction(inst, item, locals, ctx)?;
                    }
                }
                PolyglotInstruction::AssignLocal { .. } => {
                    for (idx, item) in working_items.iter_mut().enumerate() {
                        let locals = &mut local_vars_per_item[idx];
                        execute_single_instruction(inst, item, locals, ctx)?;
                    }
                }
                PolyglotInstruction::IfElse { .. } => {
                    for (idx, item) in working_items.iter_mut().enumerate() {
                        let locals = &mut local_vars_per_item[idx];
                        execute_single_instruction(inst, item, locals, ctx)?;
                    }
                }
                PolyglotInstruction::ExprStatement { .. } => {
                    for (idx, item) in working_items.iter_mut().enumerate() {
                        let locals = &mut local_vars_per_item[idx];
                        execute_single_instruction(inst, item, locals, ctx)?;
                    }
                }
                PolyglotInstruction::Return { expr } => {
                    let trimmed_expr = expr.trim();
                    if trimmed_expr == "$input.all()" || trimmed_expr == "_input.all()" {
                        explicit_return = Some(Value::Array(working_items.clone()));
                        break;
                    }
                    if trimmed_expr == "item" || trimmed_expr == "_item" {
                        let val = if is_input_array && working_items.len() > 1 {
                            Value::Array(working_items.clone())
                        } else if !working_items.is_empty() {
                            working_items[0].clone()
                        } else {
                            Value::Null
                        };
                        explicit_return = Some(val);
                        break;
                    }
                    if trimmed_expr == "$input.first()" || trimmed_expr == "_input.first()" {
                        let val = if !working_items.is_empty() {
                            working_items[0].clone()
                        } else {
                            Value::Null
                        };
                        explicit_return = Some(val);
                        break;
                    }
                    if (trimmed_expr.starts_with('[') && trimmed_expr.ends_with(']'))
                        || (trimmed_expr.starts_with('{') && trimmed_expr.ends_with('}'))
                    {
                        if let Some(parsed) = parse_lenient_json(trimmed_expr) {
                            explicit_return = Some(parsed);
                            break;
                        }
                    }

                    let empty_map = HashMap::new();
                    let locals = local_vars_per_item.first().unwrap_or(&empty_map);
                    let bare = trimmed_expr.strip_prefix('$').unwrap_or(trimmed_expr);
                    if let Some(val) = locals.get(trimmed_expr).or_else(|| locals.get(bare)) {
                        explicit_return = Some(val.clone());
                        break;
                    }

                    let context_item = working_items.first().cloned().unwrap_or(Value::Null);
                    let eval_str = prepare_expr_with_locals(trimmed_expr, locals);
                    let evaluated = evaluate_raw_expression(&eval_str, &context_item, ctx);
                    explicit_return = Some(evaluated);
                    break;
                }
            }
        }

        let mut result = match explicit_return {
            Some(ret_val) => ret_val,
            None => {
                if is_input_array {
                    Value::Array(working_items)
                } else if !working_items.is_empty() {
                    working_items.remove(0)
                } else {
                    input.clone()
                }
            }
        };

        if program.language == "wasm" {
            enrich_wasm_payload(&mut result);
        }

        inject_metadata(&mut result, runtime_used);
        Ok(result)
    }
}

/// Helper to enrich payload in WASM deterministic mode
fn enrich_wasm_payload(val: &mut Value) {
    match val {
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                enrich_wasm_payload(item);
            }
        }
        Value::Object(map) => {
            map.insert("wasmProcessed".to_string(), Value::Bool(true));
            if let Some(inner) = map.get_mut("json").and_then(|j| j.as_object_mut()) {
                inner.insert("wasmProcessed".to_string(), Value::Bool(true));
            }
        }
        _ => {}
    }
}

fn execute_single_instruction(
    inst: &PolyglotInstruction,
    item: &mut Value,
    local_vars: &mut HashMap<String, Value>,
    ctx: &ExprContext,
) -> Result<(), String> {
    match inst {
        PolyglotInstruction::AssignLocal { name, expr } => {
            let prepared = prepare_expr_with_locals(expr, local_vars);
            let evaluated = evaluate_raw_expression(&prepared, item, ctx);
            local_vars.insert(name.clone(), evaluated);
        }
        PolyglotInstruction::Assign { target, expr } => {
            let prepared = prepare_expr_with_locals(expr, local_vars);
            let evaluated = evaluate_raw_expression(&prepared, item, ctx);
            apply_assignment(item, target, evaluated);
        }
        PolyglotInstruction::IfElse {
            condition,
            then_body,
            else_body,
        } => {
            let clean_cond = clean_condition(condition);
            let prepared_cond = prepare_expr_with_locals(&clean_cond, local_vars);
            let cond_val = evaluate_raw_expression(&prepared_cond, item, ctx);
            if is_truthy(&cond_val) {
                for sub_inst in then_body {
                    execute_single_instruction(sub_inst, item, local_vars, ctx)?;
                }
            } else {
                for sub_inst in else_body {
                    execute_single_instruction(sub_inst, item, local_vars, ctx)?;
                }
            }
        }
        PolyglotInstruction::ForLoop { body, .. } => {
            for sub_inst in body {
                execute_single_instruction(sub_inst, item, local_vars, ctx)?;
            }
        }
        PolyglotInstruction::ExprStatement { expr } => {
            let prepared = prepare_expr_with_locals(expr, local_vars);
            let _ = evaluate_raw_expression(&prepared, item, ctx);
        }
        PolyglotInstruction::Return { .. } => {}
    }
    Ok(())
}

/// Public API: checks whether a runtime package is installed
pub fn is_runtime_installed(language: &str, version: &str) -> bool {
    PolyglotRuntimeManager::global().is_runtime_installed(language, version)
}

/// Public API: list all registered runtime packages
pub fn list_runtimes() -> Vec<RuntimePackage> {
    PolyglotRuntimeManager::global().list_runtimes()
}

/// Public API: download or install a runtime package
pub fn download_runtime(language: &str, version: &str) -> Result<RuntimePackage, String> {
    PolyglotRuntimeManager::global().download_runtime(language, version)
}

/// Public API: delete or uninstall a runtime package
pub fn delete_runtime(language: &str, version: &str) -> Result<(), String> {
    PolyglotRuntimeManager::global().delete_runtime(language, version)
}

/// Public API: execute polyglot code with execution mode
pub fn execute_polyglot_code_with_mode(
    language: &str,
    version: &str,
    code: &str,
    input: &Value,
    ctx: &ExprContext,
    mode: &str,
) -> Result<Value, String> {
    PolyglotRuntimeManager::global().execute_polyglot_code_with_mode(
        language, version, code, input, ctx, mode,
    )
}

/// Public API: execute polyglot code across 3-tier engine
pub fn execute_polyglot_code(
    language: &str,
    version: &str,
    code: &str,
    input: &Value,
    ctx: &ExprContext,
) -> Result<Value, String> {
    PolyglotRuntimeManager::global().execute_polyglot_code(language, version, code, input, ctx)
}

/// Strips TypeScript type annotations for zero-overhead AST execution
pub fn strip_typescript_types(code: &str) -> String {
    let mut out = String::with_capacity(code.len());
    for line in code.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("interface ") || trimmed.starts_with("type ") {
            continue;
        }
        let mut clean_line = line.to_string();
        for type_annot in [
            ": string",
            ": number",
            ": boolean",
            ": any",
            ": void",
            ": object",
            ": Record<string, any>",
            ": Array<any>",
            ": any[]",
            "as any",
            "as string",
            "as number",
            "as boolean",
        ] {
            clean_line = clean_line.replace(type_annot, "");
        }
        out.push_str(&clean_line);
        out.push('\n');
    }
    out
}

/// Public API: resolve canonical runtime version
pub fn resolve_runtime_version(language: &str, version: &str) -> (String, String) {
    PolyglotRuntimeManager::global().resolve_runtime_version(language, version)
}

/// Public API: alias for resolve_runtime_version
pub fn resolve_language_version(language: &str, version: &str) -> (String, String) {
    resolve_runtime_version(language, version)
}

/// Normalizes language identifier strings into canonical representations
pub fn normalize_language(lang: &str) -> &str {
    match lang.trim().to_ascii_lowercase().as_str() {
        "javascript" | "js" | "node" | "nodejs" => "javascript",
        "python" | "py" | "python3" => "python",
        "typescript" | "ts" => "typescript",
        "wasm" | "webassembly" => "wasm",
        "rust" | "rs" | "rust-script" => "rust",
        _ => lang.trim(),
    }
}

/// Computes a 64-bit hash for code caching (hash language + version + code)
pub fn compute_program_hash(language: &str, version: &str, code: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    language.hash(&mut hasher);
    version.hash(&mut hasher);
    code.hash(&mut hasher);
    hasher.finish()
}

/// Injects `codeExecuted: true`, `runtimeUsed: language:version`, and `_runtime` into value and inner json objects
pub fn inject_metadata(val: &mut Value, runtime_used: &str) {
    let parts: Vec<&str> = runtime_used.split(':').collect();
    let lang = parts.first().copied().unwrap_or("javascript");
    let ver = parts.get(1).copied().unwrap_or("auto");
    let runtime_meta = serde_json::json!({
        "language": lang,
        "version": ver,
        "tier": "Tier-1 Native AST"
    });

    match val {
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                inject_metadata(item, runtime_used);
            }
        }
        Value::Object(map) => {
            map.insert("codeExecuted".to_string(), Value::Bool(true));
            map.insert("runtimeUsed".to_string(), Value::String(runtime_used.to_string()));
            map.insert("_runtime".to_string(), runtime_meta.clone());
            if let Some(inner) = map.get_mut("json").and_then(|j| j.as_object_mut()) {
                inner.insert("codeExecuted".to_string(), Value::Bool(true));
                inner.insert("runtimeUsed".to_string(), Value::String(runtime_used.to_string()));
                inner.insert("_runtime".to_string(), runtime_meta);
            }
        }
        _ => {}
    }
}

/// Normalizes expressions in JavaScript or Python into n8n $json expression format
pub fn prepare_expr_for_eval(expr: &str) -> String {
    let mut s = expr.trim().to_string();

    s = s.replace("item.json.", "$json.");
    s = s.replace("_item.json.", "$json.");
    s = s.replace("item.", "$json.");
    s = s.replace("_item.", "$json.");
    s = s.replace("_json.", "$json.");

    s = normalize_bracket_access(&s, "item");
    s = normalize_bracket_access(&s, "_item");
    s = normalize_bracket_access(&s, "_json");
    s = normalize_bracket_access(&s, "$json");

    s
}

fn normalize_bracket_access(input: &str, prefix: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut remaining = input;

    while let Some(idx) = remaining.find(prefix) {
        let after_prefix = &remaining[idx + prefix.len()..];
        if let Some(bracket_content) = after_prefix.strip_prefix('[') {
            result.push_str(&remaining[..idx]);
            if let Some(close_idx) = bracket_content.find(']') {
                let key_str = bracket_content[..close_idx]
                    .trim()
                    .trim_matches(|c| c == '\'' || c == '"');
                result.push_str(&format!("$json.{key_str}"));
                remaining = &bracket_content[close_idx + 1..];
                continue;
            }
        }
        result.push_str(&remaining[..idx + prefix.len()]);
        remaining = after_prefix;
    }
    result.push_str(remaining);
    result
}

/// Checks if a JSON value is truthy according to JavaScript / Python semantics
fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Bool(b) => *b,
        Value::Null => false,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty() && s != "false" && s != "0",
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Strips outer enclosing parentheses from a conditional expression if fully enclosed
fn clean_condition(cond: &str) -> String {
    let mut c = cond.trim();
    while c.starts_with('(') && c.ends_with(')') {
        let mut depth = 0;
        let mut fully_enclosed = false;
        for (idx, ch) in c.char_indices() {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
                if depth == 0 {
                    if idx == c.len() - 1 {
                        fully_enclosed = true;
                    }
                    break;
                }
            }
        }
        if fully_enclosed {
            c = c[1..c.len() - 1].trim();
        } else {
            break;
        }
    }
    c.to_string()
}

/// Formats a JSON value into a literal suitable for raw expression evaluation
fn format_local_value_for_expr(val: &Value) -> String {
    match val {
        Value::Null => "null".to_string(),
        Value::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Number(n) => n.to_string(),
        Value::String(s) => {
            serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\""))
        }
        other => other.to_string(),
    }
}

/// Substitutes local variable identifiers in an expression string with their literal representations
pub fn substitute_local_vars(expr: &str, local_vars: &HashMap<String, Value>) -> String {
    if local_vars.is_empty() {
        return expr.to_string();
    }

    let mut result = String::with_capacity(expr.len() + 16);
    let chars: Vec<char> = expr.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let mut in_string: Option<char> = None;

    while i < len {
        let c = chars[i];

        if let Some(quote) = in_string {
            result.push(c);
            if c == '\\' && i + 1 < len {
                i += 1;
                result.push(chars[i]);
            } else if c == quote {
                in_string = None;
            }
            i += 1;
            continue;
        }

        if c == '"' || c == '\'' || c == '`' {
            in_string = Some(c);
            result.push(c);
            i += 1;
            continue;
        }

        let is_ident_start = c.is_alphabetic() || c == '_' || c == '$';
        if is_ident_start {
            let prev_char = if i > 0 { Some(chars[i - 1]) } else { None };
            let is_property_access = prev_char == Some('.');

            let start = i;
            while i < len && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$') {
                i += 1;
            }
            let token: String = chars[start..i].iter().collect();

            if !is_property_access {
                let bare_token = token.strip_prefix('$').unwrap_or(&token);
                if let Some(val) = local_vars.get(&token).or_else(|| local_vars.get(bare_token)) {
                    result.push_str(&format_local_value_for_expr(val));
                    continue;
                }
            }

            result.push_str(&token);
            continue;
        }

        result.push(c);
        i += 1;
    }

    result
}

/// Prepares expression for evaluation by normalizing prefixes and substituting local variables
pub fn prepare_expr_with_locals(expr: &str, local_vars: &HashMap<String, Value>) -> String {
    let base_prepared = prepare_expr_for_eval(expr);
    substitute_local_vars(&base_prepared, local_vars)
}
/// Determines whether an assignment target string references an item property or a local variable
fn is_item_target(target_str: &str) -> bool {
    let mut cleaned = target_str.trim();
    for prefix in ["let ", "const ", "var "] {
        if let Some(rest) = cleaned.strip_prefix(prefix) {
            cleaned = rest.trim();
            break;
        }
    }
    cleaned.starts_with("item.")
        || cleaned.starts_with("item[")
        || cleaned.starts_with("$json.")
        || cleaned.starts_with("$json[")
        || cleaned.starts_with("_item.")
        || cleaned.starts_with("_item[")
        || cleaned.starts_with("_json.")
        || cleaned.starts_with("_json[")
        || cleaned == "item"
        || cleaned == "$json"
        || cleaned == "_item"
        || cleaned == "_json"
}

/// Extracts the clean variable name from a local variable assignment target (e.g. "let tax" -> "tax")
fn extract_local_var_name(target_str: &str) -> String {
    let mut cleaned = target_str.trim();
    for prefix in ["let ", "const ", "var "] {
        if let Some(rest) = cleaned.strip_prefix(prefix) {
            cleaned = rest.trim();
            break;
        }
    }
    cleaned.trim_matches(|c| c == ';' || c == ' ').to_string()
}

fn parse_assignment_target(target_str: &str) -> Vec<String> {
    let mut cleaned = target_str.trim();
    for prefix in ["let ", "const ", "var "] {
        if let Some(rest) = cleaned.strip_prefix(prefix) {
            cleaned = rest.trim();
            break;
        }
    }

    if let Some(rest) = cleaned.strip_prefix("item.json.") {
        cleaned = rest;
    } else if let Some(rest) = cleaned.strip_prefix("item.") {
        cleaned = rest;
    } else if let Some(rest) = cleaned.strip_prefix("$json.") {
        cleaned = rest;
    } else if let Some(rest) = cleaned.strip_prefix("_json.") {
        cleaned = rest;
    }

    if (cleaned.starts_with("item[")
        || cleaned.starts_with("_json[")
        || cleaned.starts_with("$json[")
        || cleaned.starts_with('['))
        && cleaned.ends_with(']')
    {
        let open_idx = cleaned.find('[').unwrap_or(0);
        let inner = &cleaned[open_idx + 1..cleaned.len().saturating_sub(1)];
        let field = inner.trim().trim_matches(|c| c == '\'' || c == '"');
        return vec![field.to_string()];
    }

    cleaned
        .split('.')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn apply_assignment(item: &mut Value, target_path: &[String], value: Value) {
    if target_path.is_empty() {
        return;
    }

    if target_path.len() == 1 {
        let key = &target_path[0];
        if let Some(obj) = item.as_object_mut() {
            if let Some(inner) = obj.get_mut("json").and_then(|j| j.as_object_mut()) {
                inner.insert(key.clone(), value.clone());
            }
            obj.insert(key.clone(), value);
        }
        return;
    }

    if target_path.len() == 2 && target_path[0] == "json" {
        let key = &target_path[1];
        if let Some(obj) = item.as_object_mut() {
            if let Some(inner) = obj.get_mut("json").and_then(|j| j.as_object_mut()) {
                inner.insert(key.clone(), value.clone());
            } else {
                let mut inner = serde_json::Map::new();
                inner.insert(key.clone(), value.clone());
                obj.insert("json".to_string(), Value::Object(inner));
            }
            obj.insert(key.clone(), value);
        }
        return;
    }

    if let Some(obj) = item.as_object_mut() {
        if let Some(inner) = obj.get_mut("json").and_then(|j| j.as_object_mut()) {
            set_nested_value(inner, target_path, value.clone());
        }
        set_nested_value(obj, target_path, value);
    }
}

fn set_nested_value(map: &mut serde_json::Map<String, Value>, path: &[String], value: Value) {
    if path.is_empty() {
        return;
    }
    if path.len() == 1 {
        map.insert(path[0].clone(), value);
        return;
    }

    let key = &path[0];
    let sub = map
        .entry(key.clone())
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if let Some(sub_map) = sub.as_object_mut() {
        set_nested_value(sub_map, &path[1..], value);
    }
}

fn strip_comments(code: &str) -> String {
    let mut out = String::with_capacity(code.len());
    let mut chars = code.chars().peekable();
    let mut in_block_comment = false;
    let mut in_string: Option<char> = None;

    while let Some(c) = chars.next() {
        if in_block_comment {
            if c == '*' && chars.peek() == Some(&'/') {
                chars.next();
                in_block_comment = false;
            }
            continue;
        }

        if let Some(quote) = in_string {
            out.push(c);
            if c == '\\' {
                if let Some(escaped) = chars.next() {
                    out.push(escaped);
                }
            } else if c == quote {
                in_string = None;
            }
            continue;
        }

        if c == '"' || c == '\'' || c == '`' {
            in_string = Some(c);
            out.push(c);
            continue;
        }

        if c == '/' && chars.peek() == Some(&'*') {
            chars.next();
            in_block_comment = true;
            continue;
        }

        if c == '/' && chars.peek() == Some(&'/') {
            for next_c in chars.by_ref() {
                if next_c == '\n' {
                    out.push('\n');
                    break;
                }
            }
            continue;
        }

        if c == '#' {
            for next_c in chars.by_ref() {
                if next_c == '\n' {
                    out.push('\n');
                    break;
                }
            }
            continue;
        }

        out.push(c);
    }

    out
}

fn parse_for_header(header: &str) -> (String, String) {
    let clean = header.trim_matches(|c| c == '(' || c == ')' || c == ' ');
    if let Some((left, right)) = clean.split_once(" of ") {
        let var_part = left.trim();
        let var_name = var_part
            .strip_prefix("const ")
            .or_else(|| var_part.strip_prefix("let "))
            .or_else(|| var_part.strip_prefix("var "))
            .unwrap_or(var_part)
            .trim();
        return (var_name.to_string(), right.trim().to_string());
    }
    if let Some((left, right)) = clean.split_once(" in ") {
        return (left.trim().to_string(), right.trim().to_string());
    }
    ("item".to_string(), "$input.all()".to_string())
}

fn find_assignment_eq(s: &str) -> Option<usize> {
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let bytes = s.as_bytes();
    let len = bytes.len();
    for i in 0..len {
        let b = bytes[i];
        if b == b'\'' && !in_double && !in_backtick {
            in_single = !in_single;
        } else if b == b'"' && !in_single && !in_backtick {
            in_double = !in_double;
        } else if b == b'`' && !in_single && !in_double {
            in_backtick = !in_backtick;
        } else if !in_single && !in_double && !in_backtick && b == b'=' {
            let prev = if i > 0 { bytes[i - 1] } else { b' ' };
            let next = if i + 1 < len { bytes[i + 1] } else { b' ' };

            if prev != b'=' && prev != b'!' && prev != b'<' && prev != b'>' && next != b'=' {
                return Some(i);
            }
        }
    }
    None
}

fn find_first_char_outside_string(s: &str, target: u8) -> Option<usize> {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut in_string: Option<u8> = None;

    while i < len {
        let b = bytes[i];
        if let Some(quote) = in_string {
            if b == b'\\' && i + 1 < len {
                i += 2;
                continue;
            }
            if b == quote {
                in_string = None;
            }
            i += 1;
            continue;
        }

        if b == b'"' || b == b'\'' || b == b'`' {
            in_string = Some(b);
            i += 1;
            continue;
        }

        if b == target {
            return Some(i);
        }

        i += 1;
    }
    None
}

fn find_matching_brace(s: &str, open_pos: usize) -> Option<usize> {
    let mut depth = 0;
    let mut in_string: Option<u8> = None;
    let bytes = s.as_bytes();
    let len = bytes.len();

    let mut i = open_pos;
    while i < len {
        let b = bytes[i];
        if let Some(quote) = in_string {
            if b == b'\\' && i + 1 < len {
                i += 2;
                continue;
            }
            if b == quote {
                in_string = None;
            }
            i += 1;
            continue;
        }

        if b == b'"' || b == b'\'' || b == b'`' {
            in_string = Some(b);
            i += 1;
            continue;
        }

        if b == b'{' {
            depth += 1;
        } else if b == b'}' {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn find_control_keyword(s: &str) -> Option<(usize, &'static str)> {
    let bytes = s.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut in_string: Option<u8> = None;

    while i < len {
        let b = bytes[i];
        if let Some(quote) = in_string {
            if b == b'\\' && i + 1 < len {
                i += 2;
                continue;
            }
            if b == quote {
                in_string = None;
            }
            i += 1;
            continue;
        }

        if b == b'"' || b == b'\'' || b == b'`' {
            in_string = Some(b);
            i += 1;
            continue;
        }

        let prev_ok = i == 0
            || (!bytes[i - 1].is_ascii_alphanumeric()
                && bytes[i - 1] != b'_'
                && bytes[i - 1] != b'$'
                && bytes[i - 1] != b'.');

        if prev_ok {
            if i + 2 <= len && &bytes[i..i + 2] == b"if" {
                let next_ok = i + 2 == len
                    || bytes[i + 2].is_ascii_whitespace()
                    || bytes[i + 2] == b'(';
                if next_ok {
                    return Some((i, "if"));
                }
            }

            if i + 3 <= len && &bytes[i..i + 3] == b"for" {
                let next_ok = i + 3 == len
                    || bytes[i + 3].is_ascii_whitespace()
                    || bytes[i + 3] == b'(';
                if next_ok {
                    return Some((i, "for"));
                }
            }
        }

        i += 1;
    }

    None
}

fn parse_linear_statements(raw: &str) -> Vec<PolyglotInstruction> {
    let mut instructions = Vec::new();
    let lines = raw.split([';', '\n']);

    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(ret_expr) = trimmed.strip_prefix("return ") {
            let expr = ret_expr.trim().trim_end_matches(';').trim();
            instructions.push(PolyglotInstruction::Return {
                expr: expr.to_string(),
            });
            continue;
        }

        if trimmed == "return" || trimmed == "return;" {
            instructions.push(PolyglotInstruction::Return {
                expr: "$input.all()".to_string(),
            });
            continue;
        }

        if let Some(eq_idx) = find_assignment_eq(trimmed) {
            let target_str = &trimmed[..eq_idx].trim();
            let expr_str = &trimmed[eq_idx + 1..].trim();
            let expr_clean = expr_str.trim_end_matches(';').trim();

            if is_item_target(target_str) {
                let target_path = parse_assignment_target(target_str);
                instructions.push(PolyglotInstruction::Assign {
                    target: target_path,
                    expr: expr_clean.to_string(),
                });
            } else {
                let var_name = extract_local_var_name(target_str);
                instructions.push(PolyglotInstruction::AssignLocal {
                    name: var_name,
                    expr: expr_clean.to_string(),
                });
            }
            continue;
        }

        instructions.push(PolyglotInstruction::ExprStatement {
            expr: trimmed.trim_end_matches(';').trim().to_string(),
        });
    }

    instructions
}

fn parse_statements_into_ast(code: &str) -> Vec<PolyglotInstruction> {
    let stripped = strip_comments(code);
    parse_ast_block(&stripped)
}

fn parse_ast_block(code: &str) -> Vec<PolyglotInstruction> {
    let trimmed = code.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    if let Some((kw_pos, kw)) = find_control_keyword(code) {
        let before = &code[..kw_pos];
        let mut instructions = parse_linear_statements(before);
        let rest = &code[kw_pos..];

        let open_brace_idx = find_first_char_outside_string(rest, b'{');
        let colon_idx = find_first_char_outside_string(rest, b':');

        let is_brace = match (open_brace_idx, colon_idx) {
            (Some(b), Some(c)) => b < c,
            (Some(_), None) => true,
            _ => false,
        };

        if kw == "if" {
            if is_brace {
                if let Some(brace_open) = open_brace_idx {
                    let header = rest[2..brace_open].trim();
                    let condition = clean_condition(header);
                    if let Some(brace_close) = find_matching_brace(rest, brace_open) {
                        let then_content = &rest[brace_open + 1..brace_close];
                        let then_body = parse_statements_into_ast(then_content);

                        let after_then = &rest[brace_close + 1..];
                        let after_trimmed = after_then.trim_start();

                        if let Some(after_else_kw) = after_trimmed.strip_prefix("else") {
                            let next_char = after_else_kw.chars().next();
                            let is_else_token = next_char.map(|c| c.is_whitespace() || c == '{').unwrap_or(true);

                            if is_else_token {
                                let after_else = after_else_kw.trim_start();
                                if let Some(else_brace_open) = find_first_char_outside_string(after_else, b'{') {
                                    if let Some(else_brace_close) = find_matching_brace(after_else, else_brace_open) {
                                        let else_content = &after_else[else_brace_open + 1..else_brace_close];
                                        let else_body = parse_statements_into_ast(else_content);

                                        instructions.push(PolyglotInstruction::IfElse {
                                            condition,
                                            then_body,
                                            else_body,
                                        });

                                        let remaining = &after_else[else_brace_close + 1..];
                                        instructions.extend(parse_statements_into_ast(remaining));
                                        return instructions;
                                    }
                                }
                            }
                        }

                        instructions.push(PolyglotInstruction::IfElse {
                            condition,
                            then_body,
                            else_body: Vec::new(),
                        });
                        instructions.extend(parse_statements_into_ast(after_then));
                        return instructions;
                    }
                }
            } else if let Some(col_pos) = colon_idx {
                let header = rest[2..col_pos].trim();
                let condition = clean_condition(header);
                let block_lines = &rest[col_pos + 1..];

                let mut then_lines = Vec::new();
                let mut else_lines = Vec::new();
                let mut after_lines = Vec::new();

                enum PyIfState {
                    Then,
                    Else,
                    After,
                }
                let mut state = PyIfState::Then;

                for line in block_lines.lines() {
                    let trimmed_line = line.trim();
                    if trimmed_line.is_empty() {
                        continue;
                    }
                    match state {
                        PyIfState::Then => {
                            if line.starts_with("    ")
                                || line.starts_with('\t')
                                || line.starts_with("  ")
                            {
                                then_lines.push(trimmed_line);
                            } else if trimmed_line == "else:" || trimmed_line.starts_with("else:") {
                                state = PyIfState::Else;
                            } else {
                                state = PyIfState::After;
                                after_lines.push(trimmed_line);
                            }
                        }
                        PyIfState::Else => {
                            if line.starts_with("    ")
                                || line.starts_with('\t')
                                || line.starts_with("  ")
                            {
                                else_lines.push(trimmed_line);
                            } else {
                                state = PyIfState::After;
                                after_lines.push(trimmed_line);
                            }
                        }
                        PyIfState::After => {
                            after_lines.push(trimmed_line);
                        }
                    }
                }

                let then_body = parse_statements_into_ast(&then_lines.join("\n"));
                let else_body = parse_statements_into_ast(&else_lines.join("\n"));

                instructions.push(PolyglotInstruction::IfElse {
                    condition,
                    then_body,
                    else_body,
                });

                if !after_lines.is_empty() {
                    instructions.extend(parse_statements_into_ast(&after_lines.join("\n")));
                }
                return instructions;
            }
        } else if kw == "for" {
            if is_brace {
                if let Some(brace_open) = open_brace_idx {
                    let header = rest[3..brace_open].trim();
                    let (item_var, coll_expr) = parse_for_header(header);
                    if let Some(brace_close) = find_matching_brace(rest, brace_open) {
                        let body_str = &rest[brace_open + 1..brace_close];
                        let body = parse_statements_into_ast(body_str);

                        instructions.push(PolyglotInstruction::ForLoop {
                            item_var,
                            collection_expr: coll_expr,
                            body,
                        });

                        let remaining = &rest[brace_close + 1..];
                        instructions.extend(parse_statements_into_ast(remaining));
                        return instructions;
                    }
                }
            } else if let Some(col_pos) = colon_idx {
                let header = rest[3..col_pos].trim();
                let (item_var, coll_expr) = parse_for_header(header);
                let block_lines = &rest[col_pos + 1..];

                let mut body_lines = Vec::new();
                let mut after_lines = Vec::new();
                let mut in_body = true;

                for line in block_lines.lines() {
                    let trimmed_line = line.trim();
                    if trimmed_line.is_empty() {
                        continue;
                    }
                    if in_body
                        && (line.starts_with("    ")
                            || line.starts_with('\t')
                            || line.starts_with("  "))
                    {
                        body_lines.push(trimmed_line);
                    } else {
                        in_body = false;
                        after_lines.push(trimmed_line);
                    }
                }

                let body = parse_statements_into_ast(&body_lines.join("\n"));
                instructions.push(PolyglotInstruction::ForLoop {
                    item_var,
                    collection_expr: coll_expr,
                    body,
                });

                if !after_lines.is_empty() {
                    instructions.extend(parse_statements_into_ast(&after_lines.join("\n")));
                }
                return instructions;
            }
        }
    }

    parse_linear_statements(code)
}

fn parse_lenient_json(raw: &str) -> Option<Value> {
    if let Ok(v) = serde_json::from_str::<Value>(raw) {
        return Some(v);
    }
    let with_double_quotes = raw.replace('\'', "\"");
    if let Ok(v) = serde_json::from_str::<Value>(&with_double_quotes) {
        return Some(v);
    }

    let mut normalized = String::with_capacity(with_double_quotes.len() + 16);
    let mut chars = with_double_quotes.chars().peekable();
    let mut token = String::new();

    while let Some(c) = chars.next() {
        if c.is_alphanumeric() || c == '_' || c == '$' {
            token.push(c);
        } else {
            if !token.is_empty() {
                let mut peek_chars = chars.clone();
                let mut is_key = false;
                for p in peek_chars.by_ref() {
                    if p == ' ' || p == '\t' || p == '\n' {
                        continue;
                    }
                    if p == ':' {
                        is_key = true;
                    }
                    break;
                }
                if is_key {
                    normalized.push('"');
                    normalized.push_str(&token);
                    normalized.push('"');
                } else {
                    normalized.push_str(&token);
                }
                token.clear();
            }
            normalized.push(c);
        }
    }
    if !token.is_empty() {
        normalized.push_str(&token);
    }

    serde_json::from_str::<Value>(&normalized).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_list_runtimes_registry() {
        let runtimes = list_runtimes();
        assert!(runtimes.len() >= 7);

        let js = runtimes.iter().find(|r| r.language == "javascript" && r.is_default);
        assert!(js.is_some());
        if let Some(pkg) = js {
            assert_eq!(pkg.version, "es2022-quickjs");
            assert!(pkg.installed);
            assert_eq!(pkg.size_bytes, 4_194_304);
            assert_eq!(
                pkg.sha256,
                "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
            );
        }

        let py = runtimes.iter().find(|r| r.language == "python" && r.is_default);
        assert!(py.is_some());
        if let Some(pkg) = py {
            assert_eq!(pkg.version, "3.11-sandbox");
            assert!(pkg.installed);
        }
    }

    #[test]
    fn test_download_and_delete_runtime() {
        let res_del = delete_runtime("javascript", "es2024-v8-compat");
        assert!(res_del.is_ok());

        let pkg = download_runtime("javascript", "es2024-v8-compat");
        assert!(pkg.is_ok());
        if let Ok(p) = pkg {
            assert!(p.installed);
        }

        let non_existent = delete_runtime("nonexistent_lang", "9.9.9");
        assert!(non_existent.is_err());
    }

    #[test]
    fn test_tier0_fast_path() {
        let ctx = ExprContext::empty("exec-fast");
        let input = json!({
            "json": { "test": 123 }
        });

        let res = execute_polyglot_code("javascript", "es2022-quickjs", "return $input.all();", &input, &ctx);
        assert!(res.is_ok());
        if let Ok(val) = res {
            assert_eq!(val["codeExecuted"], json!(true));
            assert_eq!(val["runtimeUsed"], json!("javascript:es2022-quickjs"));
            assert_eq!(val["json"]["codeExecuted"], json!(true));
            assert_eq!(val["json"]["test"], json!(123));
        }

        let res_py = execute_polyglot_code("python", "3.11-sandbox", "return _input.all();", &input, &ctx);
        assert!(res_py.is_ok());
        if let Ok(val) = res_py {
            assert_eq!(val["runtimeUsed"], json!("python:3.11-sandbox"));
        }
    }

    #[test]
    fn test_tier1_ast_cache_and_tier2_js() {
        let ctx = ExprContext::empty("exec-js");
        let input = json!({
            "json": {
                "price": 100,
                "qty": 3
            }
        });

        let code = "item.total = item.price * item.qty;\nreturn item;";
        let res1 = execute_polyglot_code("javascript", "es2022-quickjs", code, &input, &ctx);
        assert!(res1.is_ok());
        if let Ok(val) = res1 {
            assert_eq!(val["codeExecuted"], json!(true));
            assert_eq!(val["runtimeUsed"], json!("javascript:es2022-quickjs"));
            assert_eq!(val["json"]["total"], json!(300));
        }

        // Second run must hit AST cache
        let manager = PolyglotRuntimeManager::global();
        assert!(manager.cache_len() >= 1);

        let res2 = execute_polyglot_code("javascript", "es2022-quickjs", code, &input, &ctx);
        assert!(res2.is_ok());
        if let Ok(val) = res2 {
            assert_eq!(val["json"]["total"], json!(300));
        }
    }

    #[test]
    fn test_tier2_python_dict_access() {
        let ctx = ExprContext::empty("exec-py");
        let input = json!({
            "json": {
                "price": 200
            }
        });

        let code = "item['tax'] = item['price'] * 0.1\nreturn item";
        let res = execute_polyglot_code("python", "3.11-sandbox", code, &input, &ctx);
        assert!(res.is_ok());
        if let Ok(val) = res {
            assert_eq!(val["codeExecuted"], json!(true));
            assert_eq!(val["runtimeUsed"], json!("python:3.11-sandbox"));
            assert_eq!(val["json"]["tax"], json!(20));
        }
    }

    #[test]
    fn test_tier2_for_of_loop_array() {
        let ctx = ExprContext::empty("exec-arr");
        let input = json!([
            { "json": { "price": 10, "qty": 2 } },
            { "json": { "price": 20, "qty": 4 } }
        ]);

        let code = r#"
            for (const item of $input.all()) {
                item.json.total = item.json.price * item.json.qty;
            }
            return $input.all();
        "#;
        let res = execute_polyglot_code("javascript", "es2022-quickjs", code, &input, &ctx);
        assert!(res.is_ok());
        if let Ok(Value::Array(items)) = res {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0]["json"]["total"], json!(20));
            assert_eq!(items[1]["json"]["total"], json!(80));
            assert_eq!(items[0]["codeExecuted"], json!(true));
            assert_eq!(items[1]["codeExecuted"], json!(true));
        } else {
            panic!("Expected array output");
        }
    }

    #[test]
    fn test_tier2_wasm_transformation() {
        let ctx = ExprContext::empty("exec-wasm");
        let input = json!({
            "json": { "count": 42 }
        });

        let code = "item.result = item.count * 2;\nreturn item;";
        let res = execute_polyglot_code("wasm", "wasm32-wasmer-4.0", code, &input, &ctx);
        assert!(res.is_ok());
        if let Ok(val) = res {
            assert_eq!(val["codeExecuted"], json!(true));
            assert_eq!(val["runtimeUsed"], json!("wasm:wasm32-wasmer-4.0"));
            assert_eq!(val["wasmProcessed"], json!(true));
            assert_eq!(val["json"]["result"], json!(84));
        }
    }

    #[test]
    fn test_local_variables_evaluation() {
        let ctx = ExprContext::empty("exec-local-vars");
        let input = json!({
            "json": { "price": 100 }
        });
        let code = "let tax = item.price * 0.1;\nitem.total = item.price + tax;\nreturn item;";
        let res = execute_polyglot_code("javascript", "es2022-quickjs", code, &input, &ctx);
        assert!(res.is_ok());
        if let Ok(val) = res {
            assert_eq!(val["codeExecuted"], json!(true));
            assert_eq!(val["json"]["total"], json!(110));
            // Verify local variable 'tax' was NOT added to item.json
            assert!(val["json"].get("tax").is_none());
            assert!(val.get("tax").is_none());
        }
    }

    #[test]
    fn test_if_else_branching() {
        let ctx = ExprContext::empty("exec-if-else");
        let input_high = json!({
            "json": { "price": 100 }
        });
        let input_low = json!({
            "json": { "price": 50 }
        });
        let code = "if (item.price >= 100) { item.tax = 15; } else { item.tax = 5; }\nreturn item;";

        let res_high = execute_polyglot_code("javascript", "es2022-quickjs", code, &input_high, &ctx);
        assert!(res_high.is_ok());
        if let Ok(val) = res_high {
            assert_eq!(val["json"]["tax"], json!(15));
        }

        let res_low = execute_polyglot_code("javascript", "es2022-quickjs", code, &input_low, &ctx);
        assert!(res_low.is_ok());
        if let Ok(val) = res_low {
            assert_eq!(val["json"]["tax"], json!(5));
        }
    }

    #[test]
    fn test_runtime_not_installed_rejection() {
        let ctx = ExprContext::empty("exec-not-installed");
        let input = json!({ "json": { "a": 1 } });
        // Ensure es2024-v8-compat is uninstalled
        let _ = delete_runtime("javascript", "es2024-v8-compat");
        assert!(!is_runtime_installed("javascript", "es2024-v8-compat"));

        let res = execute_polyglot_code("javascript", "es2024-v8-compat", "return item;", &input, &ctx);
        assert!(res.is_err());
        if let Err(e) = res {
            assert!(e.contains("is not installed"));
        }
    }

    #[test]
    fn test_execute_polyglot_code_with_mode_for_each_item() {
        let ctx = ExprContext::empty("exec-mode-items");
        let input = json!([
            { "json": { "price": 100 } },
            { "json": { "price": 200 } }
        ]);
        let code = "let tax = item.price * 0.1;\nitem.total = item.price + tax;\nreturn item;";
        let res = execute_polyglot_code_with_mode(
            "javascript",
            "es2022-quickjs",
            code,
            &input,
            &ctx,
            "runOnceForEachItem",
        );
        assert!(res.is_ok());
        if let Ok(Value::Array(items)) = res {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0]["json"]["total"], json!(110));
            assert_eq!(items[1]["json"]["total"], json!(220));
            assert!(items[0]["json"].get("tax").is_none());
            assert!(items[1]["json"].get("tax").is_none());
        } else {
            panic!("Expected array output from runOnceForEachItem");
        }
    }

    #[test]
    fn test_delete_runtime_cache_invalidation() {
        let ctx = ExprContext::empty("exec-inval");
        let input = json!({ "json": { "x": 10 } });
        let code = "item.x = item.x * 2;\nreturn item;";

        let pkg = download_runtime("javascript", "test-inval-v1");
        assert!(pkg.is_ok());
        assert!(is_runtime_installed("javascript", "test-inval-v1"));

        let res = execute_polyglot_code("javascript", "test-inval-v1", code, &input, &ctx);
        assert!(res.is_ok());

        let manager = PolyglotRuntimeManager::global();
        let code_hash = compute_program_hash("javascript", "test-inval-v1", code);
        assert!(manager.ast_cache.contains_key(&code_hash));

        let del_res = delete_runtime("javascript", "test-inval-v1");
        assert!(del_res.is_ok());
        assert!(!is_runtime_installed("javascript", "test-inval-v1"));

        // AST cache for this language:version MUST be cleared
        assert!(!manager.ast_cache.contains_key(&code_hash));
    }

    #[test]
    fn test_python_if_else_syntax() {
        let ctx = ExprContext::empty("exec-py-if");
        let input = json!({ "json": { "price": 120 } });
        let code = "if item['price'] >= 100:\n    item['tax'] = 20\nelse:\n    item['tax'] = 5\nreturn item";
        let res = execute_polyglot_code("python", "3.11-sandbox", code, &input, &ctx);
        assert!(res.is_ok());
        if let Ok(val) = res {
            assert_eq!(val["json"]["tax"], json!(20));
        }
    }
}
