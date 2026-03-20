use graft_core::{EdgeId, NodeId};

use crate::ast::*;
use std::collections::HashSet;

use crate::planner::{expr_display_name, AggFunction, Plan};

// ---------------------------------------------------------------------------
// Value — runtime values during query execution
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Node {
        id: NodeId,
        label: Option<String>,
    },
    Edge {
        id: EdgeId,
        source: NodeId,
        target: NodeId,
        label: Option<String>,
    },
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(n) => write!(f, "{n}"),
            Value::Float(n) => write!(f, "{n}"),
            Value::String(s) => write!(f, "{s}"),
            Value::Node { id, label } => {
                if let Some(l) = label {
                    write!(f, "(:{l} {{id: {id}}})")
                } else {
                    write!(f, "({{id: {id}}})")
                }
            }
            Value::Edge {
                id, source, target, label,
            } => {
                if let Some(l) = label {
                    write!(f, "[:{l} {{id: {id}, {source}->{target}}}]")
                } else {
                    write!(f, "[{{id: {id}, {source}->{target}}}]")
                }
            }
        }
    }
}

impl Value {
    fn is_truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Int(n) => *n != 0,
            Value::Float(f) => *f != 0.0,
            Value::String(s) => !s.is_empty(),
            _ => true,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(n) => Some(*n as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Row — a binding of variable names to values
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Row {
    bindings: Vec<(String, Value)>,
}

impl Row {
    pub fn new() -> Self {
        Self {
            bindings: Vec::new(),
        }
    }

    pub fn bind(mut self, name: String, value: Value) -> Self {
        self.bindings.push((name, value));
        self
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.bindings
            .iter()
            .rev()
            .find_map(|(k, v)| if k == name { Some(v) } else { None })
    }

    pub fn columns(&self) -> Vec<String> {
        self.bindings.iter().map(|(k, _)| k.clone()).collect()
    }

    pub fn merge(mut self, other: Row) -> Self {
        self.bindings.extend(other.bindings);
        self
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// QueryResult
// ---------------------------------------------------------------------------

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

// ---------------------------------------------------------------------------
// StorageAccess — interface to the storage layer
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct NodeInfo {
    pub id: NodeId,
    pub label: Option<String>,
}

#[derive(Debug)]
pub struct EdgeInfo {
    pub id: EdgeId,
    pub source: NodeId,
    pub target: NodeId,
    pub label: Option<String>,
}

pub trait StorageAccess {
    // Node operations
    fn scan_nodes(&self, label: Option<&str>) -> Vec<NodeInfo>;
    fn get_node(&self, id: NodeId) -> Option<NodeInfo>;
    fn node_property(&self, id: NodeId, key: &str) -> Value;

    // Edge operations
    fn outbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo>;
    fn inbound_edges(&self, node_id: NodeId, label: Option<&str>) -> Vec<EdgeInfo>;
    fn edge_property(&self, id: EdgeId, key: &str) -> Value;

    // Mutations
    fn create_node(
        &mut self,
        label: Option<&str>,
        properties: &[(String, Value)],
    ) -> NodeId;
    fn create_edge(
        &mut self,
        source: NodeId,
        target: NodeId,
        label: Option<&str>,
        properties: &[(String, Value)],
    ) -> EdgeId;
    fn set_node_property(&mut self, id: NodeId, key: &str, value: &Value);
    fn set_edge_property(&mut self, id: EdgeId, key: &str, value: &Value);
    fn delete_node(&mut self, id: NodeId);
    fn delete_edge(&mut self, id: EdgeId);

    // Transaction lifecycle (default no-ops for backward compatibility)
    fn begin_tx(&mut self) -> u64 { 0 }
    fn commit_tx(&mut self) {}
    fn abort_tx(&mut self) {}
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ExecutionError {
    UnboundVariable(String),
    TypeError(String),
    DivisionByZero,
    UnknownFunction(String),
}

impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnboundVariable(name) => write!(f, "unbound variable: {name}"),
            Self::TypeError(msg) => write!(f, "type error: {msg}"),
            Self::DivisionByZero => write!(f, "division by zero"),
            Self::UnknownFunction(name) => write!(f, "unknown function: {name}"),
        }
    }
}

impl std::error::Error for ExecutionError {}

// ---------------------------------------------------------------------------
// Executor — public API
// ---------------------------------------------------------------------------

/// Execute a plan against storage and return tabular results.
///
/// Every query is implicitly wrapped in an auto-commit transaction.
/// The execution strategy materializes intermediate results at each step.
/// The plan structure is push-oriented (each node references its input),
/// setting up the architecture for streaming execution in a future phase.
pub fn execute(
    plan: &Plan,
    storage: &mut dyn StorageAccess,
) -> Result<QueryResult, ExecutionError> {
    storage.begin_tx();

    let result = execute_plan(plan, storage);

    match &result {
        Ok(_) => storage.commit_tx(),
        Err(_) => storage.abort_tx(),
    }

    let rows = result?;

    let columns = rows
        .first()
        .map(|r| r.columns())
        .unwrap_or_default();

    let data = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|col| row.get(col).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();

    Ok(QueryResult {
        columns,
        rows: data,
    })
}

// ---------------------------------------------------------------------------
// Internal execution
// ---------------------------------------------------------------------------

fn execute_plan(
    plan: &Plan,
    storage: &mut dyn StorageAccess,
) -> Result<Vec<Row>, ExecutionError> {
    match plan {
        Plan::NodeScan { variable, label } => {
            let nodes = storage.scan_nodes(label.as_deref());
            Ok(nodes
                .into_iter()
                .map(|n| {
                    Row::new().bind(
                        variable.clone(),
                        Value::Node {
                            id: n.id,
                            label: n.label,
                        },
                    )
                })
                .collect())
        }

        Plan::Expand {
            input,
            src_var,
            edge_var,
            edge_label,
            direction,
            dst_var,
            dst_label,
        } => {
            let input_rows = execute_plan(input, storage)?;
            let mut result = Vec::new();

            for row in input_rows {
                let src_id = match row.get(src_var) {
                    Some(Value::Node { id, .. }) => *id,
                    _ => {
                        return Err(ExecutionError::TypeError(format!(
                            "expected node for variable '{src_var}'"
                        )))
                    }
                };

                let edges = match direction {
                    EdgeDirection::Right => {
                        storage.outbound_edges(src_id, edge_label.as_deref())
                    }
                    EdgeDirection::Left => {
                        storage.inbound_edges(src_id, edge_label.as_deref())
                    }
                    EdgeDirection::Undirected => {
                        let mut all =
                            storage.outbound_edges(src_id, edge_label.as_deref());
                        all.extend(
                            storage.inbound_edges(src_id, edge_label.as_deref()),
                        );
                        all
                    }
                };

                for edge_info in edges {
                    let dst_id = match direction {
                        EdgeDirection::Right => edge_info.target,
                        EdgeDirection::Left => edge_info.source,
                        EdgeDirection::Undirected => {
                            if edge_info.source == src_id {
                                edge_info.target
                            } else {
                                edge_info.source
                            }
                        }
                    };

                    // Filter by destination label if specified
                    if let Some(ref label) = dst_label {
                        if let Some(node_info) = storage.get_node(dst_id) {
                            if node_info.label.as_deref() != Some(label.as_str()) {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }

                    let dst_label_actual = storage
                        .get_node(dst_id)
                        .and_then(|n| n.label);

                    let mut new_row = row.clone();
                    if let Some(ref evar) = edge_var {
                        new_row = new_row.bind(
                            evar.clone(),
                            Value::Edge {
                                id: edge_info.id,
                                source: edge_info.source,
                                target: edge_info.target,
                                label: edge_info.label,
                            },
                        );
                    }
                    new_row = new_row.bind(
                        dst_var.clone(),
                        Value::Node {
                            id: dst_id,
                            label: dst_label_actual,
                        },
                    );

                    result.push(new_row);
                }
            }

            Ok(result)
        }

        Plan::NestedLoop { outer, inner } => {
            let outer_rows = execute_plan(outer, storage)?;
            let mut result = Vec::new();

            for outer_row in outer_rows {
                let inner_rows = execute_plan(inner, storage)?;
                for inner_row in inner_rows {
                    result.push(outer_row.clone().merge(inner_row));
                }
            }

            Ok(result)
        }

        Plan::Filter { input, predicate } => {
            let input_rows = execute_plan(input, storage)?;
            let mut result = Vec::new();

            for row in input_rows {
                let val = eval_expr(predicate, &row, storage)?;
                if val.is_truthy() {
                    result.push(row);
                }
            }

            Ok(result)
        }

        Plan::Project { input, items } => {
            let input_rows = execute_plan(input, storage)?;
            let mut result = Vec::new();

            for row in input_rows {
                let mut new_row = Row::new();
                for item in items {
                    let value = eval_expr(&item.expr, &row, storage)?;
                    let name = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| expr_display_name(&item.expr));
                    new_row = new_row.bind(name, value);
                }
                result.push(new_row);
            }

            Ok(result)
        }

        Plan::Sort { input, items } => {
            let mut rows = execute_plan(input, storage)?;

            rows.sort_by(|a, b| {
                for item in items {
                    let va = eval_expr(&item.expr, a, storage).unwrap_or(Value::Null);
                    let vb = eval_expr(&item.expr, b, storage).unwrap_or(Value::Null);
                    let ord = compare_values(&va, &vb);
                    let ord = match item.direction {
                        SortDirection::Asc => ord,
                        SortDirection::Desc => ord.reverse(),
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });

            Ok(rows)
        }

        Plan::Limit { input, count } => {
            let rows = execute_plan(input, storage)?;
            Ok(rows.into_iter().take(*count as usize).collect())
        }

        Plan::Skip { input, count } => {
            let rows = execute_plan(input, storage)?;
            Ok(rows.into_iter().skip(*count as usize).collect())
        }

        Plan::CreateNode {
            input,
            variable,
            label,
            properties,
        } => {
            let input_rows = match input {
                Some(plan) => execute_plan(plan, storage)?,
                None => vec![Row::new()],
            };
            let mut result = Vec::new();

            for row in input_rows {
                let props: Vec<(String, Value)> = properties
                    .iter()
                    .map(|(k, v)| {
                        let val = eval_expr(v, &row, storage)?;
                        Ok((k.clone(), val))
                    })
                    .collect::<Result<_, ExecutionError>>()?;

                let node_id =
                    storage.create_node(label.as_deref(), &props);
                let mut new_row = row;
                if let Some(ref var) = variable {
                    new_row = new_row.bind(
                        var.clone(),
                        Value::Node {
                            id: node_id,
                            label: label.clone(),
                        },
                    );
                }
                result.push(new_row);
            }

            Ok(result)
        }

        Plan::CreateEdge {
            input,
            variable,
            label,
            from_var,
            to_var,
            properties,
        } => {
            let input_rows = execute_plan(input, storage)?;
            let mut result = Vec::new();

            for row in input_rows {
                let from_id = match row.get(from_var) {
                    Some(Value::Node { id, .. }) => *id,
                    _ => {
                        return Err(ExecutionError::TypeError(format!(
                            "expected node for '{from_var}'"
                        )))
                    }
                };
                let to_id = match row.get(to_var) {
                    Some(Value::Node { id, .. }) => *id,
                    _ => {
                        return Err(ExecutionError::TypeError(format!(
                            "expected node for '{to_var}'"
                        )))
                    }
                };

                let props: Vec<(String, Value)> = properties
                    .iter()
                    .map(|(k, v)| {
                        let val = eval_expr(v, &row, storage)?;
                        Ok((k.clone(), val))
                    })
                    .collect::<Result<_, ExecutionError>>()?;

                let edge_id = storage.create_edge(
                    from_id,
                    to_id,
                    label.as_deref(),
                    &props,
                );
                let mut new_row = row;
                if let Some(ref var) = variable {
                    new_row = new_row.bind(
                        var.clone(),
                        Value::Edge {
                            id: edge_id,
                            source: from_id,
                            target: to_id,
                            label: label.clone(),
                        },
                    );
                }
                result.push(new_row);
            }

            Ok(result)
        }

        Plan::SetProperty {
            input,
            variable,
            property,
            value,
        } => {
            let input_rows = execute_plan(input, storage)?;
            let mut result = Vec::new();

            for row in input_rows {
                let val = eval_expr(value, &row, storage)?;
                match row.get(variable) {
                    Some(Value::Node { id, .. }) => {
                        storage.set_node_property(*id, property, &val);
                    }
                    Some(Value::Edge { id, .. }) => {
                        storage.set_edge_property(*id, property, &val);
                    }
                    _ => {
                        return Err(ExecutionError::TypeError(format!(
                            "expected node or edge for '{variable}'"
                        )))
                    }
                }
                result.push(row);
            }

            Ok(result)
        }

        Plan::Delete { input, variable } => {
            let input_rows = execute_plan(input, storage)?;

            for row in &input_rows {
                match row.get(variable) {
                    Some(Value::Node { id, .. }) => storage.delete_node(*id),
                    Some(Value::Edge { id, .. }) => storage.delete_edge(*id),
                    _ => {
                        return Err(ExecutionError::TypeError(format!(
                            "expected node or edge for '{variable}'"
                        )))
                    }
                }
            }

            Ok(input_rows)
        }

        Plan::Aggregate { input, ops } => {
            let input_rows = execute_plan(input, storage)?;
            let mut new_row = Row::new();

            for op in ops {
                let value = match op.function {
                    AggFunction::Count => Value::Int(input_rows.len() as i64),
                    AggFunction::Sum => {
                        let mut sum = 0.0f64;
                        for row in &input_rows {
                            if let Ok(v) =
                                eval_expr(&op.input_expr, row, storage)
                            {
                                if let Some(n) = v.as_f64() {
                                    sum += n;
                                }
                            }
                        }
                        Value::Float(sum)
                    }
                    AggFunction::Avg => {
                        let mut sum = 0.0f64;
                        let mut count = 0u64;
                        for row in &input_rows {
                            if let Ok(v) =
                                eval_expr(&op.input_expr, row, storage)
                            {
                                if let Some(n) = v.as_f64() {
                                    sum += n;
                                    count += 1;
                                }
                            }
                        }
                        if count > 0 {
                            Value::Float(sum / count as f64)
                        } else {
                            Value::Null
                        }
                    }
                    AggFunction::Min => {
                        let mut min: Option<f64> = None;
                        for row in &input_rows {
                            if let Ok(v) =
                                eval_expr(&op.input_expr, row, storage)
                            {
                                if let Some(n) = v.as_f64() {
                                    min = Some(
                                        min.map_or(n, |m: f64| m.min(n)),
                                    );
                                }
                            }
                        }
                        min.map_or(Value::Null, Value::Float)
                    }
                    AggFunction::Max => {
                        let mut max: Option<f64> = None;
                        for row in &input_rows {
                            if let Ok(v) =
                                eval_expr(&op.input_expr, row, storage)
                            {
                                if let Some(n) = v.as_f64() {
                                    max = Some(
                                        max.map_or(n, |m: f64| m.max(n)),
                                    );
                                }
                            }
                        }
                        max.map_or(Value::Null, Value::Float)
                    }
                };
                new_row = new_row.bind(op.output_name.clone(), value);
            }

            Ok(vec![new_row])
        }

        Plan::VarExpand {
            input,
            src_var,
            edge_label,
            direction,
            dst_var,
            dst_label,
            min_hops,
            max_hops,
        } => {
            let input_rows = execute_plan(input, storage)?;
            let mut result = Vec::new();
            let max = max_hops.unwrap_or(20); // safety cap

            for row in input_rows {
                let src_id = match row.get(src_var) {
                    Some(Value::Node { id, .. }) => *id,
                    _ => {
                        return Err(ExecutionError::TypeError(format!(
                            "expected node for variable '{src_var}'"
                        )))
                    }
                };

                // BFS: (node_id, depth)
                let mut frontier = vec![(src_id, 0u64)];
                let mut visited = HashSet::new();
                visited.insert(src_id);

                while let Some((current_id, depth)) = frontier.pop() {
                    if depth >= *min_hops {
                        // Check destination label filter
                        let include = if let Some(ref label) = dst_label {
                            storage
                                .get_node(current_id)
                                .and_then(|n| n.label)
                                .as_deref()
                                == Some(label.as_str())
                        } else {
                            true
                        };
                        if include && (depth > 0 || *min_hops == 0) {
                            let dst_label_actual =
                                storage.get_node(current_id).and_then(|n| n.label);
                            let new_row = row.clone().bind(
                                dst_var.clone(),
                                Value::Node {
                                    id: current_id,
                                    label: dst_label_actual,
                                },
                            );
                            result.push(new_row);
                        }
                    }

                    if depth < max {
                        let edges = match direction {
                            EdgeDirection::Right => {
                                storage.outbound_edges(current_id, edge_label.as_deref())
                            }
                            EdgeDirection::Left => {
                                storage.inbound_edges(current_id, edge_label.as_deref())
                            }
                            EdgeDirection::Undirected => {
                                let mut all = storage
                                    .outbound_edges(current_id, edge_label.as_deref());
                                all.extend(
                                    storage.inbound_edges(current_id, edge_label.as_deref()),
                                );
                                all
                            }
                        };

                        for edge_info in edges {
                            let next_id = match direction {
                                EdgeDirection::Right => edge_info.target,
                                EdgeDirection::Left => edge_info.source,
                                EdgeDirection::Undirected => {
                                    if edge_info.source == current_id {
                                        edge_info.target
                                    } else {
                                        edge_info.source
                                    }
                                }
                            };
                            if visited.insert(next_id) {
                                frontier.push((next_id, depth + 1));
                            }
                        }
                    }
                }
            }

            Ok(result)
        }

        Plan::Distinct { input } => {
            let rows = execute_plan(input, storage)?;
            let mut seen = HashSet::new();
            let mut result = Vec::new();

            for row in rows {
                // Create a hashable key from the row's column values
                let key: Vec<String> = row
                    .columns()
                    .iter()
                    .map(|col| {
                        format!("{}", row.get(col).cloned().unwrap_or(Value::Null))
                    })
                    .collect();
                if seen.insert(key) {
                    result.push(row);
                }
            }

            Ok(result)
        }
    }
}

// ---------------------------------------------------------------------------
// Expression evaluator
// ---------------------------------------------------------------------------

fn eval_expr(
    expr: &Expr,
    row: &Row,
    storage: &dyn StorageAccess,
) -> Result<Value, ExecutionError> {
    match expr {
        Expr::Literal(lit) => Ok(literal_to_value(lit)),

        Expr::Variable(name) => row
            .get(name)
            .cloned()
            .ok_or_else(|| ExecutionError::UnboundVariable(name.clone())),

        Expr::Property { expr, name } => {
            let base = eval_expr(expr, row, storage)?;
            match &base {
                Value::Node { id, .. } => Ok(storage.node_property(*id, name)),
                Value::Edge { id, .. } => Ok(storage.edge_property(*id, name)),
                _ => Err(ExecutionError::TypeError(format!(
                    "cannot access property '{name}' on {base}"
                ))),
            }
        }

        Expr::BinaryOp { left, op, right } => {
            let lv = eval_expr(left, row, storage)?;
            let rv = eval_expr(right, row, storage)?;
            eval_binary_op(*op, lv, rv)
        }

        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr, row, storage)?;
            eval_unary_op(*op, v)
        }

        Expr::FunctionCall { name, args } => {
            eval_function(name, args, row, storage)
        }
    }
}

fn literal_to_value(lit: &Literal) -> Value {
    match lit {
        Literal::Null => Value::Null,
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Int(n) => Value::Int(*n),
        Literal::Float(f) => Value::Float(*f),
        Literal::String(s) => Value::String(s.clone()),
    }
}

fn eval_binary_op(
    op: BinaryOp,
    left: Value,
    right: Value,
) -> Result<Value, ExecutionError> {
    // NULL propagation for most operations
    if matches!((&left, &right), (Value::Null, _) | (_, Value::Null)) {
        return match op {
            BinaryOp::Eq => Ok(Value::Bool(
                matches!((&left, &right), (Value::Null, Value::Null)),
            )),
            BinaryOp::Neq => Ok(Value::Bool(
                !matches!((&left, &right), (Value::Null, Value::Null)),
            )),
            _ => Ok(Value::Null),
        };
    }

    match op {
        // Comparison
        BinaryOp::Eq => Ok(Value::Bool(values_equal(&left, &right))),
        BinaryOp::Neq => Ok(Value::Bool(!values_equal(&left, &right))),
        BinaryOp::Lt => Ok(Value::Bool(
            compare_values(&left, &right) == std::cmp::Ordering::Less,
        )),
        BinaryOp::Gt => Ok(Value::Bool(
            compare_values(&left, &right) == std::cmp::Ordering::Greater,
        )),
        BinaryOp::Lte => Ok(Value::Bool(
            compare_values(&left, &right) != std::cmp::Ordering::Greater,
        )),
        BinaryOp::Gte => Ok(Value::Bool(
            compare_values(&left, &right) != std::cmp::Ordering::Less,
        )),

        // Logical
        BinaryOp::And => Ok(Value::Bool(left.is_truthy() && right.is_truthy())),
        BinaryOp::Or => Ok(Value::Bool(left.is_truthy() || right.is_truthy())),

        // Arithmetic
        BinaryOp::Add => numeric_op(&left, &right, |a, b| a + b, |a, b| a + b),
        BinaryOp::Sub => numeric_op(&left, &right, |a, b| a - b, |a, b| a - b),
        BinaryOp::Mul => numeric_op(&left, &right, |a, b| a * b, |a, b| a * b),
        BinaryOp::Div => {
            let is_zero = matches!(&right, Value::Int(0))
                || matches!(&right, Value::Float(f) if *f == 0.0);
            if is_zero {
                Err(ExecutionError::DivisionByZero)
            } else {
                numeric_op(&left, &right, |a, b| a / b, |a, b| a / b)
            }
        }
        BinaryOp::Mod => {
            if matches!(&right, Value::Int(0)) {
                Err(ExecutionError::DivisionByZero)
            } else {
                numeric_op(&left, &right, |a, b| a % b, |a, b| a % b)
            }
        }

        // String predicates
        BinaryOp::Contains => match (&left, &right) {
            (Value::String(haystack), Value::String(needle)) => {
                Ok(Value::Bool(haystack.contains(needle.as_str())))
            }
            _ => Err(ExecutionError::TypeError(
                "CONTAINS requires two strings".into(),
            )),
        },
        BinaryOp::StartsWith => match (&left, &right) {
            (Value::String(haystack), Value::String(prefix)) => {
                Ok(Value::Bool(haystack.starts_with(prefix.as_str())))
            }
            _ => Err(ExecutionError::TypeError(
                "STARTS WITH requires two strings".into(),
            )),
        },
        BinaryOp::EndsWith => match (&left, &right) {
            (Value::String(haystack), Value::String(suffix)) => {
                Ok(Value::Bool(haystack.ends_with(suffix.as_str())))
            }
            _ => Err(ExecutionError::TypeError(
                "ENDS WITH requires two strings".into(),
            )),
        },
    }
}

fn eval_unary_op(op: UnaryOp, val: Value) -> Result<Value, ExecutionError> {
    match op {
        UnaryOp::Not => Ok(Value::Bool(!val.is_truthy())),
        UnaryOp::Neg => match val {
            Value::Int(n) => Ok(Value::Int(-n)),
            Value::Float(f) => Ok(Value::Float(-f)),
            Value::Null => Ok(Value::Null),
            _ => Err(ExecutionError::TypeError(format!(
                "cannot negate {val}"
            ))),
        },
        UnaryOp::IsNull => Ok(Value::Bool(matches!(val, Value::Null))),
        UnaryOp::IsNotNull => Ok(Value::Bool(!matches!(val, Value::Null))),
    }
}

fn eval_function(
    name: &str,
    args: &[Expr],
    row: &Row,
    storage: &dyn StorageAccess,
) -> Result<Value, ExecutionError> {
    match name.to_ascii_uppercase().as_str() {
        "ID" => {
            let arg = args.first().ok_or_else(|| {
                ExecutionError::TypeError("id() requires one argument".into())
            })?;
            let val = eval_expr(arg, row, storage)?;
            match val {
                Value::Node { id, .. } => Ok(Value::Int(id.as_u64() as i64)),
                Value::Edge { id, .. } => Ok(Value::Int(id.as_u64() as i64)),
                _ => Err(ExecutionError::TypeError(
                    "id() requires a node or edge".into(),
                )),
            }
        }
        "TYPE" => {
            let arg = args.first().ok_or_else(|| {
                ExecutionError::TypeError("type() requires one argument".into())
            })?;
            let val = eval_expr(arg, row, storage)?;
            match val {
                Value::Edge { label, .. } => Ok(label
                    .map(Value::String)
                    .unwrap_or(Value::Null)),
                _ => Err(ExecutionError::TypeError(
                    "type() requires an edge".into(),
                )),
            }
        }
        "LABELS" => {
            let arg = args.first().ok_or_else(|| {
                ExecutionError::TypeError("labels() requires one argument".into())
            })?;
            let val = eval_expr(arg, row, storage)?;
            match val {
                Value::Node { label, .. } => Ok(label
                    .map(Value::String)
                    .unwrap_or(Value::Null)),
                _ => Err(ExecutionError::TypeError(
                    "labels() requires a node".into(),
                )),
            }
        }
        "TOSTRING" => {
            let arg = args.first().ok_or_else(|| {
                ExecutionError::TypeError("toString() requires one argument".into())
            })?;
            let val = eval_expr(arg, row, storage)?;
            Ok(Value::String(format!("{val}")))
        }
        "TOINTEGER" => {
            let arg = args.first().ok_or_else(|| {
                ExecutionError::TypeError("toInteger() requires one argument".into())
            })?;
            let val = eval_expr(arg, row, storage)?;
            match val {
                Value::Int(_) => Ok(val),
                Value::Float(f) => Ok(Value::Int(f as i64)),
                Value::String(ref s) => s
                    .parse::<i64>()
                    .map(Value::Int)
                    .map_err(|_| {
                        ExecutionError::TypeError(format!(
                            "cannot convert '{s}' to integer"
                        ))
                    }),
                Value::Bool(b) => Ok(Value::Int(if b { 1 } else { 0 })),
                Value::Null => Ok(Value::Null),
                _ => Err(ExecutionError::TypeError(format!(
                    "cannot convert {val} to integer"
                ))),
            }
        }
        "TOFLOAT" => {
            let arg = args.first().ok_or_else(|| {
                ExecutionError::TypeError("toFloat() requires one argument".into())
            })?;
            let val = eval_expr(arg, row, storage)?;
            match val {
                Value::Float(_) => Ok(val),
                Value::Int(n) => Ok(Value::Float(n as f64)),
                Value::String(ref s) => s
                    .parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| {
                        ExecutionError::TypeError(format!(
                            "cannot convert '{s}' to float"
                        ))
                    }),
                Value::Null => Ok(Value::Null),
                _ => Err(ExecutionError::TypeError(format!(
                    "cannot convert {val} to float"
                ))),
            }
        }
        _ => Err(ExecutionError::UnknownFunction(name.into())),
    }
}

fn numeric_op(
    left: &Value,
    right: &Value,
    int_op: fn(i64, i64) -> i64,
    float_op: fn(f64, f64) -> f64,
) -> Result<Value, ExecutionError> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(int_op(*a, *b))),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(float_op(*a, *b))),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(float_op(*a as f64, *b))),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(float_op(*a, *b as f64))),
        _ => Err(ExecutionError::TypeError(format!(
            "cannot perform arithmetic on {left} and {right}"
        ))),
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::Int(a), Value::Float(b)) => (*a as f64) == *b,
        (Value::Float(a), Value::Int(b)) => *a == (*b as f64),
        (Value::String(a), Value::String(b)) => a == b,
        _ => false,
    }
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Int(a), Value::Float(b)) => {
            (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (Value::Float(a), Value::Int(b)) => {
            a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // -- In-memory storage for testing --------------------------------------

    struct MemNode {
        id: NodeId,
        label: Option<String>,
        properties: HashMap<String, Value>,
    }

    struct MemEdge {
        id: EdgeId,
        source: NodeId,
        target: NodeId,
        label: Option<String>,
        properties: HashMap<String, Value>,
    }

    struct MemoryStorage {
        nodes: Vec<MemNode>,
        edges: Vec<MemEdge>,
        next_node_id: u64,
        next_edge_id: u64,
    }

    impl MemoryStorage {
        fn new() -> Self {
            Self {
                nodes: Vec::new(),
                edges: Vec::new(),
                next_node_id: 1,
                next_edge_id: 1,
            }
        }

        fn add_node(
            &mut self,
            label: Option<&str>,
            props: &[(&str, Value)],
        ) -> NodeId {
            let id = NodeId::from_raw(self.next_node_id);
            self.next_node_id += 1;
            let mut properties = HashMap::new();
            for (k, v) in props {
                properties.insert(k.to_string(), v.clone());
            }
            self.nodes.push(MemNode {
                id,
                label: label.map(String::from),
                properties,
            });
            id
        }

        fn add_edge(
            &mut self,
            source: NodeId,
            target: NodeId,
            label: Option<&str>,
            props: &[(&str, Value)],
        ) -> EdgeId {
            let id = EdgeId::from_raw(self.next_edge_id);
            self.next_edge_id += 1;
            let mut properties = HashMap::new();
            for (k, v) in props {
                properties.insert(k.to_string(), v.clone());
            }
            self.edges.push(MemEdge {
                id,
                source,
                target,
                label: label.map(String::from),
                properties,
            });
            id
        }
    }

    impl StorageAccess for MemoryStorage {
        fn scan_nodes(&self, label: Option<&str>) -> Vec<NodeInfo> {
            self.nodes
                .iter()
                .filter(|n| {
                    label.map_or(true, |l| n.label.as_deref() == Some(l))
                })
                .map(|n| NodeInfo {
                    id: n.id,
                    label: n.label.clone(),
                })
                .collect()
        }

        fn get_node(&self, id: NodeId) -> Option<NodeInfo> {
            self.nodes
                .iter()
                .find(|n| n.id == id)
                .map(|n| NodeInfo {
                    id: n.id,
                    label: n.label.clone(),
                })
        }

        fn node_property(&self, id: NodeId, key: &str) -> Value {
            self.nodes
                .iter()
                .find(|n| n.id == id)
                .and_then(|n| n.properties.get(key))
                .cloned()
                .unwrap_or(Value::Null)
        }

        fn outbound_edges(
            &self,
            node_id: NodeId,
            label: Option<&str>,
        ) -> Vec<EdgeInfo> {
            self.edges
                .iter()
                .filter(|e| {
                    e.source == node_id
                        && label.map_or(true, |l| e.label.as_deref() == Some(l))
                })
                .map(|e| EdgeInfo {
                    id: e.id,
                    source: e.source,
                    target: e.target,
                    label: e.label.clone(),
                })
                .collect()
        }

        fn inbound_edges(
            &self,
            node_id: NodeId,
            label: Option<&str>,
        ) -> Vec<EdgeInfo> {
            self.edges
                .iter()
                .filter(|e| {
                    e.target == node_id
                        && label.map_or(true, |l| e.label.as_deref() == Some(l))
                })
                .map(|e| EdgeInfo {
                    id: e.id,
                    source: e.source,
                    target: e.target,
                    label: e.label.clone(),
                })
                .collect()
        }

        fn edge_property(&self, id: EdgeId, key: &str) -> Value {
            self.edges
                .iter()
                .find(|e| e.id == id)
                .and_then(|e| e.properties.get(key))
                .cloned()
                .unwrap_or(Value::Null)
        }

        fn create_node(
            &mut self,
            label: Option<&str>,
            properties: &[(String, Value)],
        ) -> NodeId {
            let id = NodeId::from_raw(self.next_node_id);
            self.next_node_id += 1;
            let mut props = HashMap::new();
            for (k, v) in properties {
                props.insert(k.clone(), v.clone());
            }
            self.nodes.push(MemNode {
                id,
                label: label.map(String::from),
                properties: props,
            });
            id
        }

        fn create_edge(
            &mut self,
            source: NodeId,
            target: NodeId,
            label: Option<&str>,
            properties: &[(String, Value)],
        ) -> EdgeId {
            let id = EdgeId::from_raw(self.next_edge_id);
            self.next_edge_id += 1;
            let mut props = HashMap::new();
            for (k, v) in properties {
                props.insert(k.clone(), v.clone());
            }
            self.edges.push(MemEdge {
                id,
                source,
                target,
                label: label.map(String::from),
                properties: props,
            });
            id
        }

        fn set_node_property(&mut self, id: NodeId, key: &str, value: &Value) {
            if let Some(n) = self.nodes.iter_mut().find(|n| n.id == id) {
                n.properties.insert(key.to_string(), value.clone());
            }
        }

        fn set_edge_property(&mut self, id: EdgeId, key: &str, value: &Value) {
            if let Some(e) = self.edges.iter_mut().find(|e| e.id == id) {
                e.properties.insert(key.to_string(), value.clone());
            }
        }

        fn delete_node(&mut self, id: NodeId) {
            self.nodes.retain(|n| n.id != id);
            self.edges.retain(|e| e.source != id && e.target != id);
        }

        fn delete_edge(&mut self, id: EdgeId) {
            self.edges.retain(|e| e.id != id);
        }
    }

    // -- Helper to run a query against a storage ----------------------------

    fn run(
        query: &str,
        storage: &mut MemoryStorage,
    ) -> Result<QueryResult, String> {
        let stmt = crate::parse(query).map_err(|e| e)?;
        let plan =
            crate::planner::plan(&stmt).map_err(|e| e.to_string())?;
        execute(&plan, storage).map_err(|e| e.to_string())
    }

    fn setup_social_graph() -> MemoryStorage {
        let mut s = MemoryStorage::new();
        let alice = s.add_node(
            Some("Person"),
            &[("name", Value::String("Alice".into())), ("age", Value::Int(30))],
        );
        let bob = s.add_node(
            Some("Person"),
            &[("name", Value::String("Bob".into())), ("age", Value::Int(25))],
        );
        let charlie = s.add_node(
            Some("Person"),
            &[("name", Value::String("Charlie".into())), ("age", Value::Int(35))],
        );
        s.add_edge(alice, bob, Some("KNOWS"), &[("since", Value::Int(2020))]);
        s.add_edge(alice, charlie, Some("KNOWS"), &[("since", Value::Int(2019))]);
        s.add_edge(bob, charlie, Some("KNOWS"), &[("since", Value::Int(2021))]);
        s
    }

    // -- Tests --------------------------------------------------------------

    #[test]
    fn scan_all_persons() {
        let mut s = setup_social_graph();
        let result = run("MATCH (p:Person) RETURN p.name", &mut s).unwrap();
        assert_eq!(result.columns, vec!["p.name"]);
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn filter_by_age() {
        let mut s = setup_social_graph();
        let result = run(
            "MATCH (p:Person) WHERE p.age > 28 RETURN p.name",
            &mut s,
        )
        .unwrap();
        assert_eq!(result.rows.len(), 2); // Alice(30) and Charlie(35)
    }

    #[test]
    fn filter_by_property_pattern() {
        let mut s = setup_social_graph();
        let result = run(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age",
            &mut s,
        )
        .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(matches!(&result.rows[0][0], Value::Int(30)));
    }

    #[test]
    fn traverse_edges() {
        let mut s = setup_social_graph();
        let result = run(
            "MATCH (a:Person {name: 'Alice'})-[e:KNOWS]->(b:Person) RETURN b.name",
            &mut s,
        )
        .unwrap();
        assert_eq!(result.rows.len(), 2); // Bob and Charlie
    }

    #[test]
    fn order_by() {
        let mut s = setup_social_graph();
        let result = run(
            "MATCH (p:Person) RETURN p.name ORDER BY p.age DESC",
            &mut s,
        )
        .unwrap();
        assert_eq!(result.rows.len(), 3);
        assert!(matches!(&result.rows[0][0], Value::String(s) if s == "Charlie"));
        assert!(matches!(&result.rows[1][0], Value::String(s) if s == "Alice"));
        assert!(matches!(&result.rows[2][0], Value::String(s) if s == "Bob"));
    }

    #[test]
    fn limit_and_skip() {
        let mut s = setup_social_graph();
        let result = run(
            "MATCH (p:Person) RETURN p.name ORDER BY p.age LIMIT 2",
            &mut s,
        )
        .unwrap();
        assert_eq!(result.rows.len(), 2);

        let result = run(
            "MATCH (p:Person) RETURN p.name ORDER BY p.age SKIP 1 LIMIT 1",
            &mut s,
        )
        .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn aggregate_count() {
        let mut s = setup_social_graph();
        let result =
            run("MATCH (p:Person) RETURN COUNT(p)", &mut s).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(matches!(&result.rows[0][0], Value::Int(3)));
    }

    #[test]
    fn aggregate_avg() {
        let mut s = setup_social_graph();
        let result =
            run("MATCH (p:Person) RETURN AVG(p.age)", &mut s).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(matches!(&result.rows[0][0], Value::Float(f) if (*f - 30.0).abs() < 0.01));
    }

    #[test]
    fn create_node() {
        let mut s = MemoryStorage::new();
        run("CREATE (:Person {name: 'Alice', age: 30})", &mut s).unwrap();
        assert_eq!(s.nodes.len(), 1);
        assert_eq!(s.nodes[0].label.as_deref(), Some("Person"));
        assert!(matches!(
            s.nodes[0].properties.get("name"),
            Some(Value::String(s)) if s == "Alice"
        ));
    }

    #[test]
    fn create_edge_between_matched_nodes() {
        let mut s = MemoryStorage::new();
        let alice = s.add_node(Some("Person"), &[("name", Value::String("Alice".into()))]);
        let bob = s.add_node(Some("Person"), &[("name", Value::String("Bob".into()))]);

        run(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:FRIENDS]->(b)",
            &mut s,
        )
        .unwrap();

        assert_eq!(s.edges.len(), 1);
        assert_eq!(s.edges[0].source, alice);
        assert_eq!(s.edges[0].target, bob);
        assert_eq!(s.edges[0].label.as_deref(), Some("FRIENDS"));
    }

    #[test]
    fn set_property() {
        let mut s = MemoryStorage::new();
        s.add_node(
            Some("Person"),
            &[("name", Value::String("Alice".into())), ("age", Value::Int(30))],
        );

        run(
            "MATCH (p:Person {name: 'Alice'}) SET p.age = 31",
            &mut s,
        )
        .unwrap();

        assert!(matches!(
            s.nodes[0].properties.get("age"),
            Some(Value::Int(31))
        ));
    }

    #[test]
    fn delete_node() {
        let mut s = MemoryStorage::new();
        s.add_node(
            Some("Person"),
            &[("name", Value::String("Alice".into()))],
        );
        s.add_node(
            Some("Person"),
            &[("name", Value::String("Bob".into()))],
        );
        assert_eq!(s.nodes.len(), 2);

        run(
            "MATCH (p:Person {name: 'Alice'}) DELETE p",
            &mut s,
        )
        .unwrap();

        assert_eq!(s.nodes.len(), 1);
        assert_eq!(
            s.nodes[0].properties.get("name"),
            Some(&Value::String("Bob".into()))
        );
    }

    #[test]
    fn expression_arithmetic() {
        let mut s = setup_social_graph();
        let result = run(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age + 10",
            &mut s,
        )
        .unwrap();
        assert!(matches!(&result.rows[0][0], Value::Int(40)));
    }

    #[test]
    fn expression_logic() {
        let mut s = setup_social_graph();
        let result = run(
            "MATCH (p:Person) WHERE p.age > 25 AND p.age < 32 RETURN p.name",
            &mut s,
        )
        .unwrap();
        assert_eq!(result.rows.len(), 1); // Only Alice (30)
    }

    #[test]
    fn return_with_alias() {
        let mut s = setup_social_graph();
        let result = run(
            "MATCH (p:Person) RETURN p.name AS person_name LIMIT 1",
            &mut s,
        )
        .unwrap();
        assert_eq!(result.columns, vec!["person_name"]);
    }
}
