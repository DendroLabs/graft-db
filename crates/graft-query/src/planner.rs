use crate::ast::*;

// ---------------------------------------------------------------------------
// Plan — serves as both logical and physical plan for Phase 1.
//
// In future, this splits into a declarative LogicalPlan (what to compute)
// and an imperative PhysicalPlan (how to compute it), with an optimizer
// in between. For now they are the same structure.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum Plan {
    /// Scan all nodes, optionally filtered by label.
    NodeScan {
        variable: String,
        label: Option<String>,
    },

    /// Follow edges from a source node.
    Expand {
        input: Box<Plan>,
        src_var: String,
        edge_var: Option<String>,
        edge_label: Option<String>,
        direction: EdgeDirection,
        dst_var: String,
        dst_label: Option<String>,
    },

    /// Cartesian product of two independent patterns.
    NestedLoop {
        outer: Box<Plan>,
        inner: Box<Plan>,
    },

    /// Filter rows by a predicate.
    Filter {
        input: Box<Plan>,
        predicate: Expr,
    },

    /// Project to specific columns.
    Project {
        input: Box<Plan>,
        items: Vec<ReturnItem>,
    },

    /// Sort output rows.
    Sort {
        input: Box<Plan>,
        items: Vec<OrderByItem>,
    },

    /// Limit output row count.
    Limit {
        input: Box<Plan>,
        count: u64,
    },

    /// Skip leading rows.
    Skip {
        input: Box<Plan>,
        count: u64,
    },

    /// Create a node.
    CreateNode {
        input: Option<Box<Plan>>,
        variable: Option<String>,
        label: Option<String>,
        properties: Vec<(String, Expr)>,
    },

    /// Create an edge between two bound variables.
    CreateEdge {
        input: Box<Plan>,
        variable: Option<String>,
        label: Option<String>,
        from_var: String,
        to_var: String,
        properties: Vec<(String, Expr)>,
    },

    /// Set a property on a bound variable.
    SetProperty {
        input: Box<Plan>,
        variable: String,
        property: String,
        value: Expr,
    },

    /// Delete a node or edge.
    Delete {
        input: Box<Plan>,
        variable: String,
    },

    /// Compute aggregate functions over all input rows.
    Aggregate {
        input: Box<Plan>,
        ops: Vec<AggOp>,
    },

    /// Variable-length path expansion (BFS).
    VarExpand {
        input: Box<Plan>,
        src_var: String,
        edge_label: Option<String>,
        direction: EdgeDirection,
        dst_var: String,
        dst_label: Option<String>,
        min_hops: u64,
        max_hops: Option<u64>,
    },

    /// Deduplicate rows.
    Distinct {
        input: Box<Plan>,
    },
}

#[derive(Clone, Debug)]
pub struct AggOp {
    pub function: AggFunction,
    pub input_expr: Expr,
    pub output_name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum PlanError {
    EmptyStatement,
    InvalidPattern(String),
    MissingInput(String),
    UnknownAggregateFunction(String),
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyStatement => write!(f, "empty statement"),
            Self::InvalidPattern(msg) => write!(f, "invalid pattern: {msg}"),
            Self::MissingInput(msg) => write!(f, "clause requires prior input: {msg}"),
            Self::UnknownAggregateFunction(name) => {
                write!(f, "unknown aggregate function: {name}")
            }
        }
    }
}

impl std::error::Error for PlanError {}

// ---------------------------------------------------------------------------
// Planner state
// ---------------------------------------------------------------------------

struct PlannerState {
    anon_counter: usize,
}

impl PlannerState {
    fn new() -> Self {
        Self { anon_counter: 0 }
    }

    fn next_anon(&mut self) -> String {
        let name = format!("_anon_{}", self.anon_counter);
        self.anon_counter += 1;
        name
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Convert a parsed AST into an execution plan.
///
/// Clauses are reordered into correct execution order:
///   MATCH → WHERE → SET/DELETE → ORDER BY → SKIP → LIMIT → RETURN
/// This ensures ORDER BY can access original variables before projection.
pub fn plan(stmt: &Statement) -> Result<Plan, PlanError> {
    let mut state = PlannerState::new();
    let mut current: Option<Plan> = None;

    // Phase 1: MATCH, CREATE, WHERE, SET, DELETE (in statement order)
    // Phase 2: ORDER BY, SKIP, LIMIT (deferred)
    // Phase 3: RETURN (last — projection)
    let mut deferred_order_by: Option<&OrderByClause> = None;
    let mut deferred_limit: Option<u64> = None;
    let mut deferred_skip: Option<u64> = None;
    let mut deferred_return: Option<&ReturnClause> = None;

    for clause in &stmt.clauses {
        match clause {
            Clause::Match(m) => {
                current = Some(plan_match(m, current, &mut state)?);
            }
            Clause::Create(c) => {
                current = Some(plan_create(c, current, &mut state)?);
            }
            Clause::Where(w) => {
                current = Some(plan_where(w, current)?);
            }
            Clause::Set(s) => {
                current = Some(plan_set(s, current)?);
            }
            Clause::Delete(d) => {
                current = Some(plan_delete(d, current)?);
            }
            Clause::OrderBy(o) => deferred_order_by = Some(o),
            Clause::Limit(n) => deferred_limit = Some(*n),
            Clause::Skip(n) => deferred_skip = Some(*n),
            Clause::Return(r) => deferred_return = Some(r),
        }
    }

    // Apply ORDER BY before projection
    if let Some(o) = deferred_order_by {
        current = Some(plan_order_by(o, current)?);
    }

    // Apply SKIP, then LIMIT
    if let Some(n) = deferred_skip {
        current = Some(plan_skip(n, current)?);
    }
    if let Some(n) = deferred_limit {
        current = Some(plan_limit(n, current)?);
    }

    // Apply RETURN (projection) last
    if let Some(r) = deferred_return {
        current = Some(plan_return(r, current)?);
    }

    current.ok_or(PlanError::EmptyStatement)
}

// ---------------------------------------------------------------------------
// Clause planners
// ---------------------------------------------------------------------------

fn plan_match(
    m: &MatchClause,
    input: Option<Plan>,
    state: &mut PlannerState,
) -> Result<Plan, PlanError> {
    let mut plan = plan_pattern(&m.patterns[0], state)?;

    for pattern in &m.patterns[1..] {
        let right = plan_pattern(pattern, state)?;
        plan = Plan::NestedLoop {
            outer: Box::new(plan),
            inner: Box::new(right),
        };
    }

    if let Some(input) = input {
        plan = Plan::NestedLoop {
            outer: Box::new(input),
            inner: Box::new(plan),
        };
    }

    Ok(plan)
}

fn plan_create(
    c: &CreateClause,
    input: Option<Plan>,
    state: &mut PlannerState,
) -> Result<Plan, PlanError> {
    let mut plan = input;

    for pattern in &c.patterns {
        plan = Some(plan_create_pattern(pattern, plan, state)?);
    }

    plan.ok_or(PlanError::EmptyStatement)
}

fn plan_where(w: &WhereClause, input: Option<Plan>) -> Result<Plan, PlanError> {
    let input = input.ok_or_else(|| PlanError::MissingInput("WHERE".into()))?;
    Ok(Plan::Filter {
        input: Box::new(input),
        predicate: w.expr.clone(),
    })
}

fn plan_return(r: &ReturnClause, input: Option<Plan>) -> Result<Plan, PlanError> {
    let input = input.ok_or_else(|| PlanError::MissingInput("RETURN".into()))?;

    // Check if any return items are aggregate functions
    let has_agg = r.items.iter().any(|item| is_aggregate_expr(&item.expr));
    let all_agg = r.items.iter().all(|item| is_aggregate_expr(&item.expr));

    if has_agg {
        if !all_agg {
            return Err(PlanError::InvalidPattern(
                "mixed aggregate and non-aggregate expressions require GROUP BY (not yet supported)"
                    .into(),
            ));
        }

        let mut ops = Vec::new();
        for item in &r.items {
            let (func, arg) = match &item.expr {
                Expr::FunctionCall { name, args } => {
                    let func = parse_agg_function(name)?;
                    let arg = args.first().cloned().unwrap_or(Expr::Literal(Literal::Null));
                    (func, arg)
                }
                _ => unreachable!("checked by all_agg"),
            };
            let output_name = item
                .alias
                .clone()
                .unwrap_or_else(|| expr_display_name(&item.expr));
            ops.push(AggOp {
                function: func,
                input_expr: arg,
                output_name,
            });
        }

        Ok(Plan::Aggregate {
            input: Box::new(input),
            ops,
        })
    } else {
        let mut plan = Plan::Project {
            input: Box::new(input),
            items: r.items.clone(),
        };
        if r.distinct {
            plan = Plan::Distinct {
                input: Box::new(plan),
            };
        }
        Ok(plan)
    }
}

fn plan_order_by(o: &OrderByClause, input: Option<Plan>) -> Result<Plan, PlanError> {
    let input = input.ok_or_else(|| PlanError::MissingInput("ORDER BY".into()))?;
    Ok(Plan::Sort {
        input: Box::new(input),
        items: o.items.clone(),
    })
}

fn plan_limit(n: u64, input: Option<Plan>) -> Result<Plan, PlanError> {
    let input = input.ok_or_else(|| PlanError::MissingInput("LIMIT".into()))?;
    Ok(Plan::Limit {
        input: Box::new(input),
        count: n,
    })
}

fn plan_skip(n: u64, input: Option<Plan>) -> Result<Plan, PlanError> {
    let input = input.ok_or_else(|| PlanError::MissingInput("SKIP".into()))?;
    Ok(Plan::Skip {
        input: Box::new(input),
        count: n,
    })
}

fn plan_set(s: &SetClause, input: Option<Plan>) -> Result<Plan, PlanError> {
    let mut plan = input.ok_or_else(|| PlanError::MissingInput("SET".into()))?;

    for item in &s.items {
        let (variable, property) = match &item.target {
            Expr::Property { expr, name } => match expr.as_ref() {
                Expr::Variable(var) => (var.clone(), name.clone()),
                _ => {
                    return Err(PlanError::InvalidPattern(
                        "SET target must be variable.property".into(),
                    ))
                }
            },
            _ => {
                return Err(PlanError::InvalidPattern(
                    "SET target must be variable.property".into(),
                ))
            }
        };
        plan = Plan::SetProperty {
            input: Box::new(plan),
            variable,
            property,
            value: item.value.clone(),
        };
    }

    Ok(plan)
}

fn plan_delete(d: &DeleteClause, input: Option<Plan>) -> Result<Plan, PlanError> {
    let mut plan = input.ok_or_else(|| PlanError::MissingInput("DELETE".into()))?;

    for var in &d.variables {
        plan = Plan::Delete {
            input: Box::new(plan),
            variable: var.clone(),
        };
    }

    Ok(plan)
}

// ---------------------------------------------------------------------------
// Pattern planners
// ---------------------------------------------------------------------------

fn plan_pattern(pattern: &Pattern, state: &mut PlannerState) -> Result<Plan, PlanError> {
    match pattern {
        Pattern::Node(n) => plan_node_pattern(n, state),
        Pattern::Path(elements) => plan_path_pattern(elements, state),
    }
}

fn plan_node_pattern(n: &NodePattern, state: &mut PlannerState) -> Result<Plan, PlanError> {
    let variable = n.variable.clone().unwrap_or_else(|| state.next_anon());

    let mut plan = Plan::NodeScan {
        variable: variable.clone(),
        label: n.label.clone(),
    };

    // Convert inline properties to filter predicates
    for (key, value) in &n.properties {
        plan = Plan::Filter {
            input: Box::new(plan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Property {
                    expr: Box::new(Expr::Variable(variable.clone())),
                    name: key.clone(),
                }),
                op: BinaryOp::Eq,
                right: Box::new(value.clone()),
            },
        };
    }

    Ok(plan)
}

fn plan_path_pattern(
    elements: &[PathElement],
    state: &mut PlannerState,
) -> Result<Plan, PlanError> {
    let PathElement::Node(first_node) = &elements[0] else {
        return Err(PlanError::InvalidPattern(
            "path must start with a node".into(),
        ));
    };

    let mut plan = plan_node_pattern(first_node, state)?;
    let mut src_var = first_node
        .variable
        .clone()
        .unwrap_or_else(|| state.next_anon());

    let mut i = 1;
    while i < elements.len() {
        let PathElement::Edge(edge) = &elements[i] else {
            return Err(PlanError::InvalidPattern("expected edge".into()));
        };
        i += 1;
        let PathElement::Node(node) = &elements[i] else {
            return Err(PlanError::InvalidPattern(
                "expected node after edge".into(),
            ));
        };
        i += 1;

        let dst_var = node.variable.clone().unwrap_or_else(|| state.next_anon());

        if let Some(ref quantifier) = edge.quantifier {
            // Variable-length path — no edge variable binding
            plan = Plan::VarExpand {
                input: Box::new(plan),
                src_var: src_var.clone(),
                edge_label: edge.label.clone(),
                direction: edge.direction,
                dst_var: dst_var.clone(),
                dst_label: node.label.clone(),
                min_hops: quantifier.min_hops,
                max_hops: quantifier.max_hops,
            };
        } else {
            plan = Plan::Expand {
                input: Box::new(plan),
                src_var: src_var.clone(),
                edge_var: edge.variable.clone(),
                edge_label: edge.label.clone(),
                direction: edge.direction,
                dst_var: dst_var.clone(),
                dst_label: node.label.clone(),
            };
        }

        // Property filters on the destination node
        for (key, value) in &node.properties {
            plan = Plan::Filter {
                input: Box::new(plan),
                predicate: Expr::BinaryOp {
                    left: Box::new(Expr::Property {
                        expr: Box::new(Expr::Variable(dst_var.clone())),
                        name: key.clone(),
                    }),
                    op: BinaryOp::Eq,
                    right: Box::new(value.clone()),
                },
            };
        }

        // Property filters on the edge
        if let Some(ref evar) = edge.variable {
            for (key, value) in &edge.properties {
                plan = Plan::Filter {
                    input: Box::new(plan),
                    predicate: Expr::BinaryOp {
                        left: Box::new(Expr::Property {
                            expr: Box::new(Expr::Variable(evar.clone())),
                            name: key.clone(),
                        }),
                        op: BinaryOp::Eq,
                        right: Box::new(value.clone()),
                    },
                };
            }
        }

        src_var = dst_var;
    }

    Ok(plan)
}

fn plan_create_pattern(
    pattern: &Pattern,
    input: Option<Plan>,
    state: &mut PlannerState,
) -> Result<Plan, PlanError> {
    match pattern {
        Pattern::Node(n) => {
            let variable = n.variable.clone().unwrap_or_else(|| state.next_anon());
            Ok(Plan::CreateNode {
                input: input.map(Box::new),
                variable: Some(variable),
                label: n.label.clone(),
                properties: n.properties.clone(),
            })
        }
        Pattern::Path(elements) => {
            // Path in CREATE: (a)-[e:LABEL]->(b)
            // If a and b are already bound (from MATCH), create just the edge.
            // If they are new, create the nodes first, then the edge.
            let mut plan = input;

            // First pass: create any new nodes
            for elem in elements {
                if let PathElement::Node(n) = elem {
                    if n.variable.is_some() && n.label.is_some() {
                        // Could be a new node — create it
                        // In a real implementation, we'd check if the variable
                        // is already bound. For Phase 1, CREATE path assumes
                        // nodes are already bound from MATCH.
                    }
                }
            }

            // Create edges
            let mut i = 0;
            while i + 2 < elements.len() {
                let PathElement::Node(src) = &elements[i] else {
                    return Err(PlanError::InvalidPattern("expected node".into()));
                };
                let PathElement::Edge(edge) = &elements[i + 1] else {
                    return Err(PlanError::InvalidPattern("expected edge".into()));
                };
                let PathElement::Node(dst) = &elements[i + 2] else {
                    return Err(PlanError::InvalidPattern("expected node".into()));
                };

                let from_var = src.variable.clone().unwrap_or_else(|| state.next_anon());
                let to_var = dst.variable.clone().unwrap_or_else(|| state.next_anon());

                let (actual_from, actual_to) = match edge.direction {
                    EdgeDirection::Right => (from_var, to_var),
                    EdgeDirection::Left => (to_var, from_var),
                    EdgeDirection::Undirected => (from_var, to_var),
                };

                let input_plan =
                    plan.ok_or_else(|| PlanError::MissingInput("CREATE edge".into()))?;
                plan = Some(Plan::CreateEdge {
                    input: Box::new(input_plan),
                    variable: edge.variable.clone(),
                    label: edge.label.clone(),
                    from_var: actual_from,
                    to_var: actual_to,
                    properties: edge.properties.clone(),
                });

                i += 2;
            }

            plan.ok_or(PlanError::EmptyStatement)
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn is_aggregate_expr(expr: &Expr) -> bool {
    matches!(expr, Expr::FunctionCall { name, .. }
        if matches!(name.to_ascii_uppercase().as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"))
}

fn parse_agg_function(name: &str) -> Result<AggFunction, PlanError> {
    match name.to_ascii_uppercase().as_str() {
        "COUNT" => Ok(AggFunction::Count),
        "SUM" => Ok(AggFunction::Sum),
        "AVG" => Ok(AggFunction::Avg),
        "MIN" => Ok(AggFunction::Min),
        "MAX" => Ok(AggFunction::Max),
        _ => Err(PlanError::UnknownAggregateFunction(name.into())),
    }
}

/// Generate a display name for an expression (used as default column name).
pub fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Variable(name) => name.clone(),
        Expr::Property { expr, name } => {
            format!("{}.{}", expr_display_name(expr), name)
        }
        Expr::FunctionCall { name, args } => {
            let args_str: Vec<_> = args.iter().map(expr_display_name).collect();
            format!("{}({})", name, args_str.join(", "))
        }
        Expr::Literal(lit) => match lit {
            Literal::Null => "NULL".into(),
            Literal::Bool(b) => b.to_string(),
            Literal::Int(n) => n.to_string(),
            Literal::Float(n) => n.to_string(),
            Literal::String(s) => format!("'{s}'"),
        },
        _ => "?".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_and_plan(input: &str) -> Plan {
        let stmt = crate::parse(input).unwrap();
        plan(&stmt).unwrap()
    }

    #[test]
    fn simple_match_return() {
        let plan = parse_and_plan("MATCH (p:Person) RETURN p.name");
        // Should be Project(Filter or NodeScan)
        assert!(matches!(plan, Plan::Project { .. }));
    }

    #[test]
    fn match_where_return() {
        let plan = parse_and_plan("MATCH (p:Person) WHERE p.age > 25 RETURN p.name");
        assert!(matches!(plan, Plan::Project { .. }));
    }

    #[test]
    fn match_path() {
        let plan = parse_and_plan(
            "MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN a.name, b.name",
        );
        assert!(matches!(plan, Plan::Project { .. }));
    }

    #[test]
    fn create_node() {
        let plan = parse_and_plan("CREATE (:Person {name: 'Alice'})");
        assert!(matches!(plan, Plan::CreateNode { .. }));
    }

    #[test]
    fn aggregate_count() {
        let plan = parse_and_plan("MATCH (p:Person) RETURN COUNT(p)");
        assert!(matches!(plan, Plan::Aggregate { .. }));
    }

    #[test]
    fn order_by_limit_skip() {
        let plan = parse_and_plan(
            "MATCH (p:Person) RETURN p.name ORDER BY p.age DESC LIMIT 10 SKIP 5",
        );
        // Execution order: Sort → Skip → Limit → Project
        assert!(matches!(plan, Plan::Project { .. }));
    }
}
