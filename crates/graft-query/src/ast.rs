// GQL Abstract Syntax Tree — Phase 1 subset

// ---------------------------------------------------------------------------
// Statement
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct Statement {
    pub clauses: Vec<Clause>,
}

#[derive(Clone, Debug)]
pub enum Clause {
    Match(MatchClause),
    Create(CreateClause),
    Where(WhereClause),
    Return(ReturnClause),
    OrderBy(OrderByClause),
    Limit(u64),
    Skip(u64),
    Set(SetClause),
    Delete(DeleteClause),
}

// ---------------------------------------------------------------------------
// MATCH / CREATE — patterns
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct MatchClause {
    pub patterns: Vec<Pattern>,
}

#[derive(Clone, Debug)]
pub struct CreateClause {
    pub patterns: Vec<Pattern>,
}

#[derive(Clone, Debug)]
pub enum Pattern {
    Node(NodePattern),
    Path(Vec<PathElement>),
}

#[derive(Clone, Debug)]
pub enum PathElement {
    Node(NodePattern),
    Edge(EdgePattern),
}

#[derive(Clone, Debug)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub label: Option<String>,
    pub properties: Vec<(String, Expr)>,
}

#[derive(Clone, Debug)]
pub struct EdgePattern {
    pub variable: Option<String>,
    pub label: Option<String>,
    pub properties: Vec<(String, Expr)>,
    pub direction: EdgeDirection,
    pub quantifier: Option<PathQuantifier>,
}

#[derive(Clone, Debug)]
pub struct PathQuantifier {
    pub min_hops: u64,
    pub max_hops: Option<u64>, // None = unbounded
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeDirection {
    Right,
    Left,
    Undirected,
}

// ---------------------------------------------------------------------------
// WHERE
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct WhereClause {
    pub expr: Expr,
}

// ---------------------------------------------------------------------------
// RETURN
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
}

#[derive(Clone, Debug)]
pub struct ReturnItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

// ---------------------------------------------------------------------------
// ORDER BY
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct OrderByClause {
    pub items: Vec<OrderByItem>,
}

#[derive(Clone, Debug)]
pub struct OrderByItem {
    pub expr: Expr,
    pub direction: SortDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

// ---------------------------------------------------------------------------
// SET
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

#[derive(Clone, Debug)]
pub struct SetItem {
    pub target: Expr,
    pub value: Expr,
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct DeleteClause {
    pub variables: Vec<String>,
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum Expr {
    Literal(Literal),
    Variable(String),
    Property {
        expr: Box<Expr>,
        name: String,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    // Comparison
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    // Logical
    And,
    Or,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // String
    Contains,
    StartsWith,
    EndsWith,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}
