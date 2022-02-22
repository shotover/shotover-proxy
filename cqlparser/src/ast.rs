#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(Select),
    Insert(Insert),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {}

#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub distinct: bool,
    pub json: bool,
    pub select: Vec<SelectElement>,
    pub from: Vec<String>,
    /// Every element is AND'd together
    pub where_: Vec<RelationElement>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<u64>,
    pub allow_filtering: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectElement {
    pub expr: Expr,
    pub as_alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RelationElement {
    Comparison(RelationComparison),
    In(RelationIn),
    Contains(RelationContains),
    ContainsKey(RelationContainsKey),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationComparison {
    pub lhs: Expr,
    pub operator: ComparisonOperator,
    pub rhs: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationIn {
    pub lhs: String,
    pub rhs: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationContains {
    pub lhs: String,
    pub rhs: Constant,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationContainsKey {
    pub lhs: String,
    pub rhs: Constant,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOperator {
    Equals,
    LessThan,
    LessThanOrEqualTo,
    GreaterThan,
    GreaterThanOrEqualTo,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub name: String,
    pub ordering: Ordering,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Ordering {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Name(String),
    Constant(Constant),
    FunctionCall(FunctionCall),
    Wildcard,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Constant {
    UUID,
    String(String),
    Decimal(i64),
    Float(f64), // TODO: we should store raw instead of ieee
    Hex(i64),
    Bool(bool),
    CodeBlock(String),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub function: String,
    pub args: Vec<Expr>,
}
