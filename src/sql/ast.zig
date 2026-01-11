pub const DataType = enum {
    INTEGER,
    TEXT,
    BOOL,
};

pub const Statement = union(enum) {
    select: SelectStatement,
    insert: InsertStatement,
    create_table: CreateTableStatement,
};

pub const SelectStatement = struct {
    columns: []const []const u8,
    table_name: []const u8,
    where: ?Condition,
};

pub const InsertStatement = struct {
    table_name: []const u8,
    values: []const Value,
};

pub const CreateTableStatement = struct {
    table_name: []const u8,
    columns: []const ColumnDef,
};

pub const ColumnDef = struct {
    name: []const u8,
    data_type: DataType,
};

pub const Condition = union(enum) { simple: SimpleCondition, and_op: struct { left: *Condition, right: *Condition }, or_op: struct { left: *Condition, right: *Condition }, not_op: *Condition };

pub const SimpleCondition = struct {
    column: []const u8,
    op: Operator,
    value: Value,
};

pub const Operator = enum {
    eq,
    neq,
    lt,
    gt,
    lte,
    gte,
};

pub const Value = union(enum) {
    integer: i64,
    string: []const u8,
    boolean: bool,
};
