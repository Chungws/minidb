const std = @import("std");

pub const DataType = enum {
    integer,
    text,
    boolean,
};

pub const Statement = union(enum) {
    select: SelectStatement,
    insert: InsertStatement,
    create_table: CreateTableStatement,
    create_index: CreateIndexStatement,
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

pub const CreateIndexStatement = struct {
    index_name: []const u8,
    table_name: []const u8,
    column_name: []const u8,
};

pub const ColumnDef = struct {
    name: []const u8,
    data_type: DataType,
    nullable: bool,
};

pub const Condition = union(enum) {
    simple: SimpleCondition,
    and_op: struct { left: *Condition, right: *Condition },
    or_op: struct { left: *Condition, right: *Condition },
    not_op: *Condition,
};

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
    text: []const u8,
    boolean: bool,
    null_value: void,

    pub fn compareValue(self: *const Value, rhs: Value, op: Operator) bool {
        return switch (self.*) {
            .integer => |v| switch (rhs) {
                .integer => |rhsv| compareInt(v, rhsv, op),
                else => false,
            },
            .text => |v| switch (rhs) {
                .text => |rhsv| compareStr(v, rhsv, op),
                else => false,
            },
            .boolean => |v| switch (rhs) {
                .boolean => |rhsv| compareBool(v, rhsv, op),
                else => false,
            },
            .null_value => false,
        };
    }

    fn compareInt(a: i64, b: i64, op: Operator) bool {
        return switch (op) {
            .eq => a == b,
            .neq => a != b,
            .lt => a < b,
            .gt => a > b,
            .lte => a <= b,
            .gte => a >= b,
        };
    }

    fn compareStr(a: []const u8, b: []const u8, op: Operator) bool {
        const order = std.mem.order(u8, a, b);
        return switch (op) {
            .eq => order == .eq,
            .neq => order != .eq,
            .lt => order == .lt,
            .gt => order == .gt,
            .lte => order == .lt or order == .eq,
            .gte => order == .gt or order == .eq,
        };
    }

    fn compareBool(a: bool, b: bool, op: Operator) bool {
        return switch (op) {
            .eq => a == b,
            .neq => a != b,
            else => false,
        };
    }
};
