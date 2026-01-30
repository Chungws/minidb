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
    join: ?JoinClause,
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

pub const JoinClause = struct {
    table_name: []const u8,
    left_column: []const u8,
    right_column: []const u8,
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

test "compareValue integer eq" {
    const a = Value{ .integer = 10 };
    const b = Value{ .integer = 10 };
    try std.testing.expect(a.compareValue(b, .eq));
    try std.testing.expect(!a.compareValue(b, .neq));
}

test "compareValue integer ordering" {
    const a = Value{ .integer = 5 };
    const b = Value{ .integer = 10 };
    try std.testing.expect(a.compareValue(b, .lt));
    try std.testing.expect(a.compareValue(b, .lte));
    try std.testing.expect(!a.compareValue(b, .gt));
    try std.testing.expect(!a.compareValue(b, .gte));
    try std.testing.expect(a.compareValue(b, .neq));
}

test "compareValue text eq" {
    const a = Value{ .text = "alice" };
    const b = Value{ .text = "alice" };
    try std.testing.expect(a.compareValue(b, .eq));
    try std.testing.expect(!a.compareValue(b, .neq));
}

test "compareValue text ordering" {
    const a = Value{ .text = "alice" };
    const b = Value{ .text = "bob" };
    try std.testing.expect(a.compareValue(b, .lt));
    try std.testing.expect(a.compareValue(b, .lte));
    try std.testing.expect(!a.compareValue(b, .gt));
    try std.testing.expect(!a.compareValue(b, .gte));
}

test "compareValue boolean eq" {
    const a = Value{ .boolean = true };
    const b = Value{ .boolean = true };
    const c = Value{ .boolean = false };
    try std.testing.expect(a.compareValue(b, .eq));
    try std.testing.expect(!a.compareValue(c, .eq));
    try std.testing.expect(a.compareValue(c, .neq));
}

test "compareValue boolean no ordering" {
    const a = Value{ .boolean = true };
    const b = Value{ .boolean = false };
    try std.testing.expect(!a.compareValue(b, .lt));
    try std.testing.expect(!a.compareValue(b, .gt));
}

test "compareValue type mismatch returns false" {
    const int_val = Value{ .integer = 1 };
    const text_val = Value{ .text = "1" };
    const bool_val = Value{ .boolean = true };
    try std.testing.expect(!int_val.compareValue(text_val, .eq));
    try std.testing.expect(!int_val.compareValue(bool_val, .eq));
    try std.testing.expect(!text_val.compareValue(bool_val, .eq));
}

test "compareValue null always false" {
    const null_val = Value{ .null_value = {} };
    const int_val = Value{ .integer = 1 };
    try std.testing.expect(!null_val.compareValue(int_val, .eq));
    try std.testing.expect(!null_val.compareValue(null_val, .eq));
}
