const std = @import("std");

const Catalog = @import("query/catalog.zig").Catalog;
const Parser = @import("sql/parser.zig").Parser;
const Planner = @import("query/planner.zig").Planner;
const Executor = @import("query/executor.zig").Executor;
const Value = @import("sql/ast.zig").Value;
const Statement = @import("sql/ast.zig").Statement;
const Tuple = @import("record/tuple.zig").Tuple;

// ============ Result Types ============

pub const ExecuteError = union(enum) {
    parse: anyerror,
    execute: anyerror,
};

pub const SelectResult = struct {
    rows: std.ArrayList(Tuple),
    allocator: std.mem.Allocator,

    pub fn deinit(self: *SelectResult) void {
        for (self.rows.items) |*row| {
            row.deinit(self.allocator);
        }
        self.rows.deinit(self.allocator);
    }
};

pub const ExecuteResult = union(enum) {
    table_created,
    index_created,
    row_inserted,
    select: SelectResult,
    err: ExecuteError,
};

// ============ Pure Functions ============

pub fn execute(catalog: *Catalog, sql: []const u8, allocator: std.mem.Allocator) ExecuteResult {
    var parser = Parser.init(sql, allocator);
    const stmt = parser.parse() catch |e| return .{ .err = .{ .parse = e } };
    defer parser.freeStatement(stmt);

    return executeStatement(catalog, stmt, allocator);
}

fn executeStatement(catalog: *Catalog, stmt: Statement, allocator: std.mem.Allocator) ExecuteResult {
    var planner = Planner{ .allocator = allocator, .catalog = catalog };

    switch (stmt) {
        .create_table => |crt| {
            planner.executeCreateTable(crt) catch |e| return .{ .err = .{ .execute = e } };
            return .table_created;
        },
        .create_index => |cri| {
            planner.executeCreateIndex(cri) catch |e| return .{ .err = .{ .execute = e } };
            return .index_created;
        },
        .insert => |ins| {
            planner.executeInsert(ins) catch |e| return .{ .err = .{ .execute = e } };
            return .row_inserted;
        },
        .select => |sel| {
            var exec = planner.planSelect(sel) catch |e| return .{ .err = .{ .execute = e } };
            defer planner.destroyPlan(exec);

            var rows = std.ArrayList(Tuple).empty;
            while (exec.next() catch null) |row| {
                rows.append(allocator, row) catch return .{ .err = .{ .execute = error.OutOfMemory } };
            }
            return .{ .select = .{ .rows = rows, .allocator = allocator } };
        },
    }
}

// ============ I/O Functions ============

fn printValue(val: Value, writer: *std.Io.Writer) !void {
    switch (val) {
        .integer => |v| try writer.print("{}", .{v}),
        .text => |v| try writer.print("{s}", .{v}),
        .boolean => |v| try writer.print("{}", .{v}),
        .null_value => try writer.writeAll("NULL"),
    }
}

fn printRow(row: Tuple, writer: *std.Io.Writer) !void {
    for (row.values, 0..) |val, i| {
        if (i > 0) try writer.writeAll("\t");
        try printValue(val, writer);
    }
    try writer.writeAll("\n");
}

fn printResult(result: *ExecuteResult, writer: *std.Io.Writer) !void {
    switch (result.*) {
        .table_created => try writer.writeAll("Table created\n"),
        .index_created => try writer.writeAll("Index created\n"),
        .row_inserted => try writer.writeAll("1 row inserted\n"),
        .select => |*sel| {
            defer sel.deinit();
            for (sel.rows.items) |row| {
                try printRow(row, writer);
            }
            try writer.print("{} rows\n", .{sel.rows.items.len});
        },
        .err => |e| {
            switch (e) {
                .parse => |err| try writer.print("Parse error: {}\n", .{err}),
                .execute => |err| try writer.print("Error: {}\n", .{err}),
            }
        },
    }
}

// ============ Main ============

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var stdout_writer = std.fs.File.stdout().writer(&.{});
    var stdout = &stdout_writer.interface;
    var stdin = std.fs.File.stdin();

    var stdin_buf: [4096]u8 = undefined;
    var stdin_reader = stdin.reader(&stdin_buf);
    const reader = &stdin_reader.interface;

    try stdout.writeAll("MiniDB v0.1.0\n");
    try stdout.writeAll("Type 'exit' to quit.\n\n");

    while (true) {
        try stdout.writeAll("minidb> ");

        const bare_line = reader.takeDelimiter('\n') catch |err| {
            if (err == error.EndOfStream) break;
            return err;
        } orelse continue;

        const line = std.mem.trim(u8, bare_line, " \t\r\n");

        if (line.len == 0) continue;

        if (std.mem.eql(u8, line, "exit") or std.mem.eql(u8, line, "quit")) {
            try stdout.writeAll("Bye!\n");
            break;
        }

        var result = execute(&catalog, line, allocator);
        try printResult(&result, stdout);
    }
}

// ============ Integration Tests ============

test "execute: CREATE TABLE" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const result = execute(&catalog, "CREATE TABLE users (id INT NOT NULL, name TEXT)", allocator);
    try std.testing.expect(result == .table_created);
}

test "execute: INSERT" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    _ = execute(&catalog, "CREATE TABLE users (id INT NOT NULL, name TEXT)", allocator);
    const result = execute(&catalog, "INSERT INTO users VALUES (1, 'Alice')", allocator);
    try std.testing.expect(result == .row_inserted);
}

test "execute: SELECT" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    _ = execute(&catalog, "CREATE TABLE users (id INT NOT NULL, name TEXT)", allocator);
    _ = execute(&catalog, "INSERT INTO users VALUES (1, 'Alice')", allocator);
    _ = execute(&catalog, "INSERT INTO users VALUES (2, 'Bob')", allocator);

    var result = execute(&catalog, "SELECT * FROM users", allocator);
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 2), sel.rows.items.len);
    try std.testing.expectEqual(@as(i64, 1), sel.rows.items[0].values[0].integer);
    try std.testing.expectEqual(@as(i64, 2), sel.rows.items[1].values[0].integer);
}

test "execute: SELECT with WHERE" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    _ = execute(&catalog, "CREATE TABLE nums (val INT NOT NULL)", allocator);
    _ = execute(&catalog, "INSERT INTO nums VALUES (10)", allocator);
    _ = execute(&catalog, "INSERT INTO nums VALUES (20)", allocator);
    _ = execute(&catalog, "INSERT INTO nums VALUES (30)", allocator);

    var result = execute(&catalog, "SELECT * FROM nums WHERE val > 15", allocator);
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 2), sel.rows.items.len);
    try std.testing.expectEqual(@as(i64, 20), sel.rows.items[0].values[0].integer);
    try std.testing.expectEqual(@as(i64, 30), sel.rows.items[1].values[0].integer);
}

test "execute: SELECT with text WHERE" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    _ = execute(&catalog, "CREATE TABLE users (id INT NOT NULL, name TEXT)", allocator);
    _ = execute(&catalog, "INSERT INTO users VALUES (1, 'Alice')", allocator);
    _ = execute(&catalog, "INSERT INTO users VALUES (2, 'Bob')", allocator);

    var result = execute(&catalog, "SELECT * FROM users WHERE name = 'Alice'", allocator);
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 1), sel.rows.items.len);
    try std.testing.expectEqual(@as(i64, 1), sel.rows.items[0].values[0].integer);
    try std.testing.expectEqualStrings("Alice", sel.rows.items[0].values[1].text);
}

test "execute: table not found" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const result = execute(&catalog, "SELECT * FROM nonexistent", allocator);
    try std.testing.expect(result == .err);
}

test "execute: parse error" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const result = execute(&catalog, "INVALID SQL", allocator);
    try std.testing.expect(result == .err);
    try std.testing.expect(result.err == .parse);
}

test "execute: INSERT column count mismatch" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    _ = execute(&catalog, "CREATE TABLE users (id INT NOT NULL, name TEXT)", allocator);
    const result = execute(&catalog, "INSERT INTO users VALUES (1)", allocator);
    try std.testing.expect(result == .err);
    try std.testing.expect(result.err == .execute);
}

test "execute: SELECT specific columns" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    _ = execute(&catalog, "CREATE TABLE users (id INT NOT NULL, name TEXT, age INT)", allocator);
    _ = execute(&catalog, "INSERT INTO users VALUES (1, 'Alice', 30)", allocator);

    var result = execute(&catalog, "SELECT name, age FROM users", allocator);
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 1), sel.rows.items.len);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.items[0].values.len);
    try std.testing.expectEqualStrings("Alice", sel.rows.items[0].values[0].text);
    try std.testing.expectEqual(@as(i64, 30), sel.rows.items[0].values[1].integer);
}

test "execute: INSERT and SELECT with NULL" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    _ = execute(&catalog, "CREATE TABLE users (id INT NOT NULL, name TEXT)", allocator);
    _ = execute(&catalog, "INSERT INTO users VALUES (1, NULL)", allocator);

    var result = execute(&catalog, "SELECT * FROM users", allocator);
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 1), sel.rows.items.len);
    try std.testing.expectEqual(@as(i64, 1), sel.rows.items[0].values[0].integer);
    try std.testing.expect(sel.rows.items[0].values[1] == .null_value);
}

test "execute: CREATE INDEX" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    _ = execute(&catalog, "CREATE TABLE users (id INT NOT NULL, name TEXT)", allocator);
    const result = execute(&catalog, "CREATE INDEX idx_id ON users (id)", allocator);
    try std.testing.expect(result == .index_created);

    // Verify index exists
    const table = catalog.getTable("users").?;
    try std.testing.expect(table.indexes.get("id") != null);
}

test "execute: SELECT uses index after CREATE INDEX" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    _ = execute(&catalog, "CREATE TABLE users (id INT NOT NULL, name TEXT)", allocator);
    _ = execute(&catalog, "INSERT INTO users VALUES (10, 'Alice')", allocator);
    _ = execute(&catalog, "INSERT INTO users VALUES (20, 'Bob')", allocator);
    _ = execute(&catalog, "INSERT INTO users VALUES (30, 'Charlie')", allocator);
    _ = execute(&catalog, "CREATE INDEX idx_id ON users (id)", allocator);

    var result = execute(&catalog, "SELECT * FROM users WHERE id = 20", allocator);
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 1), sel.rows.items.len);
    try std.testing.expectEqual(@as(i64, 20), sel.rows.items[0].values[0].integer);
    try std.testing.expectEqualStrings("Bob", sel.rows.items[0].values[1].text);
}
