const std = @import("std");
const Allocator = std.mem.Allocator;

const Catalog = @import("query/catalog.zig").Catalog;
const Parser = @import("sql/parser.zig").Parser;
const Planner = @import("query/planner.zig").Planner;
const Executor = @import("query/executor.zig").Executor;
const Value = @import("sql/ast.zig").Value;
const Statement = @import("sql/ast.zig").Statement;
const Tuple = @import("record/tuple.zig").Tuple;
const TransactionManager = @import("tx/transaction.zig").TransactionManager;

pub const ExecutionError = error{
    TransactionAlreadyExist,
    TransactionNotExist,
};

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
    transaction_started,
    transaction_committed,
    transaction_aborted,
    select: SelectResult,
    err: ExecuteError,
};

pub const Session = struct {
    catalog: *Catalog,
    allocator: Allocator,
    txn_mgr: TransactionManager,
    current_txn: ?u64,

    pub fn init(catalog: *Catalog, allocator: Allocator) Session {
        return .{
            .catalog = catalog,
            .allocator = allocator,
            .txn_mgr = TransactionManager.init(allocator),
            .current_txn = null,
        };
    }

    pub fn deinit(self: *Session) void {
        self.txn_mgr.deinit();
    }

    pub fn currentTxnId(self: *const Session) ?u64 {
        return self.current_txn;
    }

    pub fn execute(self: *Session, sql: []const u8) ExecuteResult {
        var parser = Parser.init(sql, self.allocator);
        const stmt = parser.parse() catch |e| return .{ .err = .{ .parse = e } };
        defer parser.freeStatement(stmt);

        return self.executeStatement(stmt);
    }

    fn executeStatement(self: *Session, stmt: Statement) ExecuteResult {
        var planner = Planner{
            .allocator = self.allocator,
            .catalog = self.catalog,
        };

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
                    rows.append(self.allocator, row) catch return .{ .err = .{ .execute = error.OutOfMemory } };
                }
                return .{ .select = .{ .rows = rows, .allocator = self.allocator } };
            },
            .begin => {
                if (self.current_txn != null) {
                    return .{ .err = .{ .execute = error.TransactionAlreadyExist } };
                }
                const tx = self.txn_mgr.begin() catch |e| return .{ .err = .{ .execute = e } };
                self.current_txn = tx.id;
                return .transaction_started;
            },
            .commit => {
                if (self.current_txn == null) {
                    return .{ .err = .{ .execute = error.TransactionNotExist } };
                }
                self.txn_mgr.commit(self.current_txn.?) catch |e| return .{ .err = .{ .execute = e } };
                self.current_txn = null;
                return .transaction_committed;
            },
            .abort => {
                if (self.current_txn == null) {
                    return .{ .err = .{ .execute = error.TransactionNotExist } };
                }
                self.txn_mgr.abort(self.current_txn.?) catch |e| return .{ .err = .{ .execute = e } };
                self.current_txn = null;
                return .transaction_aborted;
            },
        }
    }
};

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
        .transaction_started => try writer.writeAll("Transaction started\n"),
        .transaction_committed => try writer.writeAll("Transaction committed\n"),
        .transaction_aborted => try writer.writeAll("Transaction aborted\n"),
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

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

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

        var result = session.execute(line);
        try printResult(&result, stdout);
    }
}

// ============ Integration Tests ============

test "session: CREATE TABLE" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    const result = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    try std.testing.expect(result == .table_created);
}

test "session: INSERT" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    const result = session.execute("INSERT INTO users VALUES (1, 'Alice')");
    try std.testing.expect(result == .row_inserted);
}

test "session: SELECT" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    _ = session.execute("INSERT INTO users VALUES (1, 'Alice')");
    _ = session.execute("INSERT INTO users VALUES (2, 'Bob')");

    var result = session.execute("SELECT * FROM users");
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 2), sel.rows.items.len);
    try std.testing.expectEqual(@as(i64, 1), sel.rows.items[0].values[0].integer);
    try std.testing.expectEqual(@as(i64, 2), sel.rows.items[1].values[0].integer);
}

test "session: SELECT with WHERE" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE nums (val INT NOT NULL)");
    _ = session.execute("INSERT INTO nums VALUES (10)");
    _ = session.execute("INSERT INTO nums VALUES (20)");
    _ = session.execute("INSERT INTO nums VALUES (30)");

    var result = session.execute("SELECT * FROM nums WHERE val > 15");
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 2), sel.rows.items.len);
    try std.testing.expectEqual(@as(i64, 20), sel.rows.items[0].values[0].integer);
    try std.testing.expectEqual(@as(i64, 30), sel.rows.items[1].values[0].integer);
}

test "session: SELECT with text WHERE" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    _ = session.execute("INSERT INTO users VALUES (1, 'Alice')");
    _ = session.execute("INSERT INTO users VALUES (2, 'Bob')");

    var result = session.execute("SELECT * FROM users WHERE name = 'Alice'");
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 1), sel.rows.items.len);
    try std.testing.expectEqual(@as(i64, 1), sel.rows.items[0].values[0].integer);
    try std.testing.expectEqualStrings("Alice", sel.rows.items[0].values[1].text);
}

test "session: table not found" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    const result = session.execute("SELECT * FROM nonexistent");
    try std.testing.expect(result == .err);
}

test "session: parse error" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    const result = session.execute("INVALID SQL");
    try std.testing.expect(result == .err);
    try std.testing.expect(result.err == .parse);
}

test "session: INSERT column count mismatch" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    const result = session.execute("INSERT INTO users VALUES (1)");
    try std.testing.expect(result == .err);
    try std.testing.expect(result.err == .execute);
}

test "session: SELECT specific columns" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT, age INT)");
    _ = session.execute("INSERT INTO users VALUES (1, 'Alice', 30)");

    var result = session.execute("SELECT name, age FROM users");
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 1), sel.rows.items.len);
    try std.testing.expectEqual(@as(usize, 2), sel.rows.items[0].values.len);
    try std.testing.expectEqualStrings("Alice", sel.rows.items[0].values[0].text);
    try std.testing.expectEqual(@as(i64, 30), sel.rows.items[0].values[1].integer);
}

test "session: INSERT and SELECT with NULL" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    _ = session.execute("INSERT INTO users VALUES (1, NULL)");

    var result = session.execute("SELECT * FROM users");
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 1), sel.rows.items.len);
    try std.testing.expectEqual(@as(i64, 1), sel.rows.items[0].values[0].integer);
    try std.testing.expect(sel.rows.items[0].values[1] == .null_value);
}

test "session: CREATE INDEX" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    const result = session.execute("CREATE INDEX idx_id ON users (id)");
    try std.testing.expect(result == .index_created);

    // Verify index exists
    const table = catalog.getTable("users").?;
    try std.testing.expect(table.indexes.get("id") != null);
}

test "session: SELECT with JOIN" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    _ = session.execute("CREATE TABLE orders (order_id INT NOT NULL, user_id INT NOT NULL)");
    _ = session.execute("INSERT INTO users VALUES (1, 'Alice')");
    _ = session.execute("INSERT INTO users VALUES (2, 'Bob')");
    _ = session.execute("INSERT INTO orders VALUES (100, 1)");
    _ = session.execute("INSERT INTO orders VALUES (101, 2)");
    _ = session.execute("INSERT INTO orders VALUES (102, 1)");

    var result = session.execute("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    // Alice has 2 orders, Bob has 1 order = 3 rows total
    try std.testing.expectEqual(@as(usize, 3), sel.rows.items.len);
}

test "session: SELECT with JOIN no matches" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    _ = session.execute("CREATE TABLE orders (order_id INT NOT NULL, user_id INT NOT NULL)");
    _ = session.execute("INSERT INTO users VALUES (1, 'Alice')");
    _ = session.execute("INSERT INTO orders VALUES (100, 999)");

    var result = session.execute("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 0), sel.rows.items.len);
}

test "session: SELECT columns with JOIN" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    _ = session.execute("CREATE TABLE orders (order_id INT NOT NULL, user_id INT NOT NULL)");
    _ = session.execute("INSERT INTO users VALUES (1, 'Alice')");
    _ = session.execute("INSERT INTO orders VALUES (100, 1)");

    var result = session.execute("SELECT name, order_id FROM users JOIN orders ON users.id = orders.user_id");
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 1), sel.rows.items.len);
    // Should have only 2 projected columns
    try std.testing.expectEqual(@as(usize, 2), sel.rows.items[0].values.len);
    try std.testing.expectEqualStrings("Alice", sel.rows.items[0].values[0].text);
    try std.testing.expectEqual(@as(i64, 100), sel.rows.items[0].values[1].integer);
}

test "session: SELECT with JOIN right table not found" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");

    const result = session.execute("SELECT * FROM users JOIN nonexistent ON users.id = nonexistent.uid");
    try std.testing.expect(result == .err);
    try std.testing.expect(result.err == .execute);
}

test "session: SELECT uses index after CREATE INDEX" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    _ = session.execute("INSERT INTO users VALUES (10, 'Alice')");
    _ = session.execute("INSERT INTO users VALUES (20, 'Bob')");
    _ = session.execute("INSERT INTO users VALUES (30, 'Charlie')");
    _ = session.execute("CREATE INDEX idx_id ON users (id)");

    var result = session.execute("SELECT * FROM users WHERE id = 20");
    try std.testing.expect(result == .select);

    var sel = &result.select;
    defer sel.deinit();

    try std.testing.expectEqual(@as(usize, 1), sel.rows.items.len);
    try std.testing.expectEqual(@as(i64, 20), sel.rows.items[0].values[0].integer);
    try std.testing.expectEqualStrings("Bob", sel.rows.items[0].values[1].text);
}

// ============ Transaction Tests ============

test "session: BEGIN starts transaction" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    const result = session.execute("BEGIN");
    try std.testing.expect(result == .transaction_started);
    try std.testing.expect(session.currentTxnId() != null);
}

test "session: COMMIT ends transaction" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("BEGIN");
    const result = session.execute("COMMIT");
    try std.testing.expect(result == .transaction_committed);
    try std.testing.expect(session.currentTxnId() == null);
}

test "session: ABORT ends transaction" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("BEGIN");
    const result = session.execute("ABORT");
    try std.testing.expect(result == .transaction_aborted);
    try std.testing.expect(session.currentTxnId() == null);
}

test "session: COMMIT without BEGIN returns error" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    const result = session.execute("COMMIT");
    try std.testing.expect(result == .err);
}

test "session: ABORT without BEGIN returns error" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    const result = session.execute("ABORT");
    try std.testing.expect(result == .err);
}

test "session: BEGIN twice returns error" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("BEGIN");
    const result = session.execute("BEGIN");
    try std.testing.expect(result == .err);
}

test "session: regular queries work without transaction" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    const result1 = session.execute("CREATE TABLE users (id INT NOT NULL)");
    try std.testing.expect(result1 == .table_created);

    const result2 = session.execute("INSERT INTO users VALUES (1)");
    try std.testing.expect(result2 == .row_inserted);

    var result3 = session.execute("SELECT * FROM users");
    try std.testing.expect(result3 == .select);
    result3.select.deinit();
}

test "session: queries work inside transaction" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL)");
    _ = session.execute("BEGIN");
    const result = session.execute("INSERT INTO users VALUES (1)");
    try std.testing.expect(result == .row_inserted);
    _ = session.execute("COMMIT");
}
