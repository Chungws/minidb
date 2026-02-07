const std = @import("std");
const Allocator = std.mem.Allocator;

const Catalog = @import("../query/catalog.zig").Catalog;
const Parser = @import("../sql/parser.zig").Parser;
const Planner = @import("../query/planner.zig").Planner;
const Executor = @import("../query/executor.zig").Executor;
const Value = @import("../sql/ast.zig").Value;
const Statement = @import("../sql/ast.zig").Statement;
const Tuple = @import("../record/tuple.zig").Tuple;
const TransactionManager = @import("transaction.zig").TransactionManager;
const WAL = @import("wal.zig").WAL;
const LogRecord = @import("wal.zig").LogRecord;

pub const SessionError = error{
    TransactionAlreadyExist,
    TransactionNotExist,
};

pub const ExecuteError = union(enum) {
    parse: anyerror,
    execute: anyerror,
};

pub const SelectResult = struct {
    rows: std.ArrayList(Tuple),
    allocator: Allocator,

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
    wal: WAL,
    current_txn: ?u64,

    pub fn init(catalog: *Catalog, allocator: Allocator) Session {
        return .{
            .catalog = catalog,
            .allocator = allocator,
            .txn_mgr = TransactionManager.init(allocator),
            .wal = WAL.init(allocator),
            .current_txn = null,
        };
    }

    pub fn deinit(self: *Session) void {
        self.wal.deinit();
        self.txn_mgr.deinit();
    }

    pub fn currentTxnId(self: *const Session) ?u64 {
        return self.current_txn;
    }

    pub fn getWAL(self: *const Session) *const WAL {
        return &self.wal;
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
                if (self.current_txn) |txn_id| {
                    self.wal.append(.{ .insert = .{
                        .txn_id = txn_id,
                        .table_name = ins.table_name,
                        .values = ins.values,
                    } }) catch |e| return .{ .err = .{ .execute = e } };
                }
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
                self.wal.append(.{ .begin = tx.id }) catch |e| return .{ .err = .{ .execute = e } };
                return .transaction_started;
            },
            .commit => {
                if (self.current_txn == null) {
                    return .{ .err = .{ .execute = error.TransactionNotExist } };
                }
                const txn_id = self.current_txn.?;
                self.txn_mgr.commit(txn_id) catch |e| return .{ .err = .{ .execute = e } };
                self.wal.append(.{ .commit = txn_id }) catch |e| return .{ .err = .{ .execute = e } };
                self.current_txn = null;
                return .transaction_committed;
            },
            .abort => {
                if (self.current_txn == null) {
                    return .{ .err = .{ .execute = error.TransactionNotExist } };
                }
                const txn_id = self.current_txn.?;
                self.txn_mgr.abort(txn_id) catch |e| return .{ .err = .{ .execute = e } };
                self.wal.append(.{ .abort = txn_id }) catch |e| return .{ .err = .{ .execute = e } };
                self.current_txn = null;
                return .transaction_aborted;
            },
        }
    }
};

// ============ Tests ============

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

// ============ WAL Integration Tests ============

test "session: BEGIN logs to WAL" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("BEGIN");

    const records = session.getWAL().getRecords();
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expect(records[0] == .begin);
    try std.testing.expectEqual(@as(u64, 1), records[0].begin);
}

test "session: INSERT inside transaction logs to WAL" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)");
    _ = session.execute("BEGIN");
    _ = session.execute("INSERT INTO users VALUES (1, 'Alice')");

    const records = session.getWAL().getRecords();
    try std.testing.expectEqual(@as(usize, 2), records.len);
    try std.testing.expect(records[0] == .begin);
    try std.testing.expect(records[1] == .insert);

    const ins = records[1].insert;
    try std.testing.expectEqual(@as(u64, 1), ins.txn_id);
    try std.testing.expectEqualStrings("users", ins.table_name);
}

test "session: COMMIT logs to WAL" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL)");
    _ = session.execute("BEGIN");
    _ = session.execute("INSERT INTO users VALUES (1)");
    _ = session.execute("COMMIT");

    const records = session.getWAL().getRecords();
    try std.testing.expectEqual(@as(usize, 3), records.len);
    try std.testing.expect(records[0] == .begin);
    try std.testing.expect(records[1] == .insert);
    try std.testing.expect(records[2] == .commit);
    try std.testing.expectEqual(@as(u64, 1), records[2].commit);
}

test "session: ABORT logs to WAL" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL)");
    _ = session.execute("BEGIN");
    _ = session.execute("INSERT INTO users VALUES (1)");
    _ = session.execute("ABORT");

    const records = session.getWAL().getRecords();
    try std.testing.expectEqual(@as(usize, 3), records.len);
    try std.testing.expect(records[0] == .begin);
    try std.testing.expect(records[1] == .insert);
    try std.testing.expect(records[2] == .abort);
    try std.testing.expectEqual(@as(u64, 1), records[2].abort);
}

test "session: INSERT outside transaction does not log to WAL" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    _ = session.execute("CREATE TABLE users (id INT NOT NULL)");
    _ = session.execute("INSERT INTO users VALUES (1)");

    const records = session.getWAL().getRecords();
    try std.testing.expectEqual(@as(usize, 0), records.len);
}
