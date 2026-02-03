const std = @import("std");
const Allocator = std.mem.Allocator;
const Value = @import("../sql/ast.zig").Value;

pub const LogRecord = union(enum) {
    begin: u64,
    insert: InsertRecord,
    commit: u64,
    abort: u64,
};

pub const InsertRecord = struct {
    txn_id: u64,
    table_name: []const u8,
    values: []const Value,
};

pub const WAL = struct {
    records: std.ArrayList(LogRecord),
    allocator: Allocator,

    pub fn init(allocator: Allocator) WAL {
        return WAL{
            .records = std.ArrayList(LogRecord).empty,
            .allocator = allocator,
        };
    }

    pub fn append(self: *WAL, record: LogRecord) !void {
        try self.records.append(self.allocator, record);
    }

    pub fn getRecords(self: *const WAL) []const LogRecord {
        return self.records.items;
    }

    pub fn deinit(self: *WAL) void {
        self.records.deinit(self.allocator);
    }
};

// ============ Tests ============

test "append and retrieve BEGIN record" {
    const allocator = std.testing.allocator;
    var wal = WAL.init(allocator);
    defer wal.deinit();

    try wal.append(.{ .begin = 1 });

    const records = wal.getRecords();
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(@as(u64, 1), records[0].begin);
}

test "append and retrieve INSERT record" {
    const allocator = std.testing.allocator;
    var wal = WAL.init(allocator);
    defer wal.deinit();

    try wal.append(.{ .insert = .{
        .txn_id = 1,
        .table_name = "users",
        .values = &[_]Value{ .{ .integer = 1 }, .{ .text = "Alice" } },
    } });

    const records = wal.getRecords();
    try std.testing.expectEqual(@as(usize, 1), records.len);

    const ins = records[0].insert;
    try std.testing.expectEqual(@as(u64, 1), ins.txn_id);
    try std.testing.expectEqualStrings("users", ins.table_name);
    try std.testing.expectEqual(@as(usize, 2), ins.values.len);
    try std.testing.expectEqual(@as(i64, 1), ins.values[0].integer);
    try std.testing.expectEqualStrings("Alice", ins.values[1].text);
}

test "append COMMIT and ABORT records" {
    const allocator = std.testing.allocator;
    var wal = WAL.init(allocator);
    defer wal.deinit();

    try wal.append(.{ .commit = 1 });
    try wal.append(.{ .abort = 2 });

    const records = wal.getRecords();
    try std.testing.expectEqual(@as(usize, 2), records.len);
    try std.testing.expectEqual(@as(u64, 1), records[0].commit);
    try std.testing.expectEqual(@as(u64, 2), records[1].abort);
}

test "records maintain append order" {
    const allocator = std.testing.allocator;
    var wal = WAL.init(allocator);
    defer wal.deinit();

    try wal.append(.{ .begin = 1 });
    try wal.append(.{ .insert = .{
        .txn_id = 1,
        .table_name = "users",
        .values = &[_]Value{.{ .integer = 1 }},
    } });
    try wal.append(.{ .commit = 1 });

    const records = wal.getRecords();
    try std.testing.expectEqual(@as(usize, 3), records.len);
    try std.testing.expect(records[0] == .begin);
    try std.testing.expect(records[1] == .insert);
    try std.testing.expect(records[2] == .commit);
}

test "interleaved transactions maintain global order" {
    const allocator = std.testing.allocator;
    var wal = WAL.init(allocator);
    defer wal.deinit();

    // txn 1: begin → insert
    // txn 2: begin → insert
    // txn 1: commit
    // txn 2: abort
    try wal.append(.{ .begin = 1 });
    try wal.append(.{ .begin = 2 });
    try wal.append(.{ .insert = .{
        .txn_id = 1,
        .table_name = "users",
        .values = &[_]Value{.{ .integer = 10 }},
    } });
    try wal.append(.{ .insert = .{
        .txn_id = 2,
        .table_name = "orders",
        .values = &[_]Value{.{ .integer = 20 }},
    } });
    try wal.append(.{ .commit = 1 });
    try wal.append(.{ .abort = 2 });

    const records = wal.getRecords();
    try std.testing.expectEqual(@as(usize, 6), records.len);

    // txn 1 records: begin(0), insert(2), commit(4)
    try std.testing.expectEqual(@as(u64, 1), records[0].begin);
    try std.testing.expectEqual(@as(u64, 1), records[2].insert.txn_id);
    try std.testing.expectEqual(@as(u64, 1), records[4].commit);

    // txn 2 records: begin(1), insert(3), abort(5)
    try std.testing.expectEqual(@as(u64, 2), records[1].begin);
    try std.testing.expectEqual(@as(u64, 2), records[3].insert.txn_id);
    try std.testing.expectEqual(@as(u64, 2), records[5].abort);
}

test "empty WAL returns empty records" {
    const allocator = std.testing.allocator;
    var wal = WAL.init(allocator);
    defer wal.deinit();

    const records = wal.getRecords();
    try std.testing.expectEqual(@as(usize, 0), records.len);
}
