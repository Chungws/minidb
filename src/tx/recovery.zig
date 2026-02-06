const std = @import("std");
const Allocator = std.mem.Allocator;

const WAL = @import("wal.zig").WAL;
const LogRecord = @import("wal.zig").LogRecord;
const Catalog = @import("../query/catalog.zig").Catalog;
const Tuple = @import("../record/tuple.zig").Tuple;
const Schema = @import("../record/tuple.zig").Schema;
const Value = @import("../sql/ast.zig").Value;
const ColumnDef = @import("../sql/ast.zig").ColumnDef;

const executor = @import("../query/executor.zig");
const SeqScan = executor.SeqScan;
const Executor = executor.Executor;

pub fn recover(wal: *const WAL, catalog: *Catalog, allocator: Allocator) !void {
    const records = wal.getRecords();
    var committed = std.AutoHashMap(u64, void).init(allocator);
    defer committed.deinit();
    for (records) |record| {
        if (record == .commit) {
            try committed.put(record.commit, {});
        }
    }

    for (records) |record| {
        if (record == .insert and committed.contains(record.insert.txn_id)) {
            if (catalog.getTable(record.insert.table_name)) |table| {
                const tuple = &Tuple{
                    .values = record.insert.values,
                    .schema = table.schema,
                };
                _ = try table.insert(tuple);
            }
        }
    }
}

// ============ Tests ============

test "recover committed transaction inserts data" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
        },
    };
    try catalog.createTable("users", schema);

    var wal = WAL.init(allocator);
    defer wal.deinit();

    try wal.append(.{ .begin = 1 });
    try wal.append(.{ .insert = .{
        .txn_id = 1,
        .table_name = "users",
        .values = &[_]Value{ .{ .integer = 1 }, .{ .text = "Alice" } },
    } });
    try wal.append(.{ .commit = 1 });

    try recover(&wal, &catalog, allocator);

    // Verify: table should have 1 row
    const table = catalog.getTable("users").?;
    var exec = Executor{ .seq_scan = SeqScan.init(&table.heap_file, table.schema, allocator) };
    var result = (try exec.next()).?;
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), result.values[0].integer);
    try std.testing.expectEqualStrings("Alice", result.values[1].text);
    try std.testing.expect((try exec.next()) == null);
}

test "recover aborted transaction skips inserts" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    try catalog.createTable("users", schema);

    var wal = WAL.init(allocator);
    defer wal.deinit();

    try wal.append(.{ .begin = 1 });
    try wal.append(.{ .insert = .{
        .txn_id = 1,
        .table_name = "users",
        .values = &[_]Value{.{ .integer = 1 }},
    } });
    try wal.append(.{ .abort = 1 });

    try recover(&wal, &catalog, allocator);

    // Verify: table should be empty
    const table = catalog.getTable("users").?;
    var exec = Executor{ .seq_scan = SeqScan.init(&table.heap_file, table.schema, allocator) };
    try std.testing.expect((try exec.next()) == null);
}

test "recover mixed: only committed transactions applied" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    try catalog.createTable("users", schema);

    var wal = WAL.init(allocator);
    defer wal.deinit();

    // txn 1: committed
    try wal.append(.{ .begin = 1 });
    try wal.append(.{ .insert = .{
        .txn_id = 1,
        .table_name = "users",
        .values = &[_]Value{.{ .integer = 10 }},
    } });
    try wal.append(.{ .commit = 1 });

    // txn 2: aborted
    try wal.append(.{ .begin = 2 });
    try wal.append(.{ .insert = .{
        .txn_id = 2,
        .table_name = "users",
        .values = &[_]Value{.{ .integer = 20 }},
    } });
    try wal.append(.{ .abort = 2 });

    // txn 3: committed
    try wal.append(.{ .begin = 3 });
    try wal.append(.{ .insert = .{
        .txn_id = 3,
        .table_name = "users",
        .values = &[_]Value{.{ .integer = 30 }},
    } });
    try wal.append(.{ .commit = 3 });

    try recover(&wal, &catalog, allocator);

    // Verify: only txn 1 and 3 applied (id=10, id=30)
    const table = catalog.getTable("users").?;
    var exec = Executor{ .seq_scan = SeqScan.init(&table.heap_file, table.schema, allocator) };

    var row1 = (try exec.next()).?;
    defer row1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 10), row1.values[0].integer);

    var row2 = (try exec.next()).?;
    defer row2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 30), row2.values[0].integer);

    try std.testing.expect((try exec.next()) == null);
}

test "recover empty WAL does nothing" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    try catalog.createTable("users", schema);

    var wal = WAL.init(allocator);
    defer wal.deinit();

    try recover(&wal, &catalog, allocator);

    // Verify: table should be empty
    const table = catalog.getTable("users").?;
    var exec = Executor{ .seq_scan = SeqScan.init(&table.heap_file, table.schema, allocator) };
    try std.testing.expect((try exec.next()) == null);
}

test "recover uncommitted transaction skips inserts" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    try catalog.createTable("users", schema);

    var wal = WAL.init(allocator);
    defer wal.deinit();

    // BEGIN + INSERT but no COMMIT or ABORT
    try wal.append(.{ .begin = 1 });
    try wal.append(.{ .insert = .{
        .txn_id = 1,
        .table_name = "users",
        .values = &[_]Value{.{ .integer = 1 }},
    } });

    try recover(&wal, &catalog, allocator);

    // Verify: table should be empty (uncommitted)
    const table = catalog.getTable("users").?;
    var exec = Executor{ .seq_scan = SeqScan.init(&table.heap_file, table.schema, allocator) };
    try std.testing.expect((try exec.next()) == null);
}
