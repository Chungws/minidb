const std = @import("std");
const Allocator = std.mem.Allocator;

const tuple = @import("../record/tuple.zig");
const Tuple = tuple.Tuple;
const Schema = tuple.Schema;
const heap = @import("../record/heap.zig");
const HeapFile = heap.HeapFile;
const HeapIterator = heap.HeapIterator;

pub const Executor = union(enum) {
    seq_scan: SeqScan,

    pub fn next(self: *Executor) !?Tuple {
        switch (self.*) {
            .seq_scan => |*scan| return try scan.next(),
        }
    }
};

pub const SeqScan = struct {
    heap: *const HeapFile,
    schema: Schema,
    iterator: HeapIterator,
    allocator: Allocator,

    pub fn init(heap_file: *const HeapFile, schema: Schema, allocator: Allocator) SeqScan {
        return SeqScan{
            .heap = heap_file,
            .schema = schema,
            .iterator = heap_file.scan(),
            .allocator = allocator,
        };
    }

    pub fn next(self: *SeqScan) !?Tuple {
        const result = self.iterator.next();
        if (result) |res| {
            return try Tuple.deserialize(res.data, self.schema, self.allocator);
        }
        return null;
    }

    pub fn reset(self: *SeqScan) void {
        self.iterator = self.heap.scan();
    }
};

// ============ Tests ============

test "seq_scan empty table returns null" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]tuple.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    var scan = SeqScan.init(&heap_file, schema, allocator);
    const result = try scan.next();

    try std.testing.expect(result == null);
}

test "seq_scan returns all tuples" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]tuple.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    // Insert 3 tuples
    const t1 = Tuple{ .values = &[_]tuple.Value{.{ .integer = 10 }} };
    const t2 = Tuple{ .values = &[_]tuple.Value{.{ .integer = 20 }} };
    const t3 = Tuple{ .values = &[_]tuple.Value{.{ .integer = 30 }} };
    _ = try heap_file.insert(&t1);
    _ = try heap_file.insert(&t2);
    _ = try heap_file.insert(&t3);

    var scan = SeqScan.init(&heap_file, schema, allocator);

    // Should return 3 tuples
    var result1 = (try scan.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 10), result1.values[0].integer);

    var result2 = (try scan.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 20), result2.values[0].integer);

    var result3 = (try scan.next()).?;
    defer result3.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 30), result3.values[0].integer);

    // 4th call returns null
    try std.testing.expect((try scan.next()) == null);
}

test "seq_scan reset restarts scan" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]tuple.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    const t1 = Tuple{ .values = &[_]tuple.Value{.{ .integer = 100 }} };
    _ = try heap_file.insert(&t1);

    var scan = SeqScan.init(&heap_file, schema, allocator);

    // First scan
    var result1 = (try scan.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 100), result1.values[0].integer);
    try std.testing.expect((try scan.next()) == null);

    // Reset and scan again
    scan.reset();

    var result2 = (try scan.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 100), result2.values[0].integer);
}

test "executor with seq_scan" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]tuple.ColumnDef{
            .{ .name = "value", .data_type = .integer, .nullable = false },
        },
    };

    const t = Tuple{ .values = &[_]tuple.Value{.{ .integer = 42 }} };
    _ = try heap_file.insert(&t);

    var executor = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    var result = (try executor.next()).?;
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 42), result.values[0].integer);

    try std.testing.expect((try executor.next()) == null);
}
