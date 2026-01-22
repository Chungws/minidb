const std = @import("std");
const Allocator = std.mem.Allocator;

const tuple = @import("../record/tuple.zig");
const Tuple = tuple.Tuple;
const Schema = tuple.Schema;
const heap = @import("../record/heap.zig");
const HeapFile = heap.HeapFile;
const HeapIterator = heap.HeapIterator;
const RID = heap.RID;
const ast = @import("../sql/ast.zig");
const BTree = @import("../index/btree.zig").BTree;

pub const Executor = union(enum) {
    seq_scan: SeqScan,
    filter: Filter,
    project: Project,
    index_scan: IndexScan,

    pub fn next(self: *Executor) anyerror!?Tuple {
        switch (self.*) {
            .seq_scan => |*scan| return try scan.next(),
            .filter => |*ft| return try ft.next(),
            .project => |*pj| return try pj.next(),
            .index_scan => |*is| return try is.next(),
        }
    }

    pub fn deinit(self: *Executor) void {
        switch (self.*) {
            .filter => |*f| {
                f.deinit();
            },
            .project => |*p| {
                p.deinit();
            },
            .index_scan => |*i| {
                i.deinit();
            },
            .seq_scan => {},
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

    pub fn next(self: *SeqScan) anyerror!?Tuple {
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

pub const Filter = struct {
    child: *Executor,
    condition: ast.Condition,
    schema: Schema,
    allocator: Allocator,

    pub fn next(self: *Filter) anyerror!?Tuple {
        while (try self.child.next()) |t| {
            var tu = t;
            if (self.evaluate(self.condition, t)) {
                return t;
            }
            tu.deinit(self.allocator);
        }
        return null;
    }

    pub fn evaluate(self: *Filter, condition: ast.Condition, t: Tuple) bool {
        switch (condition) {
            .simple => |cond| {
                const val = if (self.schema.findColumnIndex(cond.column)) |idx| t.values[idx] else return false;
                return val.compareValue(cond.value, cond.op);
            },
            .and_op => |both| {
                return self.evaluate(both.left.*, t) and self.evaluate(both.right.*, t);
            },
            .or_op => |both| {
                return self.evaluate(both.left.*, t) or self.evaluate(both.right.*, t);
            },
            .not_op => |not| {
                return !self.evaluate(not.*, t);
            },
        }
    }

    pub fn deinit(self: *Filter) void {
        self.child.deinit();
    }
};

pub const Project = struct {
    child: *Executor,
    column_indices: []const usize,
    allocator: Allocator,

    pub fn next(self: *Project) anyerror!?Tuple {
        if (try self.child.next()) |t| {
            var tu = t;
            defer tu.deinit(self.allocator);
            var new_values = try self.allocator.alloc(ast.Value, self.column_indices.len);

            for (self.column_indices, 0..) |c_i, i| {
                const val = t.values[c_i];
                new_values[i] = switch (val) {
                    .text => |s| .{ .text = try self.allocator.dupe(u8, s) },
                    else => val,
                };
            }

            return Tuple{
                .values = new_values,
            };
        }
        return null;
    }

    pub fn deinit(self: *Project) void {
        self.child.deinit();
    }
};

pub const IndexScan = struct {
    btree: *const BTree,
    heap_file: *const HeapFile,
    schema: Schema,

    search_key: ?i64,
    range_start: ?i64,
    range_end: ?i64,

    rids: ?std.ArrayList(RID),
    current_idx: usize,
    allocator: Allocator,

    pub fn init(btree: *const BTree, heap_file: *const HeapFile, schema: Schema, condition: ast.SimpleCondition, allocator: Allocator) IndexScan {
        var index = IndexScan{
            .btree = btree,
            .heap_file = heap_file,
            .schema = schema,
            .search_key = null,
            .range_start = null,
            .range_end = null,
            .rids = null,
            .current_idx = 0,
            .allocator = allocator,
        };

        const val = condition.value.integer;
        switch (condition.op) {
            .eq => {
                index.search_key = val;
            },
            .gte => {
                index.range_start = val;
            },
            .gt => {
                index.range_start = val + 1;
            },
            .lte => {
                index.range_end = val;
            },
            .lt => {
                index.range_end = val - 1;
            },
            else => {},
        }

        return index;
    }

    pub fn available(condition: ast.SimpleCondition) bool {
        return condition.op != .neq;
    }

    pub fn next(self: *IndexScan) !?Tuple {
        if (self.rids == null) {
            if (self.search_key) |key| {
                if (try self.btree.search(key)) |rid| {
                    self.rids = std.ArrayList(RID).empty;
                    try self.rids.?.append(self.allocator, rid);
                }
            } else {
                self.rids = try self.btree.rangeScan(
                    self.range_start orelse std.math.minInt(i64),
                    self.range_end orelse std.math.maxInt(i64),
                );
            }
        }

        if (self.rids) |rids| {
            if (self.current_idx >= rids.items.len) return null;
            const rid = rids.items[self.current_idx];
            self.current_idx += 1;
            if (self.heap_file.get(rid)) |data| {
                return try Tuple.deserialize(data, self.schema, self.allocator);
            }
        }

        return null;
    }

    pub fn deinit(self: *IndexScan) void {
        if (self.rids) |*rids| {
            var r = rids;
            r.deinit(self.allocator);
        }
    }
};

// ============ Tests ============

test "seq_scan empty table returns null" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
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
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    // Insert 3 tuples
    const t1 = Tuple{ .values = &[_]ast.Value{.{ .integer = 10 }} };
    const t2 = Tuple{ .values = &[_]ast.Value{.{ .integer = 20 }} };
    const t3 = Tuple{ .values = &[_]ast.Value{.{ .integer = 30 }} };
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
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    const t1 = Tuple{ .values = &[_]ast.Value{.{ .integer = 100 }} };
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
        .columns = &[_]ast.ColumnDef{
            .{ .name = "value", .data_type = .integer, .nullable = false },
        },
    };

    const t = Tuple{ .values = &[_]ast.Value{.{ .integer = 42 }} };
    _ = try heap_file.insert(&t);

    var executor = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    var result = (try executor.next()).?;
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 42), result.values[0].integer);

    try std.testing.expect((try executor.next()) == null);
}

test "filter passes matching tuples" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "age", .data_type = .integer, .nullable = false },
        },
    };

    // Insert tuples: 10, 20, 30
    const t1 = Tuple{ .values = &[_]ast.Value{.{ .integer = 10 }} };
    const t2 = Tuple{ .values = &[_]ast.Value{.{ .integer = 20 }} };
    const t3 = Tuple{ .values = &[_]ast.Value{.{ .integer = 30 }} };
    _ = try heap_file.insert(&t1);
    _ = try heap_file.insert(&t2);
    _ = try heap_file.insert(&t3);

    var child = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    // Filter: age > 15
    var filter = Filter{
        .child = &child,
        .condition = .{ .simple = .{
            .column = "age",
            .op = .gt,
            .value = .{ .integer = 15 },
        } },
        .schema = schema,
        .allocator = allocator,
    };

    // Should return 20 and 30, skip 10
    var result1 = (try filter.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 20), result1.values[0].integer);

    var result2 = (try filter.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 30), result2.values[0].integer);

    try std.testing.expect((try filter.next()) == null);
}

test "filter with no matches returns null" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "age", .data_type = .integer, .nullable = false },
        },
    };

    const t1 = Tuple{ .values = &[_]ast.Value{.{ .integer = 10 }} };
    _ = try heap_file.insert(&t1);

    var child = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    // Filter: age > 100 (no match)
    var filter = Filter{
        .child = &child,
        .condition = .{ .simple = .{
            .column = "age",
            .op = .gt,
            .value = .{ .integer = 100 },
        } },
        .schema = schema,
        .allocator = allocator,
    };

    try std.testing.expect((try filter.next()) == null);
}

test "filter with eq operator" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    const t1 = Tuple{ .values = &[_]ast.Value{.{ .integer = 1 }} };
    const t2 = Tuple{ .values = &[_]ast.Value{.{ .integer = 2 }} };
    const t3 = Tuple{ .values = &[_]ast.Value{.{ .integer = 3 }} };
    _ = try heap_file.insert(&t1);
    _ = try heap_file.insert(&t2);
    _ = try heap_file.insert(&t3);

    var child = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    // Filter: id = 2
    var filter = Filter{
        .child = &child,
        .condition = .{ .simple = .{
            .column = "id",
            .op = .eq,
            .value = .{ .integer = 2 },
        } },
        .schema = schema,
        .allocator = allocator,
    };

    var result = (try filter.next()).?;
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 2), result.values[0].integer);

    try std.testing.expect((try filter.next()) == null);
}

test "filter with and condition" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "age", .data_type = .integer, .nullable = false },
        },
    };

    // Insert tuples: 10, 20, 30, 40
    const t1 = Tuple{ .values = &[_]ast.Value{.{ .integer = 10 }} };
    const t2 = Tuple{ .values = &[_]ast.Value{.{ .integer = 20 }} };
    const t3 = Tuple{ .values = &[_]ast.Value{.{ .integer = 30 }} };
    const t4 = Tuple{ .values = &[_]ast.Value{.{ .integer = 40 }} };
    _ = try heap_file.insert(&t1);
    _ = try heap_file.insert(&t2);
    _ = try heap_file.insert(&t3);
    _ = try heap_file.insert(&t4);

    var child = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    // Filter: age > 15 AND age < 35
    var left_cond = ast.Condition{ .simple = .{ .column = "age", .op = .gt, .value = .{ .integer = 15 } } };
    var right_cond = ast.Condition{ .simple = .{ .column = "age", .op = .lt, .value = .{ .integer = 35 } } };

    var filter = Filter{
        .child = &child,
        .condition = .{ .and_op = .{ .left = &left_cond, .right = &right_cond } },
        .schema = schema,
        .allocator = allocator,
    };

    // Should return 20 and 30 only
    var result1 = (try filter.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 20), result1.values[0].integer);

    var result2 = (try filter.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 30), result2.values[0].integer);

    try std.testing.expect((try filter.next()) == null);
}

test "filter with or condition" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    // Insert tuples: 1, 2, 3
    const t1 = Tuple{ .values = &[_]ast.Value{.{ .integer = 1 }} };
    const t2 = Tuple{ .values = &[_]ast.Value{.{ .integer = 2 }} };
    const t3 = Tuple{ .values = &[_]ast.Value{.{ .integer = 3 }} };
    _ = try heap_file.insert(&t1);
    _ = try heap_file.insert(&t2);
    _ = try heap_file.insert(&t3);

    var child = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    // Filter: id = 1 OR id = 3
    var left_cond = ast.Condition{ .simple = .{ .column = "id", .op = .eq, .value = .{ .integer = 1 } } };
    var right_cond = ast.Condition{ .simple = .{ .column = "id", .op = .eq, .value = .{ .integer = 3 } } };

    var filter = Filter{
        .child = &child,
        .condition = .{ .or_op = .{ .left = &left_cond, .right = &right_cond } },
        .schema = schema,
        .allocator = allocator,
    };

    // Should return 1 and 3, skip 2
    var result1 = (try filter.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), result1.values[0].integer);

    var result2 = (try filter.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 3), result2.values[0].integer);

    try std.testing.expect((try filter.next()) == null);
}

test "filter with not condition" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    // Insert tuples: 1, 2, 3
    const t1 = Tuple{ .values = &[_]ast.Value{.{ .integer = 1 }} };
    const t2 = Tuple{ .values = &[_]ast.Value{.{ .integer = 2 }} };
    const t3 = Tuple{ .values = &[_]ast.Value{.{ .integer = 3 }} };
    _ = try heap_file.insert(&t1);
    _ = try heap_file.insert(&t2);
    _ = try heap_file.insert(&t3);

    var child = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    // Filter: NOT (id = 2)
    var inner_cond = ast.Condition{ .simple = .{ .column = "id", .op = .eq, .value = .{ .integer = 2 } } };

    var filter = Filter{
        .child = &child,
        .condition = .{ .not_op = &inner_cond },
        .schema = schema,
        .allocator = allocator,
    };

    // Should return 1 and 3, skip 2
    var result1 = (try filter.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 1), result1.values[0].integer);

    var result2 = (try filter.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 3), result2.values[0].integer);

    try std.testing.expect((try filter.next()) == null);
}

test "project selects specific columns" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
            .{ .name = "age", .data_type = .integer, .nullable = false },
        },
    };

    // Insert tuple: {1, "Alice", 20}
    const t1 = Tuple{ .values = &[_]ast.Value{
        .{ .integer = 1 },
        .{ .text = "Alice" },
        .{ .integer = 20 },
    } };
    _ = try heap_file.insert(&t1);

    var child = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    // Project: select id, name (columns 0 and 1)
    var project = Project{
        .child = &child,
        .column_indices = &[_]usize{ 0, 1 },
        .allocator = allocator,
    };

    var result = (try project.next()).?;
    defer result.deinit(allocator);

    // Should have only 2 columns
    try std.testing.expectEqual(@as(usize, 2), result.values.len);
    try std.testing.expectEqual(@as(i64, 1), result.values[0].integer);
    try std.testing.expectEqualStrings("Alice", result.values[1].text);

    try std.testing.expect((try project.next()) == null);
}

test "project reorders columns" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "a", .data_type = .integer, .nullable = false },
            .{ .name = "b", .data_type = .integer, .nullable = false },
        },
    };

    // Insert tuple: {10, 20}
    const t1 = Tuple{ .values = &[_]ast.Value{
        .{ .integer = 10 },
        .{ .integer = 20 },
    } };
    _ = try heap_file.insert(&t1);

    var child = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    // Project: select b, a (reversed order)
    var project = Project{
        .child = &child,
        .column_indices = &[_]usize{ 1, 0 },
        .allocator = allocator,
    };

    var result = (try project.next()).?;
    defer result.deinit(allocator);

    // Should be {20, 10}
    try std.testing.expectEqual(@as(i64, 20), result.values[0].integer);
    try std.testing.expectEqual(@as(i64, 10), result.values[1].integer);
}

test "project with filter pipeline" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
        },
    };

    // Insert tuples
    const t1 = Tuple{ .values = &[_]ast.Value{ .{ .integer = 1 }, .{ .text = "Alice" } } };
    const t2 = Tuple{ .values = &[_]ast.Value{ .{ .integer = 2 }, .{ .text = "Bob" } } };
    const t3 = Tuple{ .values = &[_]ast.Value{ .{ .integer = 3 }, .{ .text = "Charlie" } } };
    _ = try heap_file.insert(&t1);
    _ = try heap_file.insert(&t2);
    _ = try heap_file.insert(&t3);

    // SeqScan -> Filter (id > 1) -> Project (name only)
    var scan = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    var filter = Executor{ .filter = Filter{
        .child = &scan,
        .condition = .{ .simple = .{ .column = "id", .op = .gt, .value = .{ .integer = 1 } } },
        .schema = schema,
        .allocator = allocator,
    } };

    var project = Project{
        .child = &filter,
        .column_indices = &[_]usize{1}, // name only
        .allocator = allocator,
    };

    // Should return "Bob" and "Charlie"
    var result1 = (try project.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqualStrings("Bob", result1.values[0].text);

    var result2 = (try project.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqualStrings("Charlie", result2.values[0].text);

    try std.testing.expect((try project.next()) == null);
}

test "index_scan with eq condition" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
        },
    };

    // Insert tuples
    const t1 = Tuple{ .values = &[_]ast.Value{ .{ .integer = 10 }, .{ .text = "Alice" } } };
    const t2 = Tuple{ .values = &[_]ast.Value{ .{ .integer = 20 }, .{ .text = "Bob" } } };
    const t3 = Tuple{ .values = &[_]ast.Value{ .{ .integer = 30 }, .{ .text = "Charlie" } } };
    const rid1 = try heap_file.insert(&t1);
    const rid2 = try heap_file.insert(&t2);
    const rid3 = try heap_file.insert(&t3);

    // Build BTree index
    var btree = BTree.init(allocator);
    defer btree.deinit();
    try btree.insert(10, rid1);
    try btree.insert(20, rid2);
    try btree.insert(30, rid3);

    // IndexScan with id = 20
    var index_scan = IndexScan.init(&btree, &heap_file, schema, .{
        .column = "id",
        .op = .eq,
        .value = .{ .integer = 20 },
    }, allocator);
    defer index_scan.deinit();

    var result = (try index_scan.next()).?;
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 20), result.values[0].integer);
    try std.testing.expectEqualStrings("Bob", result.values[1].text);

    // Should return only one result
    try std.testing.expect((try index_scan.next()) == null);
}

test "index_scan with range condition gte" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    // Insert tuples
    const t1 = Tuple{ .values = &[_]ast.Value{.{ .integer = 10 }} };
    const t2 = Tuple{ .values = &[_]ast.Value{.{ .integer = 20 }} };
    const t3 = Tuple{ .values = &[_]ast.Value{.{ .integer = 30 }} };
    const rid1 = try heap_file.insert(&t1);
    const rid2 = try heap_file.insert(&t2);
    const rid3 = try heap_file.insert(&t3);

    // Build BTree index
    var btree = BTree.init(allocator);
    defer btree.deinit();
    try btree.insert(10, rid1);
    try btree.insert(20, rid2);
    try btree.insert(30, rid3);

    // IndexScan with id >= 20
    var index_scan = IndexScan.init(&btree, &heap_file, schema, .{
        .column = "id",
        .op = .gte,
        .value = .{ .integer = 20 },
    }, allocator);
    defer index_scan.deinit();

    var result1 = (try index_scan.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 20), result1.values[0].integer);

    var result2 = (try index_scan.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 30), result2.values[0].integer);

    try std.testing.expect((try index_scan.next()) == null);
}

test "index_scan with range condition lte" {
    const allocator = std.testing.allocator;

    var heap_file = try HeapFile.init(allocator);
    defer heap_file.deinit();

    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    // Insert tuples
    const t1 = Tuple{ .values = &[_]ast.Value{.{ .integer = 10 }} };
    const t2 = Tuple{ .values = &[_]ast.Value{.{ .integer = 20 }} };
    const t3 = Tuple{ .values = &[_]ast.Value{.{ .integer = 30 }} };
    const rid1 = try heap_file.insert(&t1);
    const rid2 = try heap_file.insert(&t2);
    const rid3 = try heap_file.insert(&t3);

    // Build BTree index
    var btree = BTree.init(allocator);
    defer btree.deinit();
    try btree.insert(10, rid1);
    try btree.insert(20, rid2);
    try btree.insert(30, rid3);

    // IndexScan with id <= 20
    var index_scan = IndexScan.init(&btree, &heap_file, schema, .{
        .column = "id",
        .op = .lte,
        .value = .{ .integer = 20 },
    }, allocator);
    defer index_scan.deinit();

    var result1 = (try index_scan.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 10), result1.values[0].integer);

    var result2 = (try index_scan.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 20), result2.values[0].integer);

    try std.testing.expect((try index_scan.next()) == null);
}

test "index_scan available returns false for neq" {
    try std.testing.expect(IndexScan.available(.{ .column = "id", .op = .eq, .value = .{ .integer = 1 } }));
    try std.testing.expect(IndexScan.available(.{ .column = "id", .op = .gt, .value = .{ .integer = 1 } }));
    try std.testing.expect(IndexScan.available(.{ .column = "id", .op = .gte, .value = .{ .integer = 1 } }));
    try std.testing.expect(IndexScan.available(.{ .column = "id", .op = .lt, .value = .{ .integer = 1 } }));
    try std.testing.expect(IndexScan.available(.{ .column = "id", .op = .lte, .value = .{ .integer = 1 } }));
    try std.testing.expect(!IndexScan.available(.{ .column = "id", .op = .neq, .value = .{ .integer = 1 } }));
}
