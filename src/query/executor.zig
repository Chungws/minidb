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
const Table = @import("../record/table.zig").Table;

pub const Executor = union(enum) {
    seq_scan: SeqScan,
    filter: Filter,
    project: Project,
    index_scan: IndexScan,
    nested_loop_join: NestedLoopJoin,

    pub fn next(self: *Executor) anyerror!?Tuple {
        switch (self.*) {
            .seq_scan => |*scan| return try scan.next(),
            .filter => |*ft| return try ft.next(),
            .project => |*pj| return try pj.next(),
            .index_scan => |*is| return try is.next(),
            .nested_loop_join => |*join| return try join.next(),
        }
    }

    pub fn deinit(self: *Executor, allocator: Allocator) void {
        switch (self.*) {
            .filter => |*f| {
                f.child.deinit(allocator);
                allocator.destroy(f.child);
            },
            .project => |*p| {
                allocator.free(p.column_indices);
                p.child.deinit(allocator);
                allocator.destroy(p.child);
            },
            .index_scan => |*i| {
                i.deinit();
            },
            .nested_loop_join => |*j| {
                j.allocator.free(j.merged_columns);
                j.left.deinit(allocator);
                j.allocator.destroy(j.left);
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
                const val = if (t.schema.findColumnIndex(cond.column)) |idx| t.values[idx] else return false;
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
                .schema = tu.schema,
            };
        }
        return null;
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

pub const NestedLoopJoin = struct {
    left: *Executor,
    right_table: *const Table,
    left_col_idx: usize,
    right_col_idx: usize,
    current_left: ?Tuple,
    right_iter: ?HeapIterator,
    merged_columns: []const ast.ColumnDef,
    allocator: Allocator,

    pub fn init(
        left: *Executor,
        right_table: *const Table,
        left_col_idx: usize,
        right_col_idx: usize,
        left_schema: Schema,
        allocator: Allocator,
    ) !NestedLoopJoin {
        const left_cols = left_schema.columns;
        const right_cols = right_table.schema.columns;
        const merged = try allocator.alloc(ast.ColumnDef, left_cols.len + right_cols.len);
        @memcpy(merged[0..left_cols.len], left_cols);
        @memcpy(merged[left_cols.len..], right_cols);

        return NestedLoopJoin{
            .left = left,
            .right_table = right_table,
            .left_col_idx = left_col_idx,
            .right_col_idx = right_col_idx,
            .current_left = null,
            .right_iter = null,
            .merged_columns = merged,
            .allocator = allocator,
        };
    }

    pub fn next(self: *NestedLoopJoin) !?Tuple {
        while (true) {
            if (self.current_left == null) {
                self.current_left = try self.left.next();
                if (self.current_left == null) return null;

                self.right_iter = self.right_table.heap_file.scan();
            }

            if (self.right_iter) |*iter| {
                while (iter.next()) |right_data| {
                    var right_tuple = try Tuple.deserialize(right_data.data, self.right_table.schema, self.allocator);
                    defer right_tuple.deinit(self.allocator);

                    if (self.matchJoin(self.current_left.?, right_tuple)) {
                        return try self.mergeTuples(self.current_left.?, right_tuple);
                    }
                }
            }

            if (self.current_left) |*cl| {
                cl.deinit(self.allocator);
            }
            self.current_left = null;
            self.right_iter = null;
        }
    }

    fn matchJoin(self: *const NestedLoopJoin, left: Tuple, right: Tuple) bool {
        const left_val = left.values[self.left_col_idx];
        const right_val = right.values[self.right_col_idx];

        return left_val.compareValue(right_val, .eq);
    }

    fn mergeTuples(self: *NestedLoopJoin, left: Tuple, right: Tuple) !Tuple {
        const total = left.values.len + right.values.len;
        var merged = try self.allocator.alloc(ast.Value, total);

        for (left.values, 0..) |val, i| {
            merged[i] = switch (val) {
                .text => |s| .{ .text = try self.allocator.dupe(u8, s) },
                else => val,
            };
        }

        for (right.values, 0..) |val, i| {
            merged[left.values.len + i] = switch (val) {
                .text => |s| .{ .text = try self.allocator.dupe(u8, s) },
                else => val,
            };
        }

        return Tuple{
            .values = merged,
            .schema = Schema{
                .columns = self.merged_columns,
            },
        };
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
    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 10 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 20 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 30 }},
        .schema = schema,
    };
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

    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 100 }},
        .schema = schema,
    };
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

    const t = Tuple{
        .values = &[_]ast.Value{.{ .integer = 42 }},
        .schema = schema,
    };
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
    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 10 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 20 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 30 }},
        .schema = schema,
    };
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

    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 10 }},
        .schema = schema,
    };
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

    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 1 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 2 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 3 }},
        .schema = schema,
    };
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
    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 10 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 20 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 30 }},
        .schema = schema,
    };
    const t4 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 40 }},
        .schema = schema,
    };
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
    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 1 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 2 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 3 }},
        .schema = schema,
    };
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
    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 1 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 2 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 3 }},
        .schema = schema,
    };
    _ = try heap_file.insert(&t1);
    _ = try heap_file.insert(&t2);
    _ = try heap_file.insert(&t3);

    var child = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    // Filter: NOT (id = 2)
    var inner_cond = ast.Condition{ .simple = .{ .column = "id", .op = .eq, .value = .{ .integer = 2 } } };

    var filter = Filter{
        .child = &child,
        .condition = .{ .not_op = &inner_cond },
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
    }, .schema = schema };
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
    }, .schema = schema };
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
    const t1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 1 }, .{ .text = "Alice" } },
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 2 }, .{ .text = "Bob" } },
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 3 }, .{ .text = "Charlie" } },
        .schema = schema,
    };
    _ = try heap_file.insert(&t1);
    _ = try heap_file.insert(&t2);
    _ = try heap_file.insert(&t3);

    // SeqScan -> Filter (id > 1) -> Project (name only)
    var scan = Executor{ .seq_scan = SeqScan.init(&heap_file, schema, allocator) };

    var filter = Executor{ .filter = Filter{
        .child = &scan,
        .condition = .{ .simple = .{ .column = "id", .op = .gt, .value = .{ .integer = 1 } } },
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
    const t1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 10 }, .{ .text = "Alice" } },
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 20 }, .{ .text = "Bob" } },
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 30 }, .{ .text = "Charlie" } },
        .schema = schema,
    };
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
    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 10 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 20 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 30 }},
        .schema = schema,
    };
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
    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 10 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 20 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 30 }},
        .schema = schema,
    };
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

test "nested_loop_join basic" {
    const allocator = std.testing.allocator;

    // Left table: users (id, name)
    var left_heap = try HeapFile.init(allocator);
    defer left_heap.deinit();

    const left_schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
        },
    };

    const user1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 1 }, .{ .text = "Alice" } },
        .schema = left_schema,
    };
    const user2 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 2 }, .{ .text = "Bob" } },
        .schema = left_schema,
    };
    _ = try left_heap.insert(&user1);
    _ = try left_heap.insert(&user2);

    // Right table: orders (order_id, user_id, item)
    var right_table = Table{
        .name = "orders",
        .schema = Schema{
            .columns = &[_]ast.ColumnDef{
                .{ .name = "order_id", .data_type = .integer, .nullable = false },
                .{ .name = "user_id", .data_type = .integer, .nullable = false },
                .{ .name = "item", .data_type = .text, .nullable = false },
            },
        },
        .heap_file = try HeapFile.init(allocator),
        .indexes = std.StringHashMap(*BTree).init(allocator),
        .allocator = allocator,
    };
    defer right_table.heap_file.deinit();

    const o1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 100 }, .{ .integer = 1 }, .{ .text = "Book" } },
        .schema = right_table.schema,
    };
    const o2 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 101 }, .{ .integer = 2 }, .{ .text = "Pen" } },
        .schema = right_table.schema,
    };
    _ = try right_table.heap_file.insert(&o1);
    _ = try right_table.heap_file.insert(&o2);

    // JOIN: users.id = orders.user_id
    var left_scan = Executor{ .seq_scan = SeqScan.init(&left_heap, left_schema, allocator) };

    var join = try NestedLoopJoin.init(
        &left_scan,
        &right_table,
        0, // id
        1, // user_id
        left_schema,
        allocator,
    );
    defer allocator.free(join.merged_columns);

    // First result: Alice's order
    var result1 = (try join.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 5), result1.values.len);
    try std.testing.expectEqual(@as(i64, 1), result1.values[0].integer);
    try std.testing.expectEqualStrings("Alice", result1.values[1].text);
    try std.testing.expectEqual(@as(i64, 100), result1.values[2].integer);
    try std.testing.expectEqualStrings("Book", result1.values[4].text);

    // Second result: Bob's order
    var result2 = (try join.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 2), result2.values[0].integer);
    try std.testing.expectEqualStrings("Bob", result2.values[1].text);
    try std.testing.expectEqualStrings("Pen", result2.values[4].text);

    // No more results
    try std.testing.expect((try join.next()) == null);
}

test "nested_loop_join no matches" {
    const allocator = std.testing.allocator;

    // Left table
    var left_heap = try HeapFile.init(allocator);
    defer left_heap.deinit();

    const left_schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    const user1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 1 }},
        .schema = left_schema,
    };
    _ = try left_heap.insert(&user1);

    // Right table with no matching user_id
    var right_table = Table{
        .name = "orders",
        .schema = Schema{
            .columns = &[_]ast.ColumnDef{
                .{ .name = "user_id", .data_type = .integer, .nullable = false },
            },
        },
        .heap_file = try HeapFile.init(allocator),
        .indexes = std.StringHashMap(*BTree).init(allocator),
        .allocator = allocator,
    };
    defer right_table.heap_file.deinit();

    const o1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 999 }},
        .schema = right_table.schema,
    };
    _ = try right_table.heap_file.insert(&o1);

    var left_scan = Executor{ .seq_scan = SeqScan.init(&left_heap, left_schema, allocator) };

    var join = try NestedLoopJoin.init(
        &left_scan,
        &right_table,
        0, // id
        0, // user_id
        left_schema,
        allocator,
    );
    defer allocator.free(join.merged_columns);

    // No matches
    try std.testing.expect((try join.next()) == null);
}

test "nested_loop_join one to many" {
    const allocator = std.testing.allocator;

    // Left table: one user
    var left_heap = try HeapFile.init(allocator);
    defer left_heap.deinit();

    const left_schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    const user1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 1 }},
        .schema = left_schema,
    };
    _ = try left_heap.insert(&user1);

    // Right table: multiple orders for same user
    var right_table = Table{
        .name = "orders",
        .schema = Schema{
            .columns = &[_]ast.ColumnDef{
                .{ .name = "order_id", .data_type = .integer, .nullable = false },
                .{ .name = "user_id", .data_type = .integer, .nullable = false },
            },
        },
        .heap_file = try HeapFile.init(allocator),
        .indexes = std.StringHashMap(*BTree).init(allocator),
        .allocator = allocator,
    };
    defer right_table.heap_file.deinit();

    const o1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 100 }, .{ .integer = 1 } },
        .schema = right_table.schema,
    };
    const o2 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 101 }, .{ .integer = 1 } },
        .schema = right_table.schema,
    };
    const o3 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 102 }, .{ .integer = 1 } },
        .schema = right_table.schema,
    };
    _ = try right_table.heap_file.insert(&o1);
    _ = try right_table.heap_file.insert(&o2);
    _ = try right_table.heap_file.insert(&o3);

    var left_scan = Executor{ .seq_scan = SeqScan.init(&left_heap, left_schema, allocator) };

    var join = try NestedLoopJoin.init(
        &left_scan,
        &right_table,
        0, // id
        1, // user_id
        left_schema,
        allocator,
    );
    defer allocator.free(join.merged_columns);

    // Should return 3 results (1:N)
    var result1 = (try join.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 100), result1.values[1].integer);

    var result2 = (try join.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 101), result2.values[1].integer);

    var result3 = (try join.next()).?;
    defer result3.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 102), result3.values[1].integer);

    try std.testing.expect((try join.next()) == null);
}

test "nested_loop_join empty left table" {
    const allocator = std.testing.allocator;

    // Empty left table
    var left_heap = try HeapFile.init(allocator);
    defer left_heap.deinit();

    const left_schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    // Right table with data
    var right_table = Table{
        .name = "orders",
        .schema = Schema{
            .columns = &[_]ast.ColumnDef{
                .{ .name = "user_id", .data_type = .integer, .nullable = false },
            },
        },
        .heap_file = try HeapFile.init(allocator),
        .indexes = std.StringHashMap(*BTree).init(allocator),
        .allocator = allocator,
    };
    defer right_table.heap_file.deinit();

    const o1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 1 }},
        .schema = left_schema,
    };
    _ = try right_table.heap_file.insert(&o1);

    var left_scan = Executor{ .seq_scan = SeqScan.init(&left_heap, left_schema, allocator) };

    var join = try NestedLoopJoin.init(
        &left_scan,
        &right_table,
        0, // id
        0, // user_id
        left_schema,
        allocator,
    );
    defer allocator.free(join.merged_columns);

    // Empty left = no results
    try std.testing.expect((try join.next()) == null);
}

test "nested_loop_join empty right table" {
    const allocator = std.testing.allocator;

    // Left table with data
    var left_heap = try HeapFile.init(allocator);
    defer left_heap.deinit();

    const left_schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    const user1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 1 }},
        .schema = left_schema,
    };
    _ = try left_heap.insert(&user1);

    // Empty right table
    var right_table = Table{
        .name = "orders",
        .schema = Schema{
            .columns = &[_]ast.ColumnDef{
                .{ .name = "user_id", .data_type = .integer, .nullable = false },
            },
        },
        .heap_file = try HeapFile.init(allocator),
        .indexes = std.StringHashMap(*BTree).init(allocator),
        .allocator = allocator,
    };
    defer right_table.heap_file.deinit();

    var left_scan = Executor{ .seq_scan = SeqScan.init(&left_heap, left_schema, allocator) };

    var join = try NestedLoopJoin.init(
        &left_scan,
        &right_table,
        0, // id
        0, // user_id
        left_schema,
        allocator,
    );
    defer allocator.free(join.merged_columns);

    // Empty right = no results
    try std.testing.expect((try join.next()) == null);
}

test "nested_loop_join with text columns deep copy" {
    const allocator = std.testing.allocator;

    // Left table with text
    var left_heap = try HeapFile.init(allocator);
    defer left_heap.deinit();

    const left_schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
        },
    };

    const user1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 1 }, .{ .text = "Alice" } },
        .schema = left_schema,
    };
    _ = try left_heap.insert(&user1);

    // Right table with text
    var right_table = Table{
        .name = "orders",
        .schema = Schema{
            .columns = &[_]ast.ColumnDef{
                .{ .name = "user_id", .data_type = .integer, .nullable = false },
                .{ .name = "item", .data_type = .text, .nullable = false },
            },
        },
        .heap_file = try HeapFile.init(allocator),
        .indexes = std.StringHashMap(*BTree).init(allocator),
        .allocator = allocator,
    };
    defer right_table.heap_file.deinit();

    const o1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 1 }, .{ .text = "Laptop" } },
        .schema = right_table.schema,
    };
    _ = try right_table.heap_file.insert(&o1);

    var left_scan = Executor{ .seq_scan = SeqScan.init(&left_heap, left_schema, allocator) };

    var join = try NestedLoopJoin.init(
        &left_scan,
        &right_table,
        0, // id
        0, // user_id
        left_schema,
        allocator,
    );
    defer allocator.free(join.merged_columns);

    var result = (try join.next()).?;
    defer result.deinit(allocator);

    // Verify text values are properly copied
    try std.testing.expectEqualStrings("Alice", result.values[1].text);
    try std.testing.expectEqualStrings("Laptop", result.values[3].text);

    try std.testing.expect((try join.next()) == null);
}
