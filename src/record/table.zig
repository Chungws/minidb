const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("../sql/ast.zig");
const Value = ast.Value;
const BTree = @import("../index/btree.zig").BTree;
const tuple = @import("tuple.zig");
const Tuple = tuple.Tuple;
const Schema = tuple.Schema;
const heap = @import("heap.zig");
const HeapFile = heap.HeapFile;
const HeapIterator = heap.HeapIterator;
const RID = heap.RID;

pub const Table = struct {
    name: []const u8,
    schema: Schema,
    heap_file: HeapFile,
    indexes: std.StringHashMap(*BTree),
    allocator: Allocator,

    pub fn init(name: []const u8, schema: Schema, allocator: Allocator) !Table {
        const heap_file = try HeapFile.init(allocator);
        return .{
            .name = name,
            .schema = schema,
            .heap_file = heap_file,
            .indexes = std.StringHashMap(*BTree).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Table) void {
        var it = self.indexes.valueIterator();
        while (it.next()) |btree| {
            btree.*.deinit();
            self.allocator.destroy(btree.*);
        }
        self.indexes.deinit();
        self.heap_file.deinit();
    }

    pub fn insert(self: *Table, t: *const Tuple) !RID {
        const rid = try self.heap_file.insert(t);

        var it = self.indexes.iterator();
        while (it.next()) |entry| {
            const col_idx = self.schema.findColumnIndex(entry.key_ptr.*).?;
            const key = t.values[col_idx].integer;
            try entry.value_ptr.*.insert(key, rid);
        }

        return rid;
    }

    pub fn get(self: *const Table, rid: RID, allocator: Allocator) !?Tuple {
        const record = self.heap_file.get(rid);
        if (record) |r| {
            return try Tuple.deserialize(r, self.schema, allocator);
        }
        return null;
    }

    pub fn delete(self: *Table, rid: RID) void {
        self.heap_file.delete(rid);
    }

    pub fn scan(self: *const Table) HeapIterator {
        return self.heap_file.scan();
    }

    pub fn createIndex(self: *Table, column_name: []const u8) !void {
        if (self.schema.findColumnDef(column_name)) |def| {
            if (def.data_type != .integer) {
                return;
            }
        }

        const col_idx = self.schema.findColumnIndex(column_name).?;
        var tree = try self.allocator.create(BTree);
        tree.* = BTree.init(self.allocator);

        var iter = self.scan();
        while (iter.next()) |t| {
            var d = try Tuple.deserialize(t.data, self.schema, self.allocator);
            defer d.deinit(self.allocator);

            try tree.insert(d.values[col_idx].integer, t.rid);
        }

        try self.indexes.put(column_name, tree);
    }
};

// ============ Tests ============

test "table init and deinit" {
    const allocator = std.testing.allocator;
    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    var table = try Table.init("test_table", schema, allocator);
    defer table.deinit();

    try std.testing.expectEqualStrings("test_table", table.name);
}

test "table insert and get" {
    const allocator = std.testing.allocator;
    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
        },
    };
    var table = try Table.init("users", schema, allocator);
    defer table.deinit();

    const t = Tuple{
        .values = &[_]Value{
            .{ .integer = 42 },
            .{ .text = "alice" },
        },
    };

    const rid = try table.insert(&t);
    var result = try table.get(rid, allocator);
    defer result.?.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 42), result.?.values[0].integer);
    try std.testing.expectEqualStrings("alice", result.?.values[1].text);
}

test "table delete" {
    const allocator = std.testing.allocator;
    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    var table = try Table.init("test", schema, allocator);
    defer table.deinit();

    const t = Tuple{ .values = &[_]Value{.{ .integer = 1 }} };
    const rid = try table.insert(&t);

    table.delete(rid);

    const result = try table.get(rid, allocator);
    try std.testing.expect(result == null);
}

test "table createIndex and search" {
    const allocator = std.testing.allocator;
    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
        },
    };
    var table = try Table.init("users", schema, allocator);
    defer table.deinit();

    // Insert data first
    const t1 = Tuple{ .values = &[_]Value{ .{ .integer = 10 }, .{ .text = "Alice" } } };
    const t2 = Tuple{ .values = &[_]Value{ .{ .integer = 20 }, .{ .text = "Bob" } } };
    const t3 = Tuple{ .values = &[_]Value{ .{ .integer = 30 }, .{ .text = "Charlie" } } };
    _ = try table.insert(&t1);
    _ = try table.insert(&t2);
    _ = try table.insert(&t3);

    // Create index on id column
    try table.createIndex("id");

    // Index should exist
    try std.testing.expect(table.indexes.get("id") != null);

    // Search using index
    const btree = table.indexes.get("id").?;
    const rid = try btree.search(20);
    try std.testing.expect(rid != null);

    // Verify the found record
    var result = (try table.get(rid.?, allocator)).?;
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 20), result.values[0].integer);
    try std.testing.expectEqualStrings("Bob", result.values[1].text);
}

test "table insert updates index" {
    const allocator = std.testing.allocator;
    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    var table = try Table.init("nums", schema, allocator);
    defer table.deinit();

    // Create index first (empty table)
    try table.createIndex("id");

    // Insert after index creation
    const t1 = Tuple{ .values = &[_]Value{.{ .integer = 100 }} };
    _ = try table.insert(&t1);

    // Index should be updated
    const btree = table.indexes.get("id").?;
    const rid = try btree.search(100);
    try std.testing.expect(rid != null);
}

test "table createIndex ignores non-integer columns" {
    const allocator = std.testing.allocator;
    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "name", .data_type = .text, .nullable = false },
        },
    };
    var table = try Table.init("test", schema, allocator);
    defer table.deinit();

    // Try to create index on text column - should be ignored
    try table.createIndex("name");

    // Index should not exist
    try std.testing.expect(table.indexes.get("name") == null);
}
