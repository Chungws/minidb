const std = @import("std");
const Allocator = std.mem.Allocator;

const Table = @import("../record/table.zig").Table;
const Schema = @import("../record/tuple.zig").Schema;

pub const Catalog = struct {
    tables: std.StringHashMap(*Table),
    allocator: Allocator,

    pub fn init(allocator: Allocator) Catalog {
        return .{
            .tables = std.StringHashMap(*Table).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Catalog) void {
        var it = self.tables.valueIterator();
        while (it.next()) |table| {
            self.allocator.free(table.*.name);
            for (table.*.schema.columns) |col| {
                self.allocator.free(col.name);
            }
            self.allocator.free(table.*.schema.columns);

            table.*.deinit();
            self.allocator.destroy(table.*);
        }
        self.tables.deinit();
    }

    pub fn createTable(self: *Catalog, name: []const u8, schema: Schema) !void {
        const owned_name = try self.allocator.dupe(u8, name);
        var owned_columns = try self.allocator.alloc(ColumnDef, schema.columns.len);
        for (schema.columns, 0..) |col, i| {
            owned_columns[i] = ColumnDef{
                .name = try self.allocator.dupe(u8, col.name), // 이름도 복사!
                .data_type = col.data_type,
                .nullable = col.nullable,
            };
        }
        const owned_schema = Schema{ .columns = owned_columns };

        const table_ptr = try self.allocator.create(Table);
        table_ptr.* = try Table.init(owned_name, owned_schema, self.allocator);
        try self.tables.put(owned_name, table_ptr);
    }

    pub fn getTable(self: *const Catalog, name: []const u8) ?*Table {
        return self.tables.get(name);
    }
};

// ============ Tests ============

const tuple = @import("../record/tuple.zig");
const ast = @import("../sql/ast.zig");
const ColumnDef = ast.ColumnDef;

test "catalog init and deinit" {
    const allocator = std.testing.allocator;
    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    try std.testing.expectEqual(@as(usize, 0), catalog.tables.count());
}

test "catalog create and get table" {
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

    const table = catalog.getTable("users");
    try std.testing.expect(table != null);
    try std.testing.expectEqualStrings("users", table.?.name);
}

test "catalog get non-existent table returns null" {
    const allocator = std.testing.allocator;
    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const table = catalog.getTable("no_such_table");
    try std.testing.expect(table == null);
}

test "catalog create multiple tables" {
    const allocator = std.testing.allocator;
    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const schema1 = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    const schema2 = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "order_id", .data_type = .integer, .nullable = false },
        },
    };

    try catalog.createTable("users", schema1);
    try catalog.createTable("orders", schema2);

    try std.testing.expectEqual(@as(usize, 2), catalog.tables.count());
    try std.testing.expect(catalog.getTable("users") != null);
    try std.testing.expect(catalog.getTable("orders") != null);
}

test "catalog table can insert and retrieve data" {
    const allocator = std.testing.allocator;
    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    try catalog.createTable("test", schema);

    const table = catalog.getTable("test").?;
    const t = tuple.Tuple{ .values = &[_]ast.Value{.{ .integer = 42 }} };
    const rid = try table.insert(&t);

    var result = (try table.get(rid, allocator)).?;
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 42), result.values[0].integer);
}
