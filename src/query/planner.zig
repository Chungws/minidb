const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("../sql/ast.zig");
const Schema = @import("../record/tuple.zig").Schema;
const Catalog = @import("catalog.zig").Catalog;
const executor = @import("executor.zig");
const Executor = executor.Executor;
const SeqScan = executor.SeqScan;
const Filter = executor.Filter;
const Project = executor.Project;

const PlannerError = error{
    TableNotFound,
};

pub const Planner = struct {
    catalog: *Catalog,
    allocator: Allocator,

    pub fn planSelect(self: *Planner, stmt: ast.SelectStatement) !*Executor {
        const table = self.catalog.getTable(stmt.table_name) orelse return error.TableNotFound;

        var exec_ptr = try self.allocator.create(Executor);
        exec_ptr.* = Executor{
            .seq_scan = SeqScan.init(&table.heap_file, table.schema, self.allocator),
        };

        if (stmt.where) |cond| {
            const filter_ptr = try self.allocator.create(Executor);
            filter_ptr.* = Executor{
                .filter = Filter{
                    .child = exec_ptr,
                    .condition = cond,
                    .schema = table.schema,
                    .allocator = self.allocator,
                },
            };
            exec_ptr = filter_ptr;
        }

        if (!isSelectAll(stmt.columns)) {
            const indices = try self.resolveColumns(stmt.columns, table.schema);
            const project_ptr = try self.allocator.create(Executor);
            project_ptr.* = Executor{ .project = Project{
                .child = exec_ptr,
                .column_indices = indices,
                .allocator = self.allocator,
            } };
            exec_ptr = project_ptr;
        }

        return exec_ptr;
    }

    pub fn destroyPlan(self: *Planner, exec: *Executor) void {
        switch (exec.*) {
            .filter => |*f| {
                self.destroyPlan(f.child);
            },
            .project => |*p| {
                self.allocator.free(p.column_indices); // 여기서 free
                self.destroyPlan(p.child);
            },
            .seq_scan => {},
        }
        self.allocator.destroy(exec);
    }

    fn isSelectAll(columns: []const []const u8) bool {
        for (columns) |col| {
            if (std.mem.eql(u8, col, "*")) {
                return true;
            }
        }
        return false;
    }

    fn resolveColumns(self: *Planner, columns: []const []const u8, schema: Schema) ![]const usize {
        var indices = try self.allocator.alloc(usize, columns.len);
        for (columns, 0..) |col, i| {
            for (schema.columns, 0..) |sc, j| {
                if (std.mem.eql(u8, col, sc.name)) {
                    indices[i] = j;
                }
            }
        }
        return indices;
    }
};

// ============ Tests ============

const tuple = @import("../record/tuple.zig");
const ColumnDef = tuple.ColumnDef;
const Tuple = tuple.Tuple;

test "planner select all from table" {
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

    // Insert data
    const table = catalog.getTable("users").?;
    const t1 = Tuple{ .values = &[_]ast.Value{ .{ .integer = 1 }, .{ .text = "Alice" } } };
    _ = try table.insert(&t1);

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    // SELECT * FROM users
    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{"*"},
        .table_name = "users",
        .where = null,
    };

    const exec = try planner.planSelect(stmt);
    defer planner.destroyPlan(exec);

    var result = (try exec.next()).?;
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), result.values[0].integer);
    try std.testing.expectEqualStrings("Alice", result.values[1].text);
}

test "planner select with where clause" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    try catalog.createTable("nums", schema);

    const table = catalog.getTable("nums").?;
    const t1 = Tuple{ .values = &[_]ast.Value{.{ .integer = 10 }} };
    const t2 = Tuple{ .values = &[_]ast.Value{.{ .integer = 20 }} };
    const t3 = Tuple{ .values = &[_]ast.Value{.{ .integer = 30 }} };
    _ = try table.insert(&t1);
    _ = try table.insert(&t2);
    _ = try table.insert(&t3);

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    // SELECT * FROM nums WHERE id > 15
    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{"*"},
        .table_name = "nums",
        .where = .{ .simple = .{ .column = "id", .op = .gt, .value = .{ .integer = 15 } } },
    };

    const exec = try planner.planSelect(stmt);
    defer planner.destroyPlan(exec);

    var result1 = (try exec.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 20), result1.values[0].integer);

    var result2 = (try exec.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 30), result2.values[0].integer);

    try std.testing.expect((try exec.next()) == null);
}

test "planner select specific columns" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
            .{ .name = "age", .data_type = .integer, .nullable = false },
        },
    };
    try catalog.createTable("people", schema);

    const table = catalog.getTable("people").?;
    const t1 = Tuple{ .values = &[_]ast.Value{ .{ .integer = 1 }, .{ .text = "Bob" }, .{ .integer = 25 } } };
    _ = try table.insert(&t1);

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    // SELECT name, age FROM people
    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{ "name", "age" },
        .table_name = "people",
        .where = null,
    };

    const exec = try planner.planSelect(stmt);
    defer planner.destroyPlan(exec);

    var result = (try exec.next()).?;
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), result.values.len);
    try std.testing.expectEqualStrings("Bob", result.values[0].text);
    try std.testing.expectEqual(@as(i64, 25), result.values[1].integer);
}

test "planner table not found" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{"*"},
        .table_name = "no_such_table",
        .where = null,
    };

    const result = planner.planSelect(stmt);
    try std.testing.expectError(error.TableNotFound, result);
}
