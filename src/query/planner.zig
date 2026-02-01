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
const IndexScan = executor.IndexScan;
const NestedLoopJoin = executor.NestedLoopJoin;

const PlannerError = error{
    ColumnCountMismatch,
    ColumnNotFound,
    TableNotFound,
};

pub const Planner = struct {
    catalog: *Catalog,
    allocator: Allocator,

    pub fn destroyPlan(self: *Planner, exec: *Executor) void {
        exec.deinit(self.allocator);
        self.allocator.destroy(exec);
    }

    pub fn executeInsert(self: *Planner, stmt: ast.InsertStatement) !void {
        const table = self.catalog.getTable(stmt.table_name) orelse return error.TableNotFound;

        if (stmt.values.len != table.schema.columns.len) {
            return error.ColumnCountMismatch;
        }

        const t = Tuple{
            .values = stmt.values,
            .schema = table.schema,
        };
        _ = try table.insert(&t);
    }

    pub fn executeCreateTable(self: *Planner, stmt: ast.CreateTableStatement) !void {
        const schema = Schema{ .columns = stmt.columns };
        try self.catalog.createTable(stmt.table_name, schema);
    }

    pub fn executeCreateIndex(self: *Planner, stmt: ast.CreateIndexStatement) !void {
        const table = self.catalog.getTable(stmt.table_name) orelse return error.TableNotFound;
        try table.createIndex(stmt.column_name);
    }

    pub fn planSelect(self: *Planner, stmt: ast.SelectStatement) !*Executor {
        const table = self.catalog.getTable(stmt.table_name) orelse return error.TableNotFound;

        var current_schema = table.schema;

        var exec_ptr = try self.allocator.create(Executor);
        errdefer self.destroyPlan(exec_ptr);

        var use_index = false;
        if (stmt.where) |cond| {
            if (cond == .simple and IndexScan.available(cond.simple)) {
                if (table.indexes.get(cond.simple.column)) |btree| {
                    exec_ptr.* = Executor{
                        .index_scan = IndexScan.init(
                            btree,
                            &table.heap_file,
                            current_schema,
                            cond.simple,
                            self.allocator,
                        ),
                    };
                    use_index = true;
                }
            }
        }

        if (!use_index) {
            exec_ptr.* = Executor{
                .seq_scan = SeqScan.init(
                    &table.heap_file,
                    current_schema,
                    self.allocator,
                ),
            };
        }

        if (stmt.join) |join| {
            const right_table = self.catalog.getTable(join.table_name) orelse return error.TableNotFound;
            const left_col_idx = table.schema.findColumnIndex(join.left_column) orelse return error.ColumnNotFound;
            const right_col_idx = right_table.schema.findColumnIndex(join.right_column) orelse return error.ColumnNotFound;

            const join_exec = try self.allocator.create(Executor);
            join_exec.* = Executor{ .nested_loop_join = try NestedLoopJoin.init(
                exec_ptr,
                right_table,
                left_col_idx,
                right_col_idx,
                table.schema,
                self.allocator,
            ) };
            current_schema = Schema{ .columns = join_exec.nested_loop_join.merged_columns };
            exec_ptr = join_exec;
        }

        if (!use_index) {
            if (stmt.where) |cond| {
                const filter_ptr = try self.allocator.create(Executor);
                filter_ptr.* = Executor{
                    .filter = Filter{
                        .child = exec_ptr,
                        .condition = cond,
                        .allocator = self.allocator,
                    },
                };
                exec_ptr = filter_ptr;
            }
        }

        if (!isSelectAll(stmt.columns)) {
            const indices = try self.resolveColumns(stmt.columns, current_schema);
            const proj_cols = try self.allocator.alloc(ColumnDef, indices.len);
            for (indices, 0..) |idx, i| {
                proj_cols[i] = current_schema.columns[idx];
            }

            const project_ptr = try self.allocator.create(Executor);
            project_ptr.* = Executor{ .project = Project{
                .child = exec_ptr,
                .column_indices = indices,
                .projected_schema = Schema{ .columns = proj_cols },
                .allocator = self.allocator,
            } };
            exec_ptr = project_ptr;
        }

        return exec_ptr;
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
            indices[i] = schema.findColumnIndex(col) orelse {
                self.allocator.free(indices);
                return error.ColumnNotFound;
            };
        }
        return indices;
    }
};

// ============ Tests ============

const ColumnDef = ast.ColumnDef;
const tuple = @import("../record/tuple.zig");
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
    const t1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 1 }, .{ .text = "Alice" } },
        .schema = table.schema,
    };
    _ = try table.insert(&t1);

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    // SELECT * FROM users
    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{"*"},
        .table_name = "users",
        .join = null,
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
    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 10 }},
        .schema = table.schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 20 }},
        .schema = table.schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 30 }},
        .schema = table.schema,
    };
    _ = try table.insert(&t1);
    _ = try table.insert(&t2);
    _ = try table.insert(&t3);

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    // SELECT * FROM nums WHERE id > 15
    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{"*"},
        .table_name = "nums",
        .join = null,
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
    const t1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 1 }, .{ .text = "Bob" }, .{ .integer = 25 } },
        .schema = table.schema,
    };
    _ = try table.insert(&t1);

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    // SELECT name, age FROM people
    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{ "name", "age" },
        .table_name = "people",
        .join = null,
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
        .join = null,
        .where = null,
    };

    const result = planner.planSelect(stmt);
    try std.testing.expectError(error.TableNotFound, result);
}

test "executeCreate creates table" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    const stmt = ast.CreateTableStatement{
        .table_name = "users",
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = true },
        },
    };

    try planner.executeCreateTable(stmt);

    const table = catalog.getTable("users");
    try std.testing.expect(table != null);
    try std.testing.expectEqualStrings("users", table.?.name);
    try std.testing.expectEqual(@as(usize, 2), table.?.schema.columns.len);
}

test "executeInsert inserts row" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    // Create table first
    const create_stmt = ast.CreateTableStatement{
        .table_name = "users",
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
        },
    };
    try planner.executeCreateTable(create_stmt);

    // Insert row
    const insert_stmt = ast.InsertStatement{
        .table_name = "users",
        .values = &[_]ast.Value{ .{ .integer = 1 }, .{ .text = "Alice" } },
    };
    try planner.executeInsert(insert_stmt);

    // Verify with SELECT
    const select_stmt = ast.SelectStatement{
        .columns = &[_][]const u8{"*"},
        .table_name = "users",
        .join = null,
        .where = null,
    };
    const exec = try planner.planSelect(select_stmt);
    defer planner.destroyPlan(exec);

    var result = (try exec.next()).?;
    defer result.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), result.values[0].integer);
    try std.testing.expectEqualStrings("Alice", result.values[1].text);
}

test "executeInsert table not found" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    const stmt = ast.InsertStatement{
        .table_name = "no_such_table",
        .values = &[_]ast.Value{.{ .integer = 1 }},
    };

    const result = planner.executeInsert(stmt);
    try std.testing.expectError(error.TableNotFound, result);
}

test "planner uses IndexScan when index exists" {
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

    const table = catalog.getTable("users").?;
    const t1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 10 }, .{ .text = "Alice" } },
        .schema = table.schema,
    };
    const t2 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 20 }, .{ .text = "Bob" } },
        .schema = table.schema,
    };
    const t3 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 30 }, .{ .text = "Charlie" } },
        .schema = table.schema,
    };
    _ = try table.insert(&t1);
    _ = try table.insert(&t2);
    _ = try table.insert(&t3);

    // Create index on id
    try table.createIndex("id");

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    // SELECT * FROM users WHERE id = 20
    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{"*"},
        .table_name = "users",
        .join = null,
        .where = .{ .simple = .{ .column = "id", .op = .eq, .value = .{ .integer = 20 } } },
    };

    const exec = try planner.planSelect(stmt);
    defer planner.destroyPlan(exec);

    // Should use IndexScan
    try std.testing.expect(exec.* == .index_scan);

    var result = (try exec.next()).?;
    defer result.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 20), result.values[0].integer);
    try std.testing.expectEqualStrings("Bob", result.values[1].text);

    try std.testing.expect((try exec.next()) == null);
}

test "planner uses SeqScan when no index" {
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
    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 10 }},
        .schema = table.schema,
    };
    _ = try table.insert(&t1);

    // No index created

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    // SELECT * FROM nums WHERE id = 10
    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{"*"},
        .table_name = "nums",
        .join = null,
        .where = .{ .simple = .{ .column = "id", .op = .eq, .value = .{ .integer = 10 } } },
    };

    const exec = try planner.planSelect(stmt);
    defer planner.destroyPlan(exec);

    // Should use Filter (with SeqScan as child)
    try std.testing.expect(exec.* == .filter);
}

test "planner uses SeqScan for neq condition even with index" {
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
    const t1 = Tuple{
        .values = &[_]ast.Value{.{ .integer = 10 }},
        .schema = table.schema,
    };
    _ = try table.insert(&t1);

    // Create index
    try table.createIndex("id");

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    // SELECT * FROM nums WHERE id != 10 (neq cannot use index)
    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{"*"},
        .table_name = "nums",
        .join = null,
        .where = .{ .simple = .{ .column = "id", .op = .neq, .value = .{ .integer = 10 } } },
    };

    const exec = try planner.planSelect(stmt);
    defer planner.destroyPlan(exec);

    // Should use Filter (not IndexScan)
    try std.testing.expect(exec.* == .filter);
}

test "executeCreateIndex creates index" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    try catalog.createTable("nums", schema);

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    const stmt = ast.CreateIndexStatement{
        .index_name = "idx_id",
        .table_name = "nums",
        .column_name = "id",
    };

    try planner.executeCreateIndex(stmt);

    // Verify index exists
    const table = catalog.getTable("nums").?;
    try std.testing.expect(table.indexes.get("id") != null);
}

test "executeCreateIndex table not found" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    const stmt = ast.CreateIndexStatement{
        .index_name = "idx_id",
        .table_name = "no_such_table",
        .column_name = "id",
    };

    const result = planner.executeCreateIndex(stmt);
    try std.testing.expectError(error.TableNotFound, result);
}

test "planner select with join" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    // Create users table
    const user_schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
        },
    };
    try catalog.createTable("users", user_schema);

    const users = catalog.getTable("users").?;
    const user1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 1 }, .{ .text = "Alice" } },
        .schema = users.schema,
    };
    const user2 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 2 }, .{ .text = "Bob" } },
        .schema = users.schema,
    };
    _ = try users.insert(&user1);
    _ = try users.insert(&user2);

    // Create orders table
    const order_schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "order_id", .data_type = .integer, .nullable = false },
            .{ .name = "user_id", .data_type = .integer, .nullable = false },
        },
    };
    try catalog.createTable("orders", order_schema);

    const orders = catalog.getTable("orders").?;
    const o1 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 100 }, .{ .integer = 1 } },
        .schema = orders.schema,
    };
    const o2 = Tuple{
        .values = &[_]ast.Value{ .{ .integer = 101 }, .{ .integer = 2 } },
        .schema = orders.schema,
    };
    _ = try orders.insert(&o1);
    _ = try orders.insert(&o2);

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    // SELECT * FROM users JOIN orders ON users.id = orders.user_id
    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{"*"},
        .table_name = "users",
        .join = .{
            .table_name = "orders",
            .left_column = "id",
            .right_column = "user_id",
        },
        .where = null,
    };

    const exec = try planner.planSelect(stmt);
    defer planner.destroyPlan(exec);

    // Should return merged tuples (4 values each)
    var result1 = (try exec.next()).?;
    defer result1.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 4), result1.values.len);
    try std.testing.expectEqual(@as(i64, 1), result1.values[0].integer);
    try std.testing.expectEqualStrings("Alice", result1.values[1].text);
    try std.testing.expectEqual(@as(i64, 100), result1.values[2].integer);

    var result2 = (try exec.next()).?;
    defer result2.deinit(allocator);
    try std.testing.expectEqual(@as(i64, 2), result2.values[0].integer);
    try std.testing.expectEqualStrings("Bob", result2.values[1].text);
    try std.testing.expectEqual(@as(i64, 101), result2.values[2].integer);

    try std.testing.expect((try exec.next()) == null);
}

test "planner select with join right table not found" {
    const allocator = std.testing.allocator;

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    try catalog.createTable("users", schema);

    var planner = Planner{ .catalog = &catalog, .allocator = allocator };

    const stmt = ast.SelectStatement{
        .columns = &[_][]const u8{"*"},
        .table_name = "users",
        .join = .{
            .table_name = "no_such_table",
            .left_column = "id",
            .right_column = "user_id",
        },
        .where = null,
    };

    const result = planner.planSelect(stmt);
    try std.testing.expectError(error.TableNotFound, result);
}
