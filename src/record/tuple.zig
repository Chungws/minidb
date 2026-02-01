const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("../sql/ast.zig");
const Value = ast.Value;
const DataType = ast.DataType;
const ColumnDef = ast.ColumnDef;

pub const Schema = struct {
    columns: []const ColumnDef,

    pub fn findColumnDef(self: *const Schema, name: []const u8) ?ColumnDef {
        for (self.columns) |c| {
            if (std.mem.eql(u8, c.name, name)) {
                return c;
            }
        }
        return null;
    }

    pub fn findColumnIndex(self: *const Schema, name: []const u8) ?usize {
        for (self.columns, 0..) |c, i| {
            if (std.mem.eql(u8, c.name, name)) {
                return i;
            }
        }
        return null;
    }
};

pub const Tuple = struct {
    values: []const Value,
    schema: Schema,

    pub fn serialize(self: *const Tuple, allocator: Allocator) ![]u8 {
        const bitmap_size = (self.values.len + 7) / 8;
        const total_size = self.calculateTotalSize();

        var buf = try allocator.alloc(u8, total_size);
        var offset = bitmap_size;
        @memset(buf[0..bitmap_size], 0);

        for (self.values, 0..) |value, i| {
            switch (value) {
                .null_value => {
                    buf[i / 8] |= (@as(u8, 1) << @intCast(i % 8));
                },
                .integer => |v| {
                    std.mem.writeInt(i64, buf[offset..][0..8], v, .little);
                    offset += 8;
                },
                .boolean => |b| {
                    const int_bool: u8 = if (b) 1 else 0;
                    std.mem.writeInt(u8, buf[offset..][0..1], @intCast(int_bool), .little);
                    offset += 1;
                },
                .text => |s| {
                    std.mem.writeInt(u16, buf[offset..][0..2], @intCast(s.len), .little);
                    offset += 2;
                    @memcpy(buf[offset..][0..s.len], s);
                    offset += s.len;
                },
            }
        }
        return buf;
    }

    pub fn deserialize(bytes: []const u8, schema: Schema, allocator: Allocator) !Tuple {
        const bitmap_size = (schema.columns.len + 7) / 8;
        const bitmap = try allocator.alloc(u8, bitmap_size);
        defer allocator.free(bitmap);
        @memcpy(bitmap, bytes[0..][0..bitmap_size]);

        var values = std.ArrayList(Value).empty;
        var offset = bitmap_size;
        for (schema.columns, 0..) |column, i| {
            const is_null = (bitmap[i / 8] & (@as(u8, 1) << @intCast(i % 8))) != 0;
            if (column.nullable and is_null) {
                try values.append(allocator, Value{ .null_value = {} });
                continue;
            }

            switch (column.data_type) {
                .integer => {
                    const value = std.mem.readInt(i64, bytes[offset..][0..8], .little);
                    try values.append(allocator, Value{ .integer = value });
                    offset += 8;
                },
                .boolean => {
                    const value = std.mem.readInt(u8, bytes[offset..][0..1], .little);
                    try values.append(allocator, Value{ .boolean = if (value == 1) true else false });
                    offset += 1;
                },
                .text => {
                    const length = std.mem.readInt(u16, bytes[offset..][0..2], .little);
                    offset += 2;
                    const text = try allocator.alloc(u8, length);
                    @memcpy(text, bytes[offset..][0..length]);
                    try values.append(allocator, Value{ .text = text });
                    offset += length;
                },
            }
        }
        return Tuple{
            .values = try values.toOwnedSlice(allocator),
            .schema = schema,
        };
    }

    pub fn deinit(self: *Tuple, allocator: Allocator) void {
        for (self.values) |*value| {
            switch (value.*) {
                .text => |text| {
                    allocator.free(text);
                },
                else => {},
            }
        }
        allocator.free(self.values);
    }

    fn calculateTotalSize(self: *const Tuple) usize {
        const bitmap_size = (self.values.len + 7) / 8;
        var total_size: usize = bitmap_size;
        for (self.values) |*value| {
            switch (value.*) {
                .integer => {
                    total_size += 8;
                },
                .text => {
                    total_size += 2;
                    total_size += value.text.len;
                },
                .boolean => {
                    total_size += 1;
                },
                .null_value => {},
            }
        }
        return total_size;
    }
};

// ============ Tests ============

test "serialize and deserialize simple tuple (INT, TEXT, BOOL)" {
    const allocator = std.testing.allocator;

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
            .{ .name = "active", .data_type = .boolean, .nullable = false },
        },
    };

    const tuple = Tuple{
        .values = &[_]Value{
            .{ .integer = 42 },
            .{ .text = "alice" },
            .{ .boolean = true },
        },
        .schema = schema,
    };

    const bytes = try tuple.serialize(allocator);
    defer allocator.free(bytes);

    var deserialized = try Tuple.deserialize(bytes, schema, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 42), deserialized.values[0].integer);
    try std.testing.expectEqualStrings("alice", deserialized.values[1].text);
    try std.testing.expectEqual(true, deserialized.values[2].boolean);
}

test "serialize and deserialize tuple with NULL values" {
    const allocator = std.testing.allocator;

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = true },
            .{ .name = "age", .data_type = .integer, .nullable = true },
        },
    };

    const tuple = Tuple{
        .values = &[_]Value{
            .{ .integer = 1 },
            .{ .null_value = {} },
            .{ .null_value = {} },
        },
        .schema = schema,
    };

    const bytes = try tuple.serialize(allocator);
    defer allocator.free(bytes);

    var deserialized = try Tuple.deserialize(bytes, schema, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), deserialized.values[0].integer);
    try std.testing.expectEqual(Value.null_value, std.meta.activeTag(deserialized.values[1]));
    try std.testing.expectEqual(Value.null_value, std.meta.activeTag(deserialized.values[2]));
}

test "serialize and deserialize tuple with empty string" {
    const allocator = std.testing.allocator;

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "data", .data_type = .text, .nullable = false },
        },
    };

    const tuple = Tuple{
        .values = &[_]Value{
            .{ .text = "" },
        },
        .schema = schema,
    };

    const bytes = try tuple.serialize(allocator);
    defer allocator.free(bytes);

    var deserialized = try Tuple.deserialize(bytes, schema, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqualStrings("", deserialized.values[0].text);
}

test "serialize and deserialize tuple with long string" {
    const allocator = std.testing.allocator;

    const long_text = "a" ** 1000; // 1000 character string

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "content", .data_type = .text, .nullable = false },
        },
    };

    const tuple = Tuple{
        .values = &[_]Value{
            .{ .text = long_text },
        },
        .schema = schema,
    };

    const bytes = try tuple.serialize(allocator);
    defer allocator.free(bytes);

    var deserialized = try Tuple.deserialize(bytes, schema, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1000), deserialized.values[0].text.len);
    try std.testing.expectEqualStrings(long_text, deserialized.values[0].text);
}

test "serialize and deserialize tuple with negative integer" {
    const allocator = std.testing.allocator;

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "value", .data_type = .integer, .nullable = false },
        },
    };

    const tuple = Tuple{
        .values = &[_]Value{
            .{ .integer = -12345 },
        },
        .schema = schema,
    };

    const bytes = try tuple.serialize(allocator);
    defer allocator.free(bytes);

    var deserialized = try Tuple.deserialize(bytes, schema, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(i64, -12345), deserialized.values[0].integer);
}

test "serialize and deserialize tuple with false boolean" {
    const allocator = std.testing.allocator;

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "flag", .data_type = .boolean, .nullable = false },
        },
    };

    const tuple = Tuple{
        .values = &[_]Value{
            .{ .boolean = false },
        },
        .schema = schema,
    };

    const bytes = try tuple.serialize(allocator);
    defer allocator.free(bytes);

    var deserialized = try Tuple.deserialize(bytes, schema, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(false, deserialized.values[0].boolean);
}

test "serialize and deserialize tuple with all NULL values" {
    const allocator = std.testing.allocator;

    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "a", .data_type = .integer, .nullable = true },
            .{ .name = "b", .data_type = .text, .nullable = true },
            .{ .name = "c", .data_type = .boolean, .nullable = true },
        },
    };

    const tuple = Tuple{
        .values = &[_]Value{
            .{ .null_value = {} },
            .{ .null_value = {} },
            .{ .null_value = {} },
        },
        .schema = schema,
    };

    const bytes = try tuple.serialize(allocator);
    defer allocator.free(bytes);

    var deserialized = try Tuple.deserialize(bytes, schema, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(Value.null_value, std.meta.activeTag(deserialized.values[0]));
    try std.testing.expectEqual(Value.null_value, std.meta.activeTag(deserialized.values[1]));
    try std.testing.expectEqual(Value.null_value, std.meta.activeTag(deserialized.values[2]));
}

test "schema findColumnDef returns column definition" {
    const schema = Schema{
        .columns = &[_]ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = true },
        },
    };

    const id_col = schema.findColumnDef("id");
    try std.testing.expect(id_col != null);
    try std.testing.expectEqual(DataType.integer, id_col.?.data_type);
    try std.testing.expectEqual(false, id_col.?.nullable);

    const name_col = schema.findColumnDef("name");
    try std.testing.expect(name_col != null);
    try std.testing.expectEqual(DataType.text, name_col.?.data_type);
    try std.testing.expectEqual(true, name_col.?.nullable);

    const missing = schema.findColumnDef("nonexistent");
    try std.testing.expect(missing == null);
}
