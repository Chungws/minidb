const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("../sql/ast.zig");
const Value = ast.Value;
const slot = @import("slot.zig");
const SlottedPage = slot.SlottedPage;
const Page = @import("../storage/page.zig").Page;
const tuple = @import("tuple.zig");
const Tuple = tuple.Tuple;

pub const RID = struct {
    page_id: u16,
    slot_id: u16,
};

pub const HeapIterator = struct {
    heap: *const HeapFile,
    page_id: u16 = 0,
    slot_id: u16 = 0,

    pub fn next(self: *HeapIterator) ?struct { rid: RID, data: []const u8 } {
        while (self.page_id < self.heap.pages.items.len) {
            const page = &self.heap.pages.items[self.page_id];
            const num_slots = page.numSlotArea();
            while (self.slot_id < num_slots) {
                const record = page.get(self.slot_id);
                if (record) |r| {
                    const rid = RID{
                        .page_id = self.page_id,
                        .slot_id = self.slot_id,
                    };
                    self.slot_id += 1;
                    return .{
                        .rid = rid,
                        .data = r,
                    };
                } else {
                    self.slot_id += 1;
                }
            }
            self.page_id += 1;
            self.slot_id = 0;
        }
        return null;
    }
};

pub const HeapFile = struct {
    pages: std.ArrayList(SlottedPage),
    allocator: Allocator,

    pub fn init(allocator: Allocator) !HeapFile {
        var pages = std.ArrayList(SlottedPage).empty;
        try pages.append(allocator, SlottedPage.init(Page.init()));
        return HeapFile{
            .pages = pages,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *HeapFile) void {
        self.pages.deinit(self.allocator);
    }

    pub fn insert(self: *HeapFile, t: *const Tuple) !RID {
        const data = try t.serialize(self.allocator);
        defer self.allocator.free(data);

        for (self.pages.items, 0..) |*page, i| {
            const slot_id = page.insert(data) catch |err| switch (err) {
                error.NotEnoughFreeSpace => continue,
            };
            return RID{
                .page_id = @intCast(i),
                .slot_id = slot_id,
            };
        }

        try self.pages.append(self.allocator, SlottedPage.init(Page.init()));
        const page_id = self.pages.items.len - 1;
        const slot_id = try self.pages.items[page_id].insert(data);
        return RID{
            .page_id = @intCast(page_id),
            .slot_id = slot_id,
        };
    }

    pub fn get(self: *const HeapFile, rid: RID) ?[]const u8 {
        if (self.pages.items.len <= rid.page_id) {
            return null;
        }
        return self.pages.items[rid.page_id].get(rid.slot_id);
    }

    pub fn delete(self: *HeapFile, rid: RID) void {
        if (self.pages.items.len <= rid.page_id) {
            return;
        }
        self.pages.items[rid.page_id].delete(rid.slot_id);
    }

    pub fn scan(self: *const HeapFile) HeapIterator {
        return HeapIterator{ .heap = self };
    }
};

// ============ Tests ============
const Schema = tuple.Schema;

test "heap file init creates one page" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    try std.testing.expectEqual(@as(usize, 1), heap.pages.items.len);
}

test "heap file insert and get" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    const t = Tuple{
        .values = &[_]Value{
            .{ .integer = 42 },
            .{ .text = "hello" },
        },
        .schema = Schema{
            .columns = &[_]ast.ColumnDef{
                .{
                    .data_type = .integer,
                    .name = "number",
                    .nullable = false,
                },
                .{
                    .data_type = .text,
                    .name = "string",
                    .nullable = false,
                },
            },
        },
    };

    const rid = try heap.insert(&t);
    const data = heap.get(rid);

    try std.testing.expect(data != null);
}

test "heap file delete" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    const t = Tuple{
        .values = &[_]Value{
            .{ .integer = 1 },
        },
        .schema = Schema{
            .columns = &[_]ast.ColumnDef{
                .{
                    .data_type = .integer,
                    .name = "number",
                    .nullable = false,
                },
            },
        },
    };

    const rid = try heap.insert(&t);
    try std.testing.expect(heap.get(rid) != null);

    heap.delete(rid);
    try std.testing.expect(heap.get(rid) == null);
}

test "heap file get non-existent returns null" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    const rid = RID{ .page_id = 99, .slot_id = 0 };
    try std.testing.expect(heap.get(rid) == null);
}

test "heap file insert multiple tuples" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();
    const schema = Schema{
        .columns = &[_]ast.ColumnDef{
            .{
                .data_type = .integer,
                .name = "number",
                .nullable = false,
            },
        },
    };

    const t1 = Tuple{
        .values = &[_]Value{.{ .integer = 1 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]Value{.{ .integer = 2 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]Value{.{ .integer = 3 }},
        .schema = schema,
    };

    const rid1 = try heap.insert(&t1);
    const rid2 = try heap.insert(&t2);
    const rid3 = try heap.insert(&t3);

    // All should be on page 0 with different slots
    try std.testing.expectEqual(@as(u16, 0), rid1.page_id);
    try std.testing.expectEqual(@as(u16, 0), rid2.page_id);
    try std.testing.expectEqual(@as(u16, 0), rid3.page_id);
    try std.testing.expectEqual(@as(u16, 0), rid1.slot_id);
    try std.testing.expectEqual(@as(u16, 1), rid2.slot_id);
    try std.testing.expectEqual(@as(u16, 2), rid3.slot_id);

    // All should be retrievable
    try std.testing.expect(heap.get(rid1) != null);
    try std.testing.expect(heap.get(rid2) != null);
    try std.testing.expect(heap.get(rid3) != null);
}

test "heap file creates new page when full" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    const schema = tuple.Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "text", .data_type = .text, .nullable = false },
        },
    };

    // Create a large tuple that takes most of a page
    const large_text = "x" ** 2000;
    const large_tuple = Tuple{
        .values = &[_]Value{.{ .text = large_text }},
        .schema = schema,
    };

    // First insert goes to page 0
    const rid1 = try heap.insert(&large_tuple);
    try std.testing.expectEqual(@as(u16, 0), rid1.page_id);
    try std.testing.expectEqual(@as(usize, 1), heap.pages.items.len);

    // Second insert also goes to page 0 (still fits)
    const rid2 = try heap.insert(&large_tuple);
    try std.testing.expectEqual(@as(u16, 0), rid2.page_id);

    // Third insert should create page 1
    const rid3 = try heap.insert(&large_tuple);
    try std.testing.expectEqual(@as(u16, 1), rid3.page_id);
    try std.testing.expectEqual(@as(usize, 2), heap.pages.items.len);
}

test "heap file get and deserialize tuple" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();
    const schema = tuple.Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
            .{ .name = "name", .data_type = .text, .nullable = false },
            .{ .name = "active", .data_type = .boolean, .nullable = false },
        },
    };

    const original = Tuple{
        .values = &[_]Value{
            .{ .integer = 42 },
            .{ .text = "hello" },
            .{ .boolean = true },
        },
        .schema = schema,
    };

    const rid = try heap.insert(&original);
    const data = heap.get(rid).?;

    // Deserialize and verify

    var deserialized = try Tuple.deserialize(data, schema, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 42), deserialized.values[0].integer);
    try std.testing.expectEqualStrings("hello", deserialized.values[1].text);
    try std.testing.expectEqual(true, deserialized.values[2].boolean);
}

test "heap file delete from non-existent page does nothing" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    // Should not crash
    heap.delete(RID{ .page_id = 99, .slot_id = 0 });
    try std.testing.expectEqual(@as(usize, 1), heap.pages.items.len);
}

test "heap file RID stability after other deletes" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    const schema = tuple.Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    const t1 = Tuple{
        .values = &[_]Value{.{ .integer = 100 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]Value{.{ .integer = 200 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]Value{.{ .integer = 300 }},
        .schema = schema,
    };

    const rid1 = try heap.insert(&t1);
    const rid2 = try heap.insert(&t2);
    const rid3 = try heap.insert(&t3);

    // Delete middle tuple
    heap.delete(rid2);

    // rid1 and rid3 should still work
    try std.testing.expect(heap.get(rid1) != null);
    try std.testing.expect(heap.get(rid2) == null);
    try std.testing.expect(heap.get(rid3) != null);
}

test "heap iterator scans all tuples" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    const schema = tuple.Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    const t1 = Tuple{
        .values = &[_]Value{.{ .integer = 1 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]Value{.{ .integer = 2 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]Value{.{ .integer = 3 }},
        .schema = schema,
    };

    _ = try heap.insert(&t1);
    _ = try heap.insert(&t2);
    _ = try heap.insert(&t3);

    var iter = HeapIterator{ .heap = &heap };
    var count: u32 = 0;
    while (iter.next()) |_| {
        count += 1;
    }

    try std.testing.expectEqual(@as(u32, 3), count);
}

test "heap iterator skips deleted tuples" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    const schema = tuple.Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };

    const t1 = Tuple{
        .values = &[_]Value{.{ .integer = 1 }},
        .schema = schema,
    };
    const t2 = Tuple{
        .values = &[_]Value{.{ .integer = 2 }},
        .schema = schema,
    };
    const t3 = Tuple{
        .values = &[_]Value{.{ .integer = 3 }},
        .schema = schema,
    };

    _ = try heap.insert(&t1);
    const rid2 = try heap.insert(&t2);
    _ = try heap.insert(&t3);

    heap.delete(rid2);

    var iter = HeapIterator{ .heap = &heap };
    var count: u32 = 0;
    while (iter.next()) |_| {
        count += 1;
    }

    try std.testing.expectEqual(@as(u32, 2), count);
}

test "heap iterator returns correct RIDs" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    const schema = tuple.Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    const t = Tuple{
        .values = &[_]Value{.{ .integer = 42 }},
        .schema = schema,
    };

    const rid1 = try heap.insert(&t);
    const rid2 = try heap.insert(&t);

    var iter = HeapIterator{ .heap = &heap };

    const first = iter.next().?;
    try std.testing.expectEqual(rid1.page_id, first.rid.page_id);
    try std.testing.expectEqual(rid1.slot_id, first.rid.slot_id);

    const second = iter.next().?;
    try std.testing.expectEqual(rid2.page_id, second.rid.page_id);
    try std.testing.expectEqual(rid2.slot_id, second.rid.slot_id);

    try std.testing.expect(iter.next() == null);
}

test "heap iterator empty heap returns null" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    var iter = HeapIterator{ .heap = &heap };
    try std.testing.expect(iter.next() == null);
}

test "insert reuses space in earlier pages after delete" {
    const allocator = std.testing.allocator;
    var heap = try HeapFile.init(allocator);
    defer heap.deinit();

    const schema = tuple.Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "text", .data_type = .text, .nullable = false },
        },
    };
    // Fill page 0 with large tuples
    const large_text = "x" ** 2000;
    const large_tuple = Tuple{
        .values = &[_]Value{.{ .text = large_text }},
        .schema = schema,
    };

    const rid1 = try heap.insert(&large_tuple); // page 0
    _ = try heap.insert(&large_tuple); // page 0
    _ = try heap.insert(&large_tuple); // page 1 (page 0 full)

    try std.testing.expectEqual(@as(usize, 2), heap.pages.items.len);

    // Delete from page 0
    heap.delete(rid1);

    // New insert should go to page 0, not page 2
    const new_rid = try heap.insert(&large_tuple);
    try std.testing.expectEqual(@as(u16, 0), new_rid.page_id);
    try std.testing.expectEqual(@as(usize, 2), heap.pages.items.len); // no new page
}
