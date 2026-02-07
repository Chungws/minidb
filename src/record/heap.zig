const std = @import("std");
const Allocator = std.mem.Allocator;

const ast = @import("../sql/ast.zig");
const Value = ast.Value;
const slot = @import("slot.zig");
const SlottedPage = slot.SlottedPage;
const Page = @import("../storage/page.zig").Page;
const BufferPool = @import("../storage/buffer.zig").BufferPool;
const DiskManager = @import("../storage/disk.zig").DiskManager;
const tuple = @import("tuple.zig");
const Tuple = tuple.Tuple;
const LockManager = @import("../tx/lock.zig").LockManager;

pub const RID = struct {
    page_id: u16,
    slot_id: u16,
};

pub const HeapIterator = struct {
    heap: *const HeapFile,
    page_id: usize = 0,
    slot_id: u16 = 0,
    current_page: ?*Page = null,

    pub fn next(self: *HeapIterator) !?struct { rid: RID, data: []const u8 } {
        while (self.page_id < self.heap.page_count) {
            if (self.current_page == null) {
                self.current_page = try self.heap.buffer_pool.fetchPage(self.page_id);
            }

            const spage = SlottedPage.fromPage(self.current_page.?);
            const num_slots = spage.numSlotArea();
            while (self.slot_id < num_slots) {
                const record = spage.get(self.slot_id);
                if (record) |r| {
                    const rid = RID{
                        .page_id = @intCast(self.page_id),
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
            self.heap.buffer_pool.unpinPage(self.page_id, false);
            self.current_page = null;
            self.page_id += 1;
            self.slot_id = 0;
        }
        return null;
    }

    pub fn deinit(self: *HeapIterator) void {
        if (self.current_page != null) {
            self.heap.buffer_pool.unpinPage(self.page_id, false);
        }
    }
};

pub const HeapFile = struct {
    buffer_pool: *BufferPool,
    page_count: usize,
    lock_mgr: *LockManager,
    current_tx: ?u64,
    allocator: Allocator,

    pub fn init(allocator: Allocator, buffer_pool: *BufferPool, lock: *LockManager) !HeapFile {
        const page = try buffer_pool.fetchPage(0);
        _ = SlottedPage.init(page);
        buffer_pool.unpinPage(0, true);

        return HeapFile{
            .buffer_pool = buffer_pool,
            .page_count = 1,
            .lock_mgr = lock,
            .current_tx = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *HeapFile) void {
        _ = self;
    }

    pub fn insert(self: *HeapFile, t: *const Tuple) !RID {
        const data = try t.serialize(self.allocator);
        defer self.allocator.free(data);

        var rid: RID = undefined;
        var found = false;

        for (0..self.page_count) |page_id| {
            const page = try self.buffer_pool.fetchPage(page_id);
            var spage = SlottedPage.fromPage(page);
            const slot_id = spage.insert(data) catch {
                self.buffer_pool.unpinPage(page_id, true);
                continue;
            };

            self.buffer_pool.unpinPage(page_id, true);
            rid = RID{
                .page_id = @intCast(page_id),
                .slot_id = slot_id,
            };
            found = true;
            break;
        }

        if (!found) {
            const new_page_id = self.page_count;
            const page = try self.buffer_pool.fetchPage(new_page_id);
            var spage = SlottedPage.init(page);
            const slot_id = try spage.insert(data);
            self.buffer_pool.unpinPage(new_page_id, true);
            self.page_count += 1;

            rid = RID{
                .page_id = @intCast(new_page_id),
                .slot_id = slot_id,
            };
        }

        if (self.current_tx) |txn_id| {
            try self.lock_mgr.acquireLock(txn_id, rid, .exclusive);
        }

        return rid;
    }

    pub fn get(self: *const HeapFile, rid: RID) !?[]const u8 {
        if (rid.page_id >= self.page_count) {
            return null;
        }

        if (self.current_tx) |txn_id| {
            self.lock_mgr.acquireLock(txn_id, rid, .shared) catch {};
        }

        const page = try self.buffer_pool.fetchPage(rid.page_id);
        defer self.buffer_pool.unpinPage(rid.page_id, false);

        const spage = SlottedPage.fromPage(page);
        return spage.get(rid.slot_id);
    }

    pub fn delete(self: *HeapFile, rid: RID) !void {
        if (rid.page_id >= self.page_count) {
            return;
        }

        const page = try self.buffer_pool.fetchPage(rid.page_id);
        defer self.buffer_pool.unpinPage(rid.page_id, true);

        var spage = SlottedPage.fromPage(page);
        spage.delete(rid.slot_id);
    }

    pub fn scan(self: *const HeapFile) HeapIterator {
        return HeapIterator{ .heap = self };
    }

    pub fn setCurrentTxn(self: *HeapFile, txn_id: u64) void {
        self.current_tx = txn_id;
    }

    pub fn getCurrentTxn(self: *const HeapFile) u64 {
        return self.current_tx;
    }

    pub fn clearCurrentTxn(self: *HeapFile) void {
        self.current_tx = null;
    }
};

// ============ Tests ============
const Schema = tuple.Schema;

test "heap file init creates one page" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_1.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
    defer heap.deinit();

    try std.testing.expectEqual(@as(usize, 1), heap.page_count);
}

test "heap file insert and get" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_2.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
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
    const data = try heap.get(rid);

    try std.testing.expect(data != null);
}

test "heap file delete" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_3.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
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
    try std.testing.expect((try heap.get(rid)) != null);

    try heap.delete(rid);
    try std.testing.expect((try heap.get(rid)) == null);
}

test "heap file get non-existent returns null" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_4.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
    defer heap.deinit();

    const rid = RID{ .page_id = 99, .slot_id = 0 };
    try std.testing.expect((try heap.get(rid)) == null);
}

test "heap file insert multiple tuples" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_5.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
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
    try std.testing.expect((try heap.get(rid1)) != null);
    try std.testing.expect((try heap.get(rid2)) != null);
    try std.testing.expect((try heap.get(rid3)) != null);
}

test "heap file creates new page when full" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_6.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
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
    try std.testing.expectEqual(@as(usize, 1), heap.page_count);

    // Second insert also goes to page 0 (still fits)
    const rid2 = try heap.insert(&large_tuple);
    try std.testing.expectEqual(@as(u16, 0), rid2.page_id);

    // Third insert should create page 1
    const rid3 = try heap.insert(&large_tuple);
    try std.testing.expectEqual(@as(u16, 1), rid3.page_id);
    try std.testing.expectEqual(@as(usize, 2), heap.page_count);
}

test "heap file get and deserialize tuple" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_7.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
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
    const data = (try heap.get(rid)).?;

    // Deserialize and verify

    var deserialized = try Tuple.deserialize(data, schema, allocator);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 42), deserialized.values[0].integer);
    try std.testing.expectEqualStrings("hello", deserialized.values[1].text);
    try std.testing.expectEqual(true, deserialized.values[2].boolean);
}

test "heap file delete from non-existent page does nothing" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_8.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
    defer heap.deinit();

    // Should not crash
    try heap.delete(RID{ .page_id = 99, .slot_id = 0 });
    try std.testing.expectEqual(@as(usize, 1), heap.page_count);
}

test "heap file RID stability after other deletes" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_9.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
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
    try heap.delete(rid2);

    // rid1 and rid3 should still work
    try std.testing.expect((try heap.get(rid1)) != null);
    try std.testing.expect((try heap.get(rid2)) == null);
    try std.testing.expect((try heap.get(rid3)) != null);
}

test "heap iterator scans all tuples" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_10.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
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

    var iter = heap.scan();
    var count: u32 = 0;
    while (try iter.next()) |_| {
        count += 1;
    }

    try std.testing.expectEqual(@as(u32, 3), count);
}

test "heap iterator skips deleted tuples" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_11.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
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

    try heap.delete(rid2);

    var iter = heap.scan();
    var count: u32 = 0;
    while (try iter.next()) |_| {
        count += 1;
    }

    try std.testing.expectEqual(@as(u32, 2), count);
}

test "heap iterator returns correct RIDs" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_12.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
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

    var iter = heap.scan();

    const first = (try iter.next()).?;
    try std.testing.expectEqual(rid1.page_id, first.rid.page_id);
    try std.testing.expectEqual(rid1.slot_id, first.rid.slot_id);

    const second = (try iter.next()).?;
    try std.testing.expectEqual(rid2.page_id, second.rid.page_id);
    try std.testing.expectEqual(rid2.slot_id, second.rid.slot_id);

    try std.testing.expect((try iter.next()) == null);
}

test "heap iterator empty heap returns null" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_13.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
    defer heap.deinit();

    var iter = heap.scan();
    try std.testing.expect((try iter.next()) == null);
}

test "insert reuses space in earlier pages after delete" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_14.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
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

    try std.testing.expectEqual(@as(usize, 2), heap.page_count);

    // Delete from page 0
    try heap.delete(rid1);

    // New insert should go to page 0, not page 2
    const new_rid = try heap.insert(&large_tuple);
    try std.testing.expectEqual(@as(u16, 0), new_rid.page_id);
    try std.testing.expectEqual(@as(usize, 2), heap.page_count); // no new page
}

// ============ Lock Integration Tests ============

test "heap get acquires shared lock when txn active" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_15.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
    defer heap.deinit();

    const schema = tuple.Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    const t = Tuple{
        .values = &[_]Value{.{ .integer = 1 }},
        .schema = schema,
    };

    const rid = try heap.insert(&t);

    // Set active transaction and read
    heap.setCurrentTxn(1);
    _ = try heap.get(rid);

    // Lock should be held - another exclusive lock should fail
    const result = lock_mgr.acquireLock(2, rid, .exclusive);
    try std.testing.expectError(error.LockConflict, result);
}

test "heap insert acquires exclusive lock when txn active" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_16.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
    defer heap.deinit();

    const schema = tuple.Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    const t = Tuple{
        .values = &[_]Value{.{ .integer = 1 }},
        .schema = schema,
    };

    // Set active transaction and insert
    heap.setCurrentTxn(1);
    const rid = try heap.insert(&t);

    // Lock should be held - another shared lock should fail
    const result = lock_mgr.acquireLock(2, rid, .shared);
    try std.testing.expectError(error.LockConflict, result);
}

test "heap operations without txn do not acquire locks" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_17.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
    defer heap.deinit();

    const schema = tuple.Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    const t = Tuple{
        .values = &[_]Value{.{ .integer = 1 }},
        .schema = schema,
    };

    // No active transaction
    const rid = try heap.insert(&t);
    _ = try heap.get(rid);

    // No locks held - another txn can acquire exclusive lock
    try lock_mgr.acquireLock(2, rid, .exclusive);
}

test "heap clearCurrentTxn stops acquiring locks" {
    const allocator = std.testing.allocator;
    const test_path = "test_heap_18.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var lock_mgr = LockManager.init(allocator);
    defer lock_mgr.deinit();
    var disk_mgr = try DiskManager.init(test_path);
    defer disk_mgr.deinit();
    var buffer_pool = try BufferPool.init(allocator, 10, &disk_mgr);
    defer buffer_pool.deinit();
    var heap = try HeapFile.init(allocator, &buffer_pool, &lock_mgr);
    defer heap.deinit();

    const schema = tuple.Schema{
        .columns = &[_]ast.ColumnDef{
            .{ .name = "id", .data_type = .integer, .nullable = false },
        },
    };
    const t = Tuple{
        .values = &[_]Value{.{ .integer = 1 }},
        .schema = schema,
    };

    heap.setCurrentTxn(1);
    const rid1 = try heap.insert(&t);

    // Clear transaction
    heap.clearCurrentTxn();

    // New insert should not acquire lock
    const rid2 = try heap.insert(&t);

    // rid1 still locked, rid2 not locked
    try std.testing.expectError(error.LockConflict, lock_mgr.acquireLock(2, rid1, .exclusive));
    try lock_mgr.acquireLock(2, rid2, .exclusive);
}
