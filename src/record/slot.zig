const std = @import("std");

const page = @import("../storage/page.zig");
const Page = page.Page;
const PAGE_SIZE = page.PAGE_SIZE;

const SlottedPageError = error{
    NotEnoughFreeSpace,
};

pub const HEADER_SIZE: comptime_int = 6;
pub const SLOT_SIZE: comptime_int = 4;

pub const SlottedPage = struct {
    page: Page,

    pub fn init(p: Page) SlottedPage {
        var spage = SlottedPage{ .page = p };
        spage.writeNumSlots(0);
        spage.writeFreeSpaceStart(HEADER_SIZE);
        spage.writeFreeSpaceEnd(PAGE_SIZE);
        return spage;
    }

    pub fn insert(self: *SlottedPage, bytes: []const u8) !u16 {
        if (bytes.len + SLOT_SIZE > self.freeSpace()) {
            self.compact();

            if (bytes.len + SLOT_SIZE > self.freeSpace()) {
                return error.NotEnoughFreeSpace;
            }
        }
        const record_offset = self.readFreeSpaceEnd() - @as(u16, @intCast(bytes.len));
        self.page.write(record_offset, bytes);
        self.writeFreeSpaceEnd(record_offset);

        const slot_offset = self.findFreeSlot();
        self.writeu16(slot_offset, record_offset);
        self.writeu16(slot_offset + 2, @as(u16, @intCast(bytes.len)));
        if (self.readFreeSpaceStart() == slot_offset) {
            self.writeFreeSpaceStart(slot_offset + SLOT_SIZE);
        }
        self.writeNumSlots(self.readNumSlots() + 1);

        const slot_id = (slot_offset - HEADER_SIZE) / SLOT_SIZE;
        return slot_id;
    }

    pub fn get(self: *const SlottedPage, slot_id: u16) ?[]const u8 {
        const slot_offset = HEADER_SIZE + slot_id * SLOT_SIZE;
        if (slot_offset >= self.readFreeSpaceStart()) {
            return null;
        }
        const record_offset = self.readu16(slot_offset);
        const record_length = self.readu16(slot_offset + 2);

        if (record_offset == 0) {
            return null;
        }

        return self.page.read(record_offset, record_length);
    }

    pub fn delete(self: *SlottedPage, slot_id: u16) void {
        const slot_offset = HEADER_SIZE + slot_id * SLOT_SIZE;
        if (self.readu16(slot_offset) == 0) {
            return;
        }
        self.writeu16(slot_offset, 0);
        self.writeu16(slot_offset + 2, 0);
        const free_start = self.readFreeSpaceStart();
        if (free_start - SLOT_SIZE == slot_offset) {
            self.writeFreeSpaceStart(free_start - SLOT_SIZE);
        }
        self.writeNumSlots(self.readNumSlots() - 1);
    }

    pub fn compact(self: *SlottedPage) void {
        var new_end: u16 = PAGE_SIZE;
        const num_slots = (self.readFreeSpaceStart() - HEADER_SIZE) / SLOT_SIZE;

        for (0..num_slots) |i| {
            const slot_offset: u16 = @intCast(HEADER_SIZE + i * SLOT_SIZE);
            const record_offset = self.readu16(slot_offset);
            const record_length = self.readu16(slot_offset + 2);

            if (record_offset == 0) continue;

            new_end -= record_length;

            var tmp: [PAGE_SIZE]u8 = undefined;
            const data = self.page.read(record_offset, record_length);
            @memcpy(tmp[0..record_length], data);
            self.page.write(new_end, tmp[0..record_length]);

            self.writeu16(slot_offset, new_end);
        }

        self.writeFreeSpaceEnd(new_end);
    }

    pub fn freeSpace(self: *const SlottedPage) u16 {
        return self.readFreeSpaceEnd() - self.readFreeSpaceStart();
    }

    fn findFreeSlot(self: *const SlottedPage) u16 {
        const end = self.readFreeSpaceStart();
        const num_slots = (end - HEADER_SIZE) / SLOT_SIZE;
        for (0..num_slots) |i| {
            const offset: u16 = @intCast(HEADER_SIZE + i * SLOT_SIZE);
            const record_offset = self.readu16(@as(u16, offset));

            if (record_offset == 0) {
                return offset;
            }
        }
        return end;
    }

    fn readNumSlots(self: *const SlottedPage) u16 {
        return self.readu16(0);
    }

    fn readFreeSpaceStart(self: *const SlottedPage) u16 {
        return self.readu16(2);
    }

    fn readFreeSpaceEnd(self: *const SlottedPage) u16 {
        return self.readu16(4);
    }

    fn readu16(self: *const SlottedPage, offset: usize) u16 {
        const val = self.page.read(offset, 2);
        const res = std.mem.readInt(u16, val[0..][0..2], .little);
        return res;
    }

    fn writeNumSlots(self: *SlottedPage, value: u16) void {
        self.writeu16(0, value);
    }

    fn writeFreeSpaceStart(self: *SlottedPage, value: u16) void {
        self.writeu16(2, value);
    }

    fn writeFreeSpaceEnd(self: *SlottedPage, value: u16) void {
        self.writeu16(4, value);
    }

    fn writeu16(self: *SlottedPage, offset: usize, value: u16) void {
        var bytes = [_]u8{0} ** 2;
        std.mem.writeInt(u16, bytes[0..][0..2], value, .little);
        self.page.write(offset, bytes[0..][0..2]);
    }
};

// ============ Tests ============

test "init page has correct free space" {
    const spage = SlottedPage.init(Page.init());
    try std.testing.expectEqual(@as(u16, PAGE_SIZE - HEADER_SIZE), spage.freeSpace());
}

test "insert single record" {
    var spage = SlottedPage.init(Page.init());
    const data = "hello world";

    const slot_id = try spage.insert(data);

    try std.testing.expectEqual(@as(u16, 0), slot_id);
    const retrieved = spage.get(slot_id);
    try std.testing.expect(retrieved != null);
    try std.testing.expectEqualStrings(data, retrieved.?);
}

test "insert multiple records" {
    var spage = SlottedPage.init(Page.init());

    const slot0 = try spage.insert("first");
    const slot1 = try spage.insert("second");
    const slot2 = try spage.insert("third");

    try std.testing.expectEqual(@as(u16, 0), slot0);
    try std.testing.expectEqual(@as(u16, 1), slot1);
    try std.testing.expectEqual(@as(u16, 2), slot2);

    try std.testing.expectEqualStrings("first", spage.get(slot0).?);
    try std.testing.expectEqualStrings("second", spage.get(slot1).?);
    try std.testing.expectEqualStrings("third", spage.get(slot2).?);
}

test "delete record returns null on get" {
    var spage = SlottedPage.init(Page.init());

    const slot_id = try spage.insert("to be deleted");
    try std.testing.expect(spage.get(slot_id) != null);

    spage.delete(slot_id);
    try std.testing.expect(spage.get(slot_id) == null);
}

test "get non-existent slot returns null" {
    const spage = SlottedPage.init(Page.init());
    try std.testing.expect(spage.get(99) == null);
}

test "insert fails when not enough space" {
    var spage = SlottedPage.init(Page.init());

    // Fill most of the page
    const large_data = [_]u8{'x'} ** 4000;
    _ = try spage.insert(&large_data);

    // Try to insert more than remaining space
    const result = spage.insert(&large_data);
    try std.testing.expectError(error.NotEnoughFreeSpace, result);
}

test "free space decreases after insert" {
    var spage = SlottedPage.init(Page.init());
    const initial_free = spage.freeSpace();

    const data = "test data";
    _ = try spage.insert(data);

    const after_insert = spage.freeSpace();
    try std.testing.expect(after_insert < initial_free);
    // Should decrease by data.len + SLOT_SIZE
    try std.testing.expectEqual(initial_free - data.len - SLOT_SIZE, after_insert);
}

test "reuse deleted slot" {
    var spage = SlottedPage.init(Page.init());

    const slot0 = try spage.insert("first");
    _ = try spage.insert("second");

    spage.delete(slot0);

    // New insert should reuse slot 0
    const new_slot = try spage.insert("reused");
    try std.testing.expectEqual(@as(u16, 0), new_slot);
    try std.testing.expectEqualStrings("reused", spage.get(new_slot).?);
}

test "compact reclaims deleted record space" {
    var spage = SlottedPage.init(Page.init());

    // Insert three records
    const slot0 = try spage.insert("aaaa"); // 4 bytes
    const slot1 = try spage.insert("bbbbbbbb"); // 8 bytes
    const slot2 = try spage.insert("cccc"); // 4 bytes

    const free_before_delete = spage.freeSpace();

    // Delete middle record (8 bytes)
    spage.delete(slot1);

    // freeSpace doesn't increase after delete (middle slot - fragmented)
    const free_after_delete = spage.freeSpace();
    try std.testing.expectEqual(free_before_delete, free_after_delete);

    // Compact should reclaim the 8 bytes
    spage.compact();

    const free_after_compact = spage.freeSpace();
    try std.testing.expectEqual(free_after_delete + 8, free_after_compact);

    // Remaining records should still be accessible
    try std.testing.expectEqualStrings("aaaa", spage.get(slot0).?);
    try std.testing.expect(spage.get(slot1) == null); // deleted
    try std.testing.expectEqualStrings("cccc", spage.get(slot2).?);
}

test "compact with all records deleted" {
    var spage = SlottedPage.init(Page.init());

    const slot0 = try spage.insert("test1");
    const slot1 = try spage.insert("test2");

    // Delete in reverse order so freeSpaceStart shrinks properly
    spage.delete(slot1);
    spage.delete(slot0);

    spage.compact();

    // Should have maximum free space (minus header)
    try std.testing.expectEqual(@as(u16, PAGE_SIZE - HEADER_SIZE), spage.freeSpace());
}

test "insert triggers auto compact when fragmented" {
    var spage = SlottedPage.init(Page.init());

    // Fill page with records
    const large_data = [_]u8{'x'} ** 2000;
    _ = try spage.insert(&large_data); // slot0
    const slot1 = try spage.insert(&large_data);

    // Delete first record (creates 2000 bytes of fragmented space)
    spage.delete(0);

    // freeSpace is small (no contiguous space)
    const free_before = spage.freeSpace();
    try std.testing.expect(free_before < 2000);

    // This insert should trigger compact and succeed
    // Note: slot2 will reuse slot0's slot (findFreeSlot returns 0)
    const slot2 = try spage.insert(&large_data);
    try std.testing.expectEqual(@as(u16, 0), slot2); // reuses slot 0

    // Verify records are accessible
    try std.testing.expect(spage.get(slot1) != null);
    try std.testing.expect(spage.get(slot2) != null);
}

test "get with out of bounds slot_id returns null" {
    var spage = SlottedPage.init(Page.init());
    _ = try spage.insert("test");

    // slot_id 0 exists, but 1 does not
    try std.testing.expect(spage.get(0) != null);
    try std.testing.expect(spage.get(1) == null);
    try std.testing.expect(spage.get(100) == null);
}

test "insert and get empty record" {
    var spage = SlottedPage.init(Page.init());

    const slot_id = try spage.insert("");

    try std.testing.expectEqual(@as(u16, 0), slot_id);
    const retrieved = spage.get(slot_id);
    try std.testing.expect(retrieved != null);
    try std.testing.expectEqual(@as(usize, 0), retrieved.?.len);
}
