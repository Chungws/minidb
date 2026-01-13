// const Page = @import("src/lib.zig");
const std = @import("std");

const SlottledPageError = error{
    NotEnoughFreeSpace,
};

pub const HEADER_SIZE: comptime_int = 6;
pub const SLOT_SIZE: comptime_int = 6;
pub const PAGE_SIZE: comptime_int = 4096;

pub const Page = struct {
    data: [PAGE_SIZE]u8,

    pub fn init() Page {
        return Page{
            .data = [_]u8{0} ** PAGE_SIZE,
        };
    }

    pub fn read(self: *const Page, offset: usize, len: usize) []const u8 {
        return self.data[offset..(offset + len)];
    }

    pub fn write(self: *Page, offset: usize, bytes: []const u8) void {
        @memcpy(self.data[offset..][0..bytes.len], bytes);
    }
};

pub const SlottledPage = struct {
    page: Page,

    pub fn init(page: Page) SlottledPage {
        var spage = SlottledPage{ .page = page };
        spage.writeNumSlots(0);
        spage.writeFreeSpaceStart(HEADER_SIZE);
        spage.writeFreeSpaceEnd(PAGE_SIZE);
        return spage;
    }

    pub fn insert(self: *SlottledPage, bytes: []const u8) !u16 {
        if (bytes.len + SLOT_SIZE > self.freeSpace()) {
            return error.NotEnoughFreeSpace;
        }
        const record_offset = self.readFreeSpaceEnd() - @as(u16, bytes.len);
        self.page.write(record_offset, bytes);
        self.writeFreeSpaceEnd(record_offset);

        const slot_offset = self.findFreeSlot();
        self.writeu16(slot_offset, record_offset);
        self.writeu16(slot_offset + 2, @as(u16, bytes.len));
        if (self.readFreeSpaceStart() == slot_offset) {
            self.writeFreeSpaceStart(slot_offset + SLOT_SIZE);
        }
        self.writeNumSlots(self.readNumSlots() + 1);

        const slot_id = (slot_offset - HEADER_SIZE) / SLOT_SIZE;
        return slot_id;
    }

    pub fn get(self: *const SlottledPage, slot_id: u16) ?[]const u8 {
        const slot_offset = HEADER_SIZE + slot_id * SLOT_SIZE;
        const record_offset = self.readu16(slot_offset);
        const record_length = self.readu16(slot_offset + 2);

        if (record_offset == 0 or record_length == 0) {
            return null;
        }

        return self.page.read(record_offset, record_length);
    }

    pub fn delete(self: *SlottledPage, slot_id: u16) void {
        const slot_offset = HEADER_SIZE + slot_id * SLOT_SIZE;
        self.writeu16(slot_offset, 0);
        self.writeu16(slot_offset + 2, 0);
        const free_start = self.readFreeSpaceStart();
        if (free_start == slot_offset) {
            self.writeFreeSpaceStart(free_start - SLOT_SIZE);
        }
        self.writeNumSlots(self.readNumSlots() - 1);
    }

    pub fn freeSpace(self: *const SlottledPage) u16 {
        return self.readFreeSpaceEnd() - self.readFreeSpaceStart();
    }

    fn findFreeSlot(self: *const SlottledPage) u16 {
        const end = self.readFreeSpaceStart();
        for (0..end) |i| {
            const offset: u16 = HEADER_SIZE + i * SLOT_SIZE;
            const record_offset = self.readu16(@as(u16, offset));

            if (record_offset == 0) {
                return offset;
            }
        }
        return end;
    }

    fn readNumSlots(self: *const SlottledPage) u16 {
        return self.readu16(0);
    }

    fn readFreeSpaceStart(self: *const SlottledPage) u16 {
        return self.readu16(2);
    }

    fn readFreeSpaceEnd(self: *const SlottledPage) u16 {
        return self.readu16(4);
    }

    fn readu16(self: *const SlottledPage, offset: usize) u16 {
        const val = self.page.read(offset, 2);
        const res = std.mem.readInt(u16, val[0..][0..2], .little);
        return res;
    }

    fn writeNumSlots(self: *SlottledPage, value: u16) void {
        self.writeu16(0, value);
    }

    fn writeFreeSpaceStart(self: *SlottledPage, value: u16) void {
        self.writeu16(2, value);
    }

    fn writeFreeSpaceEnd(self: *SlottledPage, value: u16) void {
        self.writeu16(4, value);
    }

    fn writeu16(self: *SlottledPage, offset: usize, value: u16) void {
        var bytes = [_]u8{0} ** 2;
        std.mem.writeInt(u16, bytes[0..][0..2], value, .little);
        self.page.write(offset, bytes[0..][0..2]);
    }
};

// ============ Tests ============

test "test" {
    const spage = SlottledPage.init(Page.init());
    try std.testing.expectEqual(@as(u16, 4096 - 6), spage.freeSpace());
}
