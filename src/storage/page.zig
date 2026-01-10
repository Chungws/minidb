const std = @import("std");

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

test "new page is zeroed" {
    const p = Page.init();
    for (p.data) |byte| {
        try std.testing.expectEqual(@as(u8, 0), byte);
    }
}

test "write then read returns same data" {
    var p = Page.init();
    const data = "hello";
    p.write(0, data);
    const result = p.read(0, data.len);
    try std.testing.expectEqualStrings(data, result);
}

test "multiple writes at different offsets" {
    var p = Page.init();
    p.write(0, "aaa");
    p.write(100, "bbb");
    p.write(200, "ccc");

    try std.testing.expectEqualStrings("aaa", p.read(0, 3));
    try std.testing.expectEqualStrings("bbb", p.read(100, 3));
    try std.testing.expectEqualStrings("ccc", p.read(200, 3));
}
