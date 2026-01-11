const std = @import("std");
const Page = @import("page.zig").Page;
const PAGE_SIZE = @import("page.zig").PAGE_SIZE;

pub const DiskManager = struct {
    file: std.fs.File,

    pub fn init(path: []const u8) !DiskManager {
        const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = false });
        return DiskManager{
            .file = file,
        };
    }

    pub fn deinit(self: *DiskManager) void {
        self.file.close();
    }

    pub fn readPage(self: *DiskManager, page_id: usize, page: *Page) !void {
        const offset = page_id * PAGE_SIZE;
        try self.file.seekTo(offset);
        try self.file.readAll(&page.data);
    }

    pub fn writePage(self: *DiskManager, page_id: usize, page: *const Page) !void {
        const offset = page_id * PAGE_SIZE;
        try self.file.seekTo(offset);
        try self.file.writeAll(&page.data);
    }
};

test "write and read page" {
    const test_path = "test_disk.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var dm = try DiskManager.init(test_path);
    defer dm.deinit();

    // write
    var page_out = Page.init();
    page_out.write(0, "hello disk");
    try dm.writePage(0, &page_out);

    // read
    var page_in = Page.init();
    try dm.readPage(0, &page_in);

    try std.testing.expectEqualStrings("hello disk", page_in.read(0, 10));
}

test "write and read multiple pages non-sequential" {
    const test_path = "test_disk2.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var dm = try DiskManager.init(test_path);
    defer dm.deinit();

    // write pages 0, 5, 2 (non-sequential)
    var p0 = Page.init();
    p0.write(0, "page zero");
    try dm.writePage(0, &p0);

    var p5 = Page.init();
    p5.write(0, "page five");
    try dm.writePage(5, &p5);

    var p2 = Page.init();
    p2.write(0, "page two");
    try dm.writePage(2, &p2);

    // read back
    var read_p = Page.init();

    try dm.readPage(0, &read_p);
    try std.testing.expectEqualStrings("page zero", read_p.read(0, 9));

    try dm.readPage(5, &read_p);
    try std.testing.expectEqualStrings("page five", read_p.read(0, 9));

    try dm.readPage(2, &read_p);
    try std.testing.expectEqualStrings("page two", read_p.read(0, 8));
}

test "data persists after reopen" {
    const test_path = "test_disk3.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    // write and close
    {
        var dm = try DiskManager.init(test_path);
        defer dm.deinit();

        var page_out = Page.init();
        page_out.write(0, "persistent");
        try dm.writePage(0, &page_out);
    }

    // reopen and read
    {
        var dm = try DiskManager.init(test_path);
        defer dm.deinit();

        var page_in = Page.init();
        try dm.readPage(0, &page_in);
        try std.testing.expectEqualStrings("persistent", page_in.read(0, 10));
    }
}
