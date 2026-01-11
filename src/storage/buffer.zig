const std = @import("std");
const Allocator = std.mem.Allocator;
const Page = @import("page.zig").Page;
const DiskManager = @import("disk.zig").DiskManager;

const BufferPoolError = error{
    NoFreeFrame,
    PageNotFound,
};

const Frame = struct {
    page: Page,
    page_id: ?usize,
    pin_count: usize,
    is_dirty: bool,
};

pub const BufferPool = struct {
    frames: []Frame,
    disk_manager: *DiskManager,
    page_table: std.AutoHashMap(usize, usize),
    allocator: Allocator,

    pub fn init(allocator: Allocator, pool_size: usize, disk_manager: *DiskManager) !BufferPool {
        const frames = try allocator.alloc(Frame, pool_size);
        for (frames) |*frame| {
            frame.* = Frame{
                .page = Page.init(),
                .page_id = null,
                .pin_count = 0,
                .is_dirty = false,
            };
        }
        return BufferPool{
            .frames = frames,
            .disk_manager = disk_manager,
            .page_table = std.AutoHashMap(usize, usize).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BufferPool) void {
        self.allocator.free(self.frames);
        self.page_table.deinit();
    }

    pub fn fetchPage(self: *BufferPool, page_id: usize) !*Page {
        const idx = self.page_table.get(page_id);
        if (idx) |i| {
            self.frames[i].pin_count += 1;
            return &self.frames[i].page;
        }

        for (self.frames, 0..) |*frame, i| {
            if (frame.pin_count != 0) {
                continue;
            }

            try self.evictFrame(frame);

            try self.page_table.put(page_id, i);
            errdefer _ = self.page_table.remove(page_id);

            try self.disk_manager.readPage(page_id, &frame.page);
            frame.pin_count = 1;
            frame.page_id = page_id;
            frame.is_dirty = false;
            return &frame.page;
        }
        return error.NoFreeFrame;
    }

    pub fn unpinPage(self: *BufferPool, page_id: usize, is_dirty: bool) void {
        const idx = self.page_table.get(page_id) orelse return;
        self.frames[idx].pin_count -= 1;
        if (is_dirty) {
            self.frames[idx].is_dirty = is_dirty;
        }
    }

    pub fn flushPage(self: *BufferPool, page_id: usize) !void {
        const idx = self.page_table.get(page_id) orelse return error.PageNotFound;
        try self.disk_manager.writePage(page_id, &self.frames[idx].page);
    }

    fn evictFrame(self: *BufferPool, frame: *Frame) !void {
        const old_page_id = frame.page_id orelse return;
        if (frame.is_dirty) {
            try self.flushPage(old_page_id);
        }
        _ = self.page_table.remove(old_page_id);
    }
};

test "fetch and modify page" {
    const allocator = std.testing.allocator;
    const test_path = "test_buffer1.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var dm = try DiskManager.init(test_path);
    defer dm.deinit();

    var pool = try BufferPool.init(allocator, 3, &dm);
    defer pool.deinit();

    // fetch and modify
    const page = try pool.fetchPage(0);
    page.write(0, "hello buffer");
    pool.unpinPage(0, true);

    // fetch again - should see modification
    const page2 = try pool.fetchPage(0);
    try std.testing.expectEqualStrings("hello buffer", page2.read(0, 12));
    pool.unpinPage(0, false);
}

test "eviction when pool is full" {
    const allocator = std.testing.allocator;
    const test_path = "test_buffer2.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var dm = try DiskManager.init(test_path);
    defer dm.deinit();

    var pool = try BufferPool.init(allocator, 2, &dm); // pool size = 2
    defer pool.deinit();

    // fetch page 0 and 1
    const p0 = try pool.fetchPage(0);
    p0.write(0, "page0");
    pool.unpinPage(0, true);

    const p1 = try pool.fetchPage(1);
    p1.write(0, "page1");
    pool.unpinPage(1, true);

    // fetch page 2 - should evict page 0
    const p2 = try pool.fetchPage(2);
    p2.write(0, "page2");
    pool.unpinPage(2, false);

    // fetch page 0 again - should reload from disk
    const p0_again = try pool.fetchPage(0);
    try std.testing.expectEqualStrings("page0", p0_again.read(0, 5));
    pool.unpinPage(0, false);
}

test "no free frame error when all pinned" {
    const allocator = std.testing.allocator;
    const test_path = "test_buffer3.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var dm = try DiskManager.init(test_path);
    defer dm.deinit();

    var pool = try BufferPool.init(allocator, 2, &dm);
    defer pool.deinit();

    // pin both frames
    _ = try pool.fetchPage(0);
    _ = try pool.fetchPage(1);
    // don't unpin!

    // try to fetch another - should fail
    const result = pool.fetchPage(2);
    try std.testing.expectError(error.NoFreeFrame, result);
}
