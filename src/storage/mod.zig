pub const page = @import("page.zig");
pub const disk = @import("disk.zig");
pub const buffer = @import("buffer.zig");

test {
    _ = @import("page.zig");
    _ = @import("disk.zig");
    _ = @import("buffer.zig");
}
