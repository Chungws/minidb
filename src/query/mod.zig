pub const executor = @import("executor.zig");
pub const catalog = @import("catalog.zig");
pub const planner = @import("planner.zig");

test {
    _ = @import("executor.zig");
    _ = @import("catalog.zig");
    _ = @import("planner.zig");
}
