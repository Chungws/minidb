//! MiniDB - A minimal relational database implementation in Zig
//!
//! This is the library root that exports all public modules.

const std = @import("std");

/// MiniDB version
pub const version = "0.1.0";

// Future module exports:
pub const storage = @import("storage/mod.zig");
pub const sql = @import("sql/mod.zig");
pub const record = @import("record/mod.zig");
pub const index = @import("index/mod.zig");
pub const query = @import("query/mod.zig");

test "version is defined" {
    try std.testing.expectEqualStrings("0.1.0", version);
}

test "basic sanity check" {
    const x: i32 = 42;
    try std.testing.expectEqual(@as(i32, 42), x);
}

test {
    _ = @import("storage/mod.zig");
    _ = @import("sql/mod.zig");
    _ = @import("record/mod.zig");
    _ = @import("index/mod.zig");
    _ = @import("query/mod.zig");
}
