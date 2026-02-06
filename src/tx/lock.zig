const std = @import("std");
const Allocator = std.mem.Allocator;

const RID = @import("../record/heap.zig").RID;

const LockError = error{
    LockConflict,
};

pub const LockMode = enum {
    shared,
    exclusive,
};

const LockInfo = struct {
    mode: LockMode,
    holders: std.AutoHashMap(u64, void),
};

pub const LockManager = struct {
    allocator: Allocator,
    table: std.AutoHashMap(RID, LockInfo),

    pub fn init(allocator: Allocator) LockManager {
        return .{
            .table = std.AutoHashMap(RID, LockInfo).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn acquireLock(self: *LockManager, txn_id: u64, rid: RID, mode: LockMode) !void {
        if (self.table.getPtr(rid)) |info| {
            if (info.holders.contains(txn_id)) {
                return;
            }

            if (mode == .shared and info.mode == .shared) {
                try info.holders.put(txn_id, {});
                return;
            }

            return error.LockConflict;
        }

        var holders = std.AutoHashMap(u64, void).init(self.allocator);
        try holders.put(txn_id, {});
        try self.table.put(rid, .{
            .mode = mode,
            .holders = holders,
        });
    }

    pub fn releaseLock(self: *LockManager, txn_id: u64, rid: RID) void {
        if (self.table.getPtr(rid)) |info| {
            _ = info.holders.remove(txn_id);
            if (info.holders.count() == 0) {
                info.holders.deinit();
                _ = self.table.remove(rid);
            }
        }
    }

    pub fn releaseAllLocks(self: *LockManager, txn_id: u64) void {
        var iter = self.table.iterator();
        while (iter.next()) |entry| {
            const rid = entry.key_ptr;
            var info = entry.value_ptr;
            if (info.holders.contains(txn_id)) {
                _ = info.holders.remove(txn_id);
                if (info.holders.count() == 0) {
                    info.holders.deinit();
                    _ = self.table.remove(rid.*);
                }
            }
        }
    }

    pub fn deinit(self: *LockManager) void {
        var iter = self.table.valueIterator();
        while (iter.next()) |info| {
            info.holders.deinit();
        }
        self.table.deinit();
    }
};

// ============ Tests ============

test "acquire shared lock succeeds" {
    const allocator = std.testing.allocator;
    var lm = LockManager.init(allocator);
    defer lm.deinit();

    try lm.acquireLock(1, RID{ .page_id = 0, .slot_id = 0 }, .shared);
}

test "acquire exclusive lock succeeds" {
    const allocator = std.testing.allocator;
    var lm = LockManager.init(allocator);
    defer lm.deinit();

    try lm.acquireLock(1, RID{ .page_id = 0, .slot_id = 0 }, .exclusive);
}

test "same txn can acquire lock multiple times (reentrant)" {
    const allocator = std.testing.allocator;
    var lm = LockManager.init(allocator);
    defer lm.deinit();

    const rid = RID{ .page_id = 0, .slot_id = 0 };
    try lm.acquireLock(1, rid, .shared);
    try lm.acquireLock(1, rid, .shared);
    try lm.acquireLock(1, rid, .exclusive);
}

test "multiple txns can hold shared locks" {
    const allocator = std.testing.allocator;
    var lm = LockManager.init(allocator);
    defer lm.deinit();

    const rid = RID{ .page_id = 0, .slot_id = 0 };
    try lm.acquireLock(1, rid, .shared);
    try lm.acquireLock(2, rid, .shared);
    try lm.acquireLock(3, rid, .shared);
}

test "exclusive lock blocks other shared lock" {
    const allocator = std.testing.allocator;
    var lm = LockManager.init(allocator);
    defer lm.deinit();

    const rid = RID{ .page_id = 0, .slot_id = 0 };
    try lm.acquireLock(1, rid, .exclusive);

    const result = lm.acquireLock(2, rid, .shared);
    try std.testing.expectError(error.LockConflict, result);
}

test "shared lock blocks other exclusive lock" {
    const allocator = std.testing.allocator;
    var lm = LockManager.init(allocator);
    defer lm.deinit();

    const rid = RID{ .page_id = 0, .slot_id = 0 };
    try lm.acquireLock(1, rid, .shared);

    const result = lm.acquireLock(2, rid, .exclusive);
    try std.testing.expectError(error.LockConflict, result);
}

test "exclusive lock blocks other exclusive lock" {
    const allocator = std.testing.allocator;
    var lm = LockManager.init(allocator);
    defer lm.deinit();

    const rid = RID{ .page_id = 0, .slot_id = 0 };
    try lm.acquireLock(1, rid, .exclusive);

    const result = lm.acquireLock(2, rid, .exclusive);
    try std.testing.expectError(error.LockConflict, result);
}

test "releaseLock allows other txn to acquire" {
    const allocator = std.testing.allocator;
    var lm = LockManager.init(allocator);
    defer lm.deinit();

    const rid = RID{ .page_id = 0, .slot_id = 0 };
    try lm.acquireLock(1, rid, .exclusive);
    lm.releaseLock(1, rid);

    try lm.acquireLock(2, rid, .exclusive);
}

test "releaseAllLocks releases all locks for txn" {
    const allocator = std.testing.allocator;
    var lm = LockManager.init(allocator);
    defer lm.deinit();

    const rid1 = RID{ .page_id = 0, .slot_id = 0 };
    const rid2 = RID{ .page_id = 0, .slot_id = 1 };
    const rid3 = RID{ .page_id = 1, .slot_id = 0 };

    try lm.acquireLock(1, rid1, .exclusive);
    try lm.acquireLock(1, rid2, .exclusive);
    try lm.acquireLock(1, rid3, .exclusive);

    lm.releaseAllLocks(1);

    // Now txn 2 can acquire all of them
    try lm.acquireLock(2, rid1, .exclusive);
    try lm.acquireLock(2, rid2, .exclusive);
    try lm.acquireLock(2, rid3, .exclusive);
}

test "different rids are independent" {
    const allocator = std.testing.allocator;
    var lm = LockManager.init(allocator);
    defer lm.deinit();

    const rid1 = RID{ .page_id = 0, .slot_id = 0 };
    const rid2 = RID{ .page_id = 0, .slot_id = 1 };

    try lm.acquireLock(1, rid1, .exclusive);
    try lm.acquireLock(2, rid2, .exclusive);
}
