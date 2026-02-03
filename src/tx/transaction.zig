const std = @import("std");
const Allocator = std.mem.Allocator;

const TransactionError = error{
    TransactionNotActive,
    TransactionNotFound,
};

pub const TXState = enum {
    active,
    committed,
    aborted,
};

pub const Transaction = struct {
    id: u64,
    state: TXState,
};

pub const TransactionManager = struct {
    id: u64,
    tx_table: std.AutoHashMap(u64, TXState),
    allocator: Allocator,

    pub fn init(allocator: Allocator) TransactionManager {
        return TransactionManager{
            .id = 0,
            .tx_table = std.AutoHashMap(u64, TXState).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn begin(self: *TransactionManager) !Transaction {
        self.id += 1;
        try self.tx_table.put(self.id, .active);
        return .{
            .id = self.id,
            .state = .active,
        };
    }

    pub fn commit(self: *TransactionManager, txn_id: u64) !void {
        if (self.tx_table.get(txn_id)) |prev| {
            if (prev != .active) {
                return TransactionError.TransactionNotActive;
            }
            try self.tx_table.put(txn_id, .committed);
            return;
        }

        return TransactionError.TransactionNotFound;
    }

    pub fn abort(self: *TransactionManager, txn_id: u64) !void {
        if (self.tx_table.get(txn_id)) |prev| {
            if (prev != .active) {
                return TransactionError.TransactionNotActive;
            }
            try self.tx_table.put(txn_id, .aborted);
            return;
        }
        return TransactionError.TransactionNotFound;
    }

    pub fn getTransaction(self: *TransactionManager, txn_id: u64) ?Transaction {
        if (self.tx_table.get(txn_id)) |state| {
            return .{
                .id = txn_id,
                .state = state,
            };
        }
        return null;
    }

    pub fn deinit(self: *TransactionManager) void {
        self.tx_table.deinit();
    }
};

// ============ Tests ============

test "begin creates active transaction" {
    const allocator = std.testing.allocator;
    var mgr = TransactionManager.init(allocator);
    defer mgr.deinit();

    const txn = try mgr.begin();
    try std.testing.expectEqual(TXState.active, txn.state);
}

test "commit changes state to committed" {
    const allocator = std.testing.allocator;
    var mgr = TransactionManager.init(allocator);
    defer mgr.deinit();

    const txn = try mgr.begin();
    try mgr.commit(txn.id);

    const updated = mgr.getTransaction(txn.id).?;
    try std.testing.expectEqual(TXState.committed, updated.state);
}

test "abort changes state to aborted" {
    const allocator = std.testing.allocator;
    var mgr = TransactionManager.init(allocator);
    defer mgr.deinit();

    const txn = try mgr.begin();
    try mgr.abort(txn.id);

    const updated = mgr.getTransaction(txn.id).?;
    try std.testing.expectEqual(TXState.aborted, updated.state);
}

test "txn ids increment sequentially" {
    const allocator = std.testing.allocator;
    var mgr = TransactionManager.init(allocator);
    defer mgr.deinit();

    const txn1 = try mgr.begin();
    const txn2 = try mgr.begin();
    const txn3 = try mgr.begin();

    try std.testing.expectEqual(@as(u64, 1), txn1.id);
    try std.testing.expectEqual(@as(u64, 2), txn2.id);
    try std.testing.expectEqual(@as(u64, 3), txn3.id);
}

test "manage multiple transactions independently" {
    const allocator = std.testing.allocator;
    var mgr = TransactionManager.init(allocator);
    defer mgr.deinit();

    const txn1 = try mgr.begin();
    const txn2 = try mgr.begin();

    try mgr.commit(txn1.id);
    try mgr.abort(txn2.id);

    const t1 = mgr.getTransaction(txn1.id).?;
    const t2 = mgr.getTransaction(txn2.id).?;
    try std.testing.expectEqual(TXState.committed, t1.state);
    try std.testing.expectEqual(TXState.aborted, t2.state);
}

test "commit already committed transaction returns error" {
    const allocator = std.testing.allocator;
    var mgr = TransactionManager.init(allocator);
    defer mgr.deinit();

    const txn = try mgr.begin();
    try mgr.commit(txn.id);

    const result = mgr.commit(txn.id);
    try std.testing.expectError(error.TransactionNotActive, result);
}

test "abort already committed transaction returns error" {
    const allocator = std.testing.allocator;
    var mgr = TransactionManager.init(allocator);
    defer mgr.deinit();

    const txn = try mgr.begin();
    try mgr.commit(txn.id);

    const result = mgr.abort(txn.id);
    try std.testing.expectError(error.TransactionNotActive, result);
}

test "commit already aborted transaction returns error" {
    const allocator = std.testing.allocator;
    var mgr = TransactionManager.init(allocator);
    defer mgr.deinit();

    const txn = try mgr.begin();
    try mgr.abort(txn.id);

    const result = mgr.commit(txn.id);
    try std.testing.expectError(error.TransactionNotActive, result);
}

test "commit nonexistent transaction returns error" {
    const allocator = std.testing.allocator;
    var mgr = TransactionManager.init(allocator);
    defer mgr.deinit();

    const result = mgr.commit(999);
    try std.testing.expectError(error.TransactionNotFound, result);
}

test "abort nonexistent transaction returns error" {
    const allocator = std.testing.allocator;
    var mgr = TransactionManager.init(allocator);
    defer mgr.deinit();

    const result = mgr.abort(999);
    try std.testing.expectError(error.TransactionNotFound, result);
}

test "getTransaction returns null for nonexistent id" {
    const allocator = std.testing.allocator;
    var mgr = TransactionManager.init(allocator);
    defer mgr.deinit();

    try std.testing.expect(mgr.getTransaction(999) == null);
}
