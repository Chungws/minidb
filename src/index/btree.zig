const std = @import("std");
const Allocator = std.mem.Allocator;

const Page = @import("../storage/page.zig").Page;
const RID = @import("../record/heap.zig").RID;

const node = @import("node.zig");
const Node = node.Node;
const InternalNode = node.InternalNode;
const LeafNode = node.LeafNode;

const MAX_KEYS: comptime_int = 4;

const SplitResult = struct {
    mid_key: i64,
    new_page_id: u16,
};

pub const BTree = struct {
    pages: std.ArrayList(Page),
    root_page_id: ?u16,
    allocator: Allocator,

    pub fn init(allocator: Allocator) BTree {
        return BTree{
            .pages = std.ArrayList(Page).empty,
            .root_page_id = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BTree) void {
        self.pages.deinit(self.allocator);
    }

    pub fn search(self: *const BTree, key: i64) !?RID {
        if (self.root_page_id == null) {
            return null;
        }

        const leaf_page_id = try self.findLeaf(key, null);

        const page = &self.pages.items[leaf_page_id];
        var leaf = try LeafNode.deserialize(page, self.allocator);
        defer leaf.deinit(self.allocator);

        for (leaf.keys, 0..) |k, i| {
            if (k == key) return leaf.rids[i];
        }
        return null;
    }

    pub fn rangeScan(self: *const BTree, start_key: i64, end_key: i64) !std.ArrayList(RID) {
        var results = std.ArrayList(RID).empty;

        if (self.root_page_id == null) {
            return results;
        }

        var page_id: ?u16 = try self.findLeaf(start_key, null);
        while (page_id) |pid| {
            var leaf = try LeafNode.deserialize(&self.pages.items[pid], self.allocator);
            defer leaf.deinit(self.allocator);

            for (leaf.keys, 0..) |k, i| {
                if (k > end_key) return results;
                if (k >= start_key) {
                    try results.append(self.allocator, leaf.rids[i]);
                }
            }

            page_id = leaf.next;
        }

        return results;
    }

    pub fn insert(self: *BTree, key: i64, rid: RID) !void {
        if (self.root_page_id == null) {
            var keys = [_]i64{key};
            var rids = [_]RID{rid};
            var leaf = Node{ .leaf = LeafNode{
                .keys = &keys,
                .rids = &rids,
                .next = null,
            } };
            const new_page_id = try self.createNewNode(&leaf);
            self.root_page_id = new_page_id;
            return;
        }

        var path = std.ArrayList(u16).empty;
        defer path.deinit(self.allocator);

        const leaf_page_id = try self.findLeaf(key, &path);

        var split_info: ?struct { left: u16, mid_key: i64, right: u16 } = null;
        var split_result = try self.insertInLeaf(leaf_page_id, key, rid);
        split_info = if (split_result) |res| .{
            .left = leaf_page_id,
            .mid_key = res.mid_key,
            .right = res.new_page_id,
        } else null;

        while (split_info) |info| {
            _ = path.pop();

            if (path.items.len == 0) {
                try self.createNewRoot(info.left, info.mid_key, info.right);
                break;
            }

            const parent_id = path.getLast();
            split_result = try self.insertInInternal(parent_id, info.mid_key, info.right);
            split_info = if (split_result) |res| .{
                .left = parent_id,
                .mid_key = res.mid_key,
                .right = res.new_page_id,
            } else null;
        }
    }

    fn findLeaf(self: *const BTree, key: i64, path: ?*std.ArrayList(u16)) !u16 {
        var page_id = self.root_page_id.?;
        var n: Node = undefined;

        while (true) {
            const page = &self.pages.items[page_id];
            n = try Node.deserialize(page, self.allocator);
            if (path) |p| {
                try p.append(self.allocator, page_id);
            }

            switch (n) {
                .leaf => |*ln| {
                    defer ln.deinit(self.allocator);
                    return page_id;
                },
                .internal => |*in| {
                    defer in.deinit(self.allocator);
                    page_id = in.findChildPageId(key);
                },
            }
        }
    }

    fn insertInLeaf(self: *BTree, page_id: u16, key: i64, rid: RID) !?SplitResult {
        const page = &self.pages.items[page_id];
        var leaf = try LeafNode.deserialize(page, self.allocator);
        defer leaf.deinit(self.allocator);

        var keys = std.ArrayList(i64).empty;
        defer keys.deinit(self.allocator);
        var rids = std.ArrayList(RID).empty;
        defer rids.deinit(self.allocator);

        try keys.appendSlice(self.allocator, leaf.keys);
        try rids.appendSlice(self.allocator, leaf.rids);

        const pos = findInsertPos(keys.items, key);
        try keys.insert(self.allocator, pos, key);
        try rids.insert(self.allocator, pos, rid);

        if (keys.items.len <= MAX_KEYS) {
            var new_leaf = LeafNode{
                .keys = keys.items[0..],
                .rids = rids.items[0..],
                .next = leaf.next,
            };
            new_leaf.serialize(&self.pages.items[page_id]);
            return null;
        }

        // Split
        const mid = keys.items.len / 2;

        var right_leaf = Node{ .leaf = LeafNode{
            .keys = keys.items[mid..],
            .rids = rids.items[mid..],
            .next = leaf.next,
        } };
        const new_page_id = try self.createNewNode(&right_leaf);

        var left_leaf = LeafNode{
            .keys = keys.items[0..mid],
            .rids = rids.items[0..mid],
            .next = new_page_id,
        };
        left_leaf.serialize(&self.pages.items[page_id]);

        return .{
            .mid_key = keys.items[mid],
            .new_page_id = new_page_id,
        };
    }

    fn insertInInternal(self: *BTree, page_id: u16, key: i64, child_page_id: u16) !?SplitResult {
        const page = &self.pages.items[page_id];
        var internal = try InternalNode.deserialize(page, self.allocator);
        defer internal.deinit(self.allocator);

        var keys = std.ArrayList(i64).empty;
        defer keys.deinit(self.allocator);
        var children = std.ArrayList(u16).empty;
        defer children.deinit(self.allocator);

        try keys.appendSlice(self.allocator, internal.keys);
        try children.appendSlice(self.allocator, internal.children);

        const pos = findInsertPos(keys.items, key);
        try keys.insert(self.allocator, pos, key);
        try children.insert(self.allocator, pos + 1, child_page_id);

        if (keys.items.len <= MAX_KEYS) {
            var new_internal = InternalNode{
                .keys = keys.items[0..],
                .children = children.items[0..],
            };
            new_internal.serialize(&self.pages.items[page_id]);
            return null;
        }

        // Split
        const mid = keys.items.len / 2;

        var new_node = Node{
            .internal = InternalNode{
                .keys = keys.items[mid + 1 ..],
                .children = children.items[mid + 1 ..],
            },
        };
        const new_page_id = try self.createNewNode(&new_node);

        var left_internal = InternalNode{
            .keys = keys.items[0..mid],
            .children = children.items[0 .. mid + 1],
        };
        left_internal.serialize(&self.pages.items[page_id]);

        return .{
            .mid_key = keys.items[mid],
            .new_page_id = new_page_id,
        };
    }

    fn createNewRoot(self: *BTree, left_page_id: u16, mid_key: i64, right_page_id: u16) !void {
        var keys = [_]i64{mid_key};
        var children = [_]u16{ left_page_id, right_page_id };
        var new_internal_node = Node{ .internal = InternalNode{
            .keys = &keys,
            .children = &children,
        } };
        const new_page_id = try self.createNewNode(&new_internal_node);
        self.root_page_id = new_page_id;
    }

    fn createNewNode(self: *BTree, n: *Node) !u16 {
        var new_page = Page.init();
        n.serialize(&new_page);
        try self.pages.append(self.allocator, new_page);
        return @intCast(self.pages.items.len - 1);
    }
};

fn findInsertPos(keys: []const i64, key: i64) usize {
    var pos: usize = 0;
    while (pos < keys.len and keys[pos] < key) : (pos += 1) {}
    return pos;
}

// ============ Tests ============

test "btree init and deinit" {
    const allocator = std.testing.allocator;
    var btree = BTree.init(allocator);
    defer btree.deinit();

    try std.testing.expect(btree.root_page_id == null);
}

test "btree search empty tree returns null" {
    const allocator = std.testing.allocator;
    var btree = BTree.init(allocator);
    defer btree.deinit();

    const result = try btree.search(42);
    try std.testing.expect(result == null);
}

test "btree search in single leaf node" {
    const allocator = std.testing.allocator;
    var btree = BTree.init(allocator);
    defer btree.deinit();

    // Manually create a leaf page
    var page = Page.init();
    var keys = [_]i64{ 10, 20, 30 };
    var rids = [_]RID{
        .{ .page_id = 1, .slot_id = 1 },
        .{ .page_id = 2, .slot_id = 2 },
        .{ .page_id = 3, .slot_id = 3 },
    };
    var leaf = LeafNode{
        .keys = &keys,
        .rids = &rids,
        .next = null,
    };
    leaf.serialize(&page);

    try btree.pages.append(allocator, page);
    btree.root_page_id = 0;

    // Search for existing key
    const result1 = try btree.search(20);
    try std.testing.expect(result1 != null);
    try std.testing.expectEqual(@as(u16, 2), result1.?.page_id);
    try std.testing.expectEqual(@as(u16, 2), result1.?.slot_id);

    // Search for non-existing key
    const result2 = try btree.search(15);
    try std.testing.expect(result2 == null);
}

test "btree insert into empty tree" {
    const allocator = std.testing.allocator;
    var btree = BTree.init(allocator);
    defer btree.deinit();

    try btree.insert(42, RID{ .page_id = 1, .slot_id = 1 });

    try std.testing.expectEqual(@as(?u16, 0), btree.root_page_id);

    const result = try btree.search(42);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u16, 1), result.?.page_id);
    try std.testing.expectEqual(@as(u16, 1), result.?.slot_id);
}

test "btree insert multiple keys maintains sorted order" {
    const allocator = std.testing.allocator;
    var btree = BTree.init(allocator);
    defer btree.deinit();

    // Insert keys out of order
    try btree.insert(30, RID{ .page_id = 3, .slot_id = 3 });
    try btree.insert(10, RID{ .page_id = 1, .slot_id = 1 });
    try btree.insert(20, RID{ .page_id = 2, .slot_id = 2 });
    try btree.insert(15, RID{ .page_id = 15, .slot_id = 15 });

    // Verify all keys are searchable
    const r1 = try btree.search(10);
    try std.testing.expect(r1 != null);
    try std.testing.expectEqual(@as(u16, 1), r1.?.page_id);

    const r2 = try btree.search(15);
    try std.testing.expect(r2 != null);
    try std.testing.expectEqual(@as(u16, 15), r2.?.page_id);

    const r3 = try btree.search(20);
    try std.testing.expect(r3 != null);
    try std.testing.expectEqual(@as(u16, 2), r3.?.page_id);

    const r4 = try btree.search(30);
    try std.testing.expect(r4 != null);
    try std.testing.expectEqual(@as(u16, 3), r4.?.page_id);
}

test "btree insert triggers leaf split" {
    const allocator = std.testing.allocator;
    var btree = BTree.init(allocator);
    defer btree.deinit();

    // Insert MAX_KEYS + 1 = 5 keys to trigger split
    try btree.insert(10, RID{ .page_id = 1, .slot_id = 1 });
    try btree.insert(20, RID{ .page_id = 2, .slot_id = 2 });
    try btree.insert(30, RID{ .page_id = 3, .slot_id = 3 });
    try btree.insert(40, RID{ .page_id = 4, .slot_id = 4 });
    try btree.insert(25, RID{ .page_id = 25, .slot_id = 25 }); // triggers split

    // Should have 3 pages now (left leaf + right leaf + internal root)
    try std.testing.expectEqual(@as(usize, 3), btree.pages.items.len);

    // All keys should still be searchable
    const r1 = try btree.search(10);
    try std.testing.expect(r1 != null);
    try std.testing.expectEqual(@as(u16, 1), r1.?.page_id);

    const r2 = try btree.search(25);
    try std.testing.expect(r2 != null);
    try std.testing.expectEqual(@as(u16, 25), r2.?.page_id);

    const r3 = try btree.search(40);
    try std.testing.expect(r3 != null);
    try std.testing.expectEqual(@as(u16, 4), r3.?.page_id);
}

test "btree insert triggers second leaf split" {
    const allocator = std.testing.allocator;
    var btree = BTree.init(allocator);
    defer btree.deinit();

    // First split: 5 keys
    try btree.insert(10, RID{ .page_id = 10, .slot_id = 10 });
    try btree.insert(20, RID{ .page_id = 20, .slot_id = 20 });
    try btree.insert(30, RID{ .page_id = 30, .slot_id = 30 });
    try btree.insert(40, RID{ .page_id = 40, .slot_id = 40 });
    try btree.insert(50, RID{ .page_id = 50, .slot_id = 50 }); // first split

    // Add more to right leaf to trigger second split
    try btree.insert(60, RID{ .page_id = 60, .slot_id = 60 });
    try btree.insert(70, RID{ .page_id = 70, .slot_id = 70 });
    try btree.insert(80, RID{ .page_id = 80, .slot_id = 80 }); // second split

    // All keys should be searchable
    const r1 = try btree.search(10);
    try std.testing.expect(r1 != null);

    const r2 = try btree.search(50);
    try std.testing.expect(r2 != null);

    const r3 = try btree.search(80);
    try std.testing.expect(r3 != null);
}

test "btree range scan empty tree" {
    const allocator = std.testing.allocator;
    var btree = BTree.init(allocator);
    defer btree.deinit();

    var results = try btree.rangeScan(10, 50);
    defer results.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 0), results.items.len);
}

test "btree range scan single leaf" {
    const allocator = std.testing.allocator;
    var btree = BTree.init(allocator);
    defer btree.deinit();

    try btree.insert(10, RID{ .page_id = 10, .slot_id = 10 });
    try btree.insert(20, RID{ .page_id = 20, .slot_id = 20 });
    try btree.insert(30, RID{ .page_id = 30, .slot_id = 30 });

    // Full range
    var results1 = try btree.rangeScan(10, 30);
    defer results1.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 3), results1.items.len);

    // Partial range
    var results2 = try btree.rangeScan(15, 25);
    defer results2.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), results2.items.len);
    try std.testing.expectEqual(@as(u16, 20), results2.items[0].page_id);

    // Empty range
    var results3 = try btree.rangeScan(100, 200);
    defer results3.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), results3.items.len);
}

test "btree range scan across multiple leaves" {
    const allocator = std.testing.allocator;
    var btree = BTree.init(allocator);
    defer btree.deinit();

    // Insert enough to trigger splits
    try btree.insert(10, RID{ .page_id = 10, .slot_id = 10 });
    try btree.insert(20, RID{ .page_id = 20, .slot_id = 20 });
    try btree.insert(30, RID{ .page_id = 30, .slot_id = 30 });
    try btree.insert(40, RID{ .page_id = 40, .slot_id = 40 });
    try btree.insert(50, RID{ .page_id = 50, .slot_id = 50 });
    try btree.insert(60, RID{ .page_id = 60, .slot_id = 60 });

    // Range spanning multiple leaves
    var results = try btree.rangeScan(20, 50);
    defer results.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 4), results.items.len);
}
