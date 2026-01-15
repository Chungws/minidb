const std = @import("std");
const Allocator = std.mem.Allocator;

const Page = @import("../storage/page.zig").Page;
const RID = @import("../record/heap.zig").RID;

const node = @import("node.zig");
const Node = node.Node;
const InternalNode = node.InternalNode;
const LeafNode = node.LeafNode;

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

        const leaf_page_id = try self.findLeaf(key);

        const page = &self.pages.items[leaf_page_id];
        var leaf = try LeafNode.deserialize(page, self.allocator);
        defer leaf.deinit(self.allocator);

        for (leaf.keys, 0..) |k, i| {
            if (k == key) return leaf.rids[i];
        }
        return null;
    }

    fn findLeaf(self: *const BTree, key: i64) !u16 {
        var page_id = self.root_page_id.?;
        var n: Node = undefined;

        while (true) {
            const page = &self.pages.items[page_id];
            n = try Node.deserialize(page, self.allocator);

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
};

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
