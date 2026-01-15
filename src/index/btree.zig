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
};

// ============ Tests ============

test "btree init and deinit" {
    const allocator = std.testing.allocator;
    var btree = BTree.init(allocator);
    defer btree.deinit();

    try std.testing.expect(btree.root_page_id == null);
}
