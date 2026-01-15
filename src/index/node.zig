const std = @import("std");
const Allocator = std.mem.Allocator;

const Page = @import("../storage/page.zig").Page;
const RID = @import("../record/heap.zig").RID;

pub const NODE_TYPE_INTERNAL: u8 = 0;
pub const NODE_TYPE_LEAF: u8 = 1;

pub const INTERNAL_HEADER_SIZE: usize = 3;
pub const LEAF_HEADER_SIZE: usize = 5;

pub const InternalNode = struct {
    keys: []i64,
    children: []u16,

    pub fn serialize(self: *InternalNode, page: *Page) void {
        var buf: [8]u8 = undefined;
        var offset: usize = 0;

        std.mem.writeInt(u8, buf[0..1], NODE_TYPE_INTERNAL, .little);
        page.write(offset, buf[0..1]);
        offset += 1;

        std.mem.writeInt(u16, buf[0..2], @intCast(self.keys.len), .little);
        page.write(offset, buf[0..2]);
        offset += 2;

        for (0..self.keys.len) |i| {
            std.mem.writeInt(u16, buf[0..2], self.children[i], .little);
            page.write(offset, buf[0..2]);
            offset += 2;

            std.mem.writeInt(i64, buf[0..8], self.keys[i], .little);
            page.write(offset, buf[0..8]);
            offset += 8;
        }
        std.mem.writeInt(u16, buf[0..2], self.children[self.children.len - 1], .little);
        page.write(offset, buf[0..2]);
    }

    pub fn deserialize(page: *const Page, allocator: Allocator) !InternalNode {
        var offset: usize = 1;

        const num_keys = std.mem.readInt(u16, page.data[offset..][0..2], .little);
        offset += 2;

        const keys = try allocator.alloc(i64, num_keys);
        const children = try allocator.alloc(u16, num_keys + 1);

        for (0..num_keys) |i| {
            children[i] = std.mem.readInt(u16, page.data[offset..][0..2], .little);
            offset += 2;

            keys[i] = std.mem.readInt(i64, page.data[offset..][0..8], .little);
            offset += 8;
        }
        children[num_keys] = std.mem.readInt(u16, page.data[offset..][0..2], .little);

        return InternalNode{
            .keys = keys,
            .children = children,
        };
    }
};

pub const LeafNode = struct {
    keys: []i64,
    rids: []RID,
    next: ?u16,

    pub fn serialize(self: *LeafNode, page: *Page) void {
        var buf: [8]u8 = undefined;
        var offset: usize = 0;

        std.mem.writeInt(u8, buf[0..1], NODE_TYPE_LEAF, .little);
        page.write(offset, buf[0..1]);
        offset += 1;

        std.mem.writeInt(u16, buf[0..2], @intCast(self.keys.len), .little);
        page.write(offset, buf[0..2]);
        offset += 2;

        const next = if (self.next) |next| next else 0;
        std.mem.writeInt(u16, buf[0..2], next, .little);
        page.write(offset, buf[0..2]);
        offset += 2;

        for (0..self.keys.len) |i| {
            std.mem.writeInt(i64, buf[0..8], self.keys[i], .little);
            page.write(offset, buf[0..8]);
            offset += 8;

            std.mem.writeInt(u16, buf[0..2], self.rids[i].page_id, .little);
            page.write(offset, buf[0..2]);
            offset += 2;

            std.mem.writeInt(u16, buf[0..2], self.rids[i].slot_id, .little);
            page.write(offset, buf[0..2]);
            offset += 2;
        }
    }

    pub fn deserialize(page: *const Page, allocator: Allocator) !LeafNode {
        var offset: usize = 1;

        const num_keys = std.mem.readInt(u16, page.data[offset..][0..2], .little);
        offset += 2;

        const next = std.mem.readInt(u16, page.data[offset..][0..2], .little);
        offset += 2;

        const keys = try allocator.alloc(i64, num_keys);
        const rids = try allocator.alloc(RID, num_keys);

        for (0..num_keys) |i| {
            keys[i] = std.mem.readInt(i64, page.data[offset..][0..8], .little);
            offset += 8;

            const page_id = std.mem.readInt(u16, page.data[offset..][0..2], .little);
            offset += 2;

            const slot_id = std.mem.readInt(u16, page.data[offset..][0..2], .little);
            offset += 2;

            rids[i] = RID{ .page_id = page_id, .slot_id = slot_id };
        }

        return LeafNode{
            .keys = keys,
            .rids = rids,
            .next = if (next != 0) next else null,
        };
    }
};

pub const Node = union(enum) {
    internal: InternalNode,
    leaf: LeafNode,

    pub fn serialize(self: *Node, page: *Page) void {
        switch (self.*) {
            .internal => |*n| {
                n.serialize(page);
            },
            .leaf => |*n| {
                n.serialize(page);
            },
        }
    }

    pub fn deserialize(page: *const Page, allocator: Allocator) !Node {
        const node_type = std.mem.readInt(u8, page.data[0..1], .little);

        switch (node_type) {
            NODE_TYPE_INTERNAL => {
                return Node{
                    .internal = try InternalNode.deserialize(page, allocator),
                };
            },
            NODE_TYPE_LEAF => {
                return Node{
                    .leaf = try LeafNode.deserialize(page, allocator),
                };
            },
            else => {
                unreachable;
            },
        }
    }
};

// ============ Tests ============

test "serialize internal node" {
    var page = Page.init();

    var keys = [_]i64{ 10, 20, 30 };
    var children = [_]u16{ 1, 2, 3, 4 };

    var internal = InternalNode{
        .keys = &keys,
        .children = &children,
    };
    internal.serialize(&page);

    // Verify header
    try std.testing.expectEqual(NODE_TYPE_INTERNAL, page.data[0]); // type
    const num_keys = std.mem.readInt(u16, page.data[1..3], .little);
    try std.testing.expectEqual(@as(u16, 3), num_keys);

    // Verify first child (offset 3)
    const child0 = std.mem.readInt(u16, page.data[3..5], .little);
    try std.testing.expectEqual(@as(u16, 1), child0);

    // Verify first key (offset 5)
    const key0 = std.mem.readInt(i64, page.data[5..13], .little);
    try std.testing.expectEqual(@as(i64, 10), key0);
}

test "serialize leaf node" {
    var page = Page.init();

    var keys = [_]i64{ 100, 200 };
    var rids = [_]RID{
        .{ .page_id = 1, .slot_id = 10 },
        .{ .page_id = 2, .slot_id = 20 },
    };

    var leaf = LeafNode{
        .keys = &keys,
        .rids = &rids,
        .next = 99,
    };
    leaf.serialize(&page);

    // Verify header
    try std.testing.expectEqual(NODE_TYPE_LEAF, page.data[0]); // type
    const num_keys = std.mem.readInt(u16, page.data[1..3], .little);
    try std.testing.expectEqual(@as(u16, 2), num_keys);

    // Verify next pointer (offset 3)
    const next = std.mem.readInt(u16, page.data[3..5], .little);
    try std.testing.expectEqual(@as(u16, 99), next);

    // Verify first key (offset 5)
    const key0 = std.mem.readInt(i64, page.data[5..13], .little);
    try std.testing.expectEqual(@as(i64, 100), key0);

    // Verify first rid (offset 13)
    const page_id0 = std.mem.readInt(u16, page.data[13..15], .little);
    const slot_id0 = std.mem.readInt(u16, page.data[15..17], .little);
    try std.testing.expectEqual(@as(u16, 1), page_id0);
    try std.testing.expectEqual(@as(u16, 10), slot_id0);
}

test "serialize and deserialize internal node roundtrip" {
    const allocator = std.testing.allocator;
    var page = Page.init();

    var keys = [_]i64{ 10, 20, 30 };
    var children = [_]u16{ 1, 2, 3, 4 };

    var internal = InternalNode{
        .keys = &keys,
        .children = &children,
    };
    internal.serialize(&page);

    const result = try InternalNode.deserialize(&page, allocator);
    defer allocator.free(result.keys);
    defer allocator.free(result.children);

    try std.testing.expectEqual(@as(usize, 3), result.keys.len);
    try std.testing.expectEqual(@as(i64, 10), result.keys[0]);
    try std.testing.expectEqual(@as(i64, 20), result.keys[1]);
    try std.testing.expectEqual(@as(i64, 30), result.keys[2]);

    try std.testing.expectEqual(@as(usize, 4), result.children.len);
    try std.testing.expectEqual(@as(u16, 1), result.children[0]);
    try std.testing.expectEqual(@as(u16, 4), result.children[3]);
}

test "serialize and deserialize leaf node roundtrip" {
    const allocator = std.testing.allocator;
    var page = Page.init();

    var keys = [_]i64{ 100, 200 };
    var rids = [_]RID{
        .{ .page_id = 1, .slot_id = 10 },
        .{ .page_id = 2, .slot_id = 20 },
    };

    var leaf = LeafNode{
        .keys = &keys,
        .rids = &rids,
        .next = 99,
    };
    leaf.serialize(&page);

    const result = try LeafNode.deserialize(&page, allocator);
    defer allocator.free(result.keys);
    defer allocator.free(result.rids);

    try std.testing.expectEqual(@as(usize, 2), result.keys.len);
    try std.testing.expectEqual(@as(i64, 100), result.keys[0]);
    try std.testing.expectEqual(@as(i64, 200), result.keys[1]);

    try std.testing.expectEqual(@as(u16, 1), result.rids[0].page_id);
    try std.testing.expectEqual(@as(u16, 10), result.rids[0].slot_id);
    try std.testing.expectEqual(@as(u16, 99), result.next.?);
}
