const std = @import("std");

const Catalog = @import("query/catalog.zig").Catalog;
const Value = @import("sql/ast.zig").Value;
const Tuple = @import("record/tuple.zig").Tuple;
const Session = @import("tx/session.zig").Session;
const ExecuteResult = @import("tx/session.zig").ExecuteResult;

// ============ I/O Functions ============

fn printValue(val: Value, writer: *std.Io.Writer) !void {
    switch (val) {
        .integer => |v| try writer.print("{}", .{v}),
        .text => |v| try writer.print("{s}", .{v}),
        .boolean => |v| try writer.print("{}", .{v}),
        .null_value => try writer.writeAll("NULL"),
    }
}

fn printRow(row: Tuple, writer: *std.Io.Writer) !void {
    for (row.values, 0..) |val, i| {
        if (i > 0) try writer.writeAll("\t");
        try printValue(val, writer);
    }
    try writer.writeAll("\n");
}

fn printResult(result: *ExecuteResult, writer: *std.Io.Writer) !void {
    switch (result.*) {
        .table_created => try writer.writeAll("Table created\n"),
        .index_created => try writer.writeAll("Index created\n"),
        .row_inserted => try writer.writeAll("1 row inserted\n"),
        .transaction_started => try writer.writeAll("Transaction started\n"),
        .transaction_committed => try writer.writeAll("Transaction committed\n"),
        .transaction_aborted => try writer.writeAll("Transaction aborted\n"),
        .select => |*sel| {
            defer sel.deinit();
            for (sel.rows.items) |row| {
                try printRow(row, writer);
            }
            try writer.print("{} rows\n", .{sel.rows.items.len});
        },
        .err => |e| {
            switch (e) {
                .parse => |err| try writer.print("Parse error: {}\n", .{err}),
                .execute => |err| try writer.print("Error: {}\n", .{err}),
            }
        },
    }
}

// ============ Main ============

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var catalog = Catalog.init(allocator);
    defer catalog.deinit();

    var session = Session.init(&catalog, allocator);
    defer session.deinit();

    var stdout_writer = std.fs.File.stdout().writer(&.{});
    var stdout = &stdout_writer.interface;
    var stdin = std.fs.File.stdin();

    var stdin_buf: [4096]u8 = undefined;
    var stdin_reader = stdin.reader(&stdin_buf);
    const reader = &stdin_reader.interface;

    try stdout.writeAll("MiniDB v0.1.0\n");
    try stdout.writeAll("Type 'exit' to quit.\n\n");

    while (true) {
        try stdout.writeAll("minidb> ");

        const bare_line = reader.takeDelimiter('\n') catch |err| {
            if (err == error.EndOfStream) break;
            return err;
        } orelse continue;

        const line = std.mem.trim(u8, bare_line, " \t\r\n");

        if (line.len == 0) continue;

        if (std.mem.eql(u8, line, "exit") or std.mem.eql(u8, line, "quit")) {
            try stdout.writeAll("Bye!\n");
            break;
        }

        var result = session.execute(line);
        try printResult(&result, stdout);
    }
}
