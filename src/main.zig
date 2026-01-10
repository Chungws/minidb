const std = @import("std");

pub fn main() !void {
    const stdout = std.fs.File.stdout();
    const stdin = std.fs.File.stdin();

    try stdout.writeAll("MiniDB v0.1.0\n");
    try stdout.writeAll("Type 'exit' to quit.\n\n");

    var buf: [1024]u8 = undefined;

    while (true) {
        try stdout.writeAll("minidb> ");

        const n = stdin.read(&buf) catch {
            try stdout.writeAll("Error reading input\n");
            continue;
        };

        if (n == 0) {
            // EOF
            try stdout.writeAll("\nBye!\n");
            break;
        }

        const line = std.mem.trim(u8, buf[0..n], " \t\r\n");

        if (line.len == 0) continue;

        if (std.mem.eql(u8, line, "exit") or std.mem.eql(u8, line, "quit")) {
            try stdout.writeAll("Bye!\n");
            break;
        }

        // TODO: Parse and execute SQL
        try stdout.writeAll("Received: ");
        try stdout.writeAll(line);
        try stdout.writeAll("\n");
    }
}

test "main module compiles" {
    _ = @import("std");
}
