const std = @import("std");

const Token = @import("token.zig").Token;
const TokenType = @import("token.zig").TokenType;

pub const Lexer = struct {
    input: []const u8,
    pos: usize,

    pub fn init(input: []const u8) Lexer {
        return Lexer{
            .input = input,
            .pos = 0,
        };
    }

    pub fn nextToken(self: *Lexer) Token {
        const char = self.peek() orelse return Token{ .type = TokenType.eof, .lexeme = "EOF" };
        if (char == ' ' or char == '\t' or char == '\n' or char == '\r') {
            self.advance();
            return self.nextToken();
        }
        switch (char) {
            '(' => {
                self.advance();
                return Token{ .type = TokenType.lparen, .lexeme = "(" };
            },
            ')' => {
                self.advance();
                return Token{ .type = TokenType.rparen, .lexeme = ")" };
            },
            ',' => {
                self.advance();
                return Token{ .type = TokenType.comma, .lexeme = "," };
            },
            '.' => {
                self.advance();
                return Token{ .type = TokenType.dot, .lexeme = "." };
            },
            ';' => {
                self.advance();
                return Token{ .type = TokenType.semicolon, .lexeme = ";" };
            },
            '*' => {
                self.advance();
                return Token{ .type = TokenType.asterisk, .lexeme = "*" };
            },
            '=' => {
                self.advance();
                return Token{ .type = TokenType.eq, .lexeme = "=" };
            },
            '<' => {
                self.advance();
                if (self.peek()) |c| {
                    switch (c) {
                        '>' => {
                            self.advance();
                            return Token{ .type = TokenType.neq, .lexeme = "<>" };
                        },
                        '=' => {
                            self.advance();
                            return Token{ .type = TokenType.lte, .lexeme = "<=" };
                        },
                        else => {},
                    }
                }
                return Token{ .type = TokenType.lt, .lexeme = "<" };
            },
            '>' => {
                self.advance();
                if (self.peek()) |c| {
                    switch (c) {
                        '=' => {
                            self.advance();
                            return Token{ .type = TokenType.gte, .lexeme = ">=" };
                        },
                        else => {},
                    }
                }
                return Token{ .type = TokenType.gt, .lexeme = ">" };
            },
            '!' => {
                self.advance();
                const next = self.peek();
                if (next) |c| {
                    switch (c) {
                        '=' => {
                            self.advance();
                            return Token{ .type = TokenType.neq, .lexeme = "!=" };
                        },
                        else => {},
                    }
                }
                return Token{ .type = TokenType.illegal, .lexeme = "!" };
            },
            '0'...'9' => {
                const start = self.pos;
                while (self.peek()) |c| {
                    if (c < '0' or c > '9') break;
                    self.advance();
                }
                return Token{ .type = TokenType.integer, .lexeme = self.input[start..self.pos] };
            },
            '\'' => {
                self.advance();
                const start = self.pos;
                while (self.peek()) |c| {
                    if (c == '\'') break;
                    self.advance();
                }
                const lexeme = self.input[start..self.pos];
                self.advance();
                return Token{ .type = TokenType.string, .lexeme = lexeme };
            },
            'a'...'z', 'A'...'Z', '_' => {
                const start = self.pos;
                while (self.peek()) |c| {
                    if (!isAlphaNumeric(c)) break;
                    self.advance();
                }
                const keyword = self.input[start..self.pos];
                return Token{ .type = lookupKeyword(keyword), .lexeme = keyword };
            },
            else => {},
        }
        self.advance();
        return Token{ .type = TokenType.illegal, .lexeme = "?" };
    }

    fn peek(self: *Lexer) ?u8 {
        if (self.pos >= self.input.len) return null;
        return self.input[self.pos];
    }

    fn advance(self: *Lexer) void {
        self.pos += 1;
    }
};

fn isAlphaNumeric(c: u8) bool {
    return (c >= 'a' and c <= 'z') or
        (c >= 'A' and c <= 'Z') or
        (c >= '0' and c <= '9') or
        c == '_';
}

const keywords = std.StaticStringMap(TokenType).initComptime(.{
    .{ "select", .select },
    .{ "from", .from },
    .{ "where", .where },
    .{ "insert", .insert },
    .{ "into", .into },
    .{ "values", .values },
    .{ "create", .create },
    .{ "table", .table },
    .{ "index", .index },
    .{ "on", .on },
    .{ "join", .join },
    .{ "int", .int_type },
    .{ "text", .text_type },
    .{ "bool", .bool_type },
    .{ "true", .true_lit },
    .{ "false", .false_lit },
    .{ "null", .null_lit },
    .{ "and", .and_op },
    .{ "or", .or_op },
    .{ "not", .not_op },
});

fn lookupKeyword(ident: []const u8) TokenType {
    var lower: [32]u8 = undefined;
    const len = @min(ident.len, 32);
    for (ident[0..len], 0..) |c, i| {
        lower[i] = std.ascii.toLower(c);
    }
    return keywords.get(lower[0..len]) orelse .identifier;
}

test "simple select" {
    var lexer = Lexer.init("SELECT * FROM users");

    try std.testing.expectEqual(TokenType.select, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.asterisk, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.from, lexer.nextToken().type);

    const ident = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, ident.type);
    try std.testing.expectEqualStrings("users", ident.lexeme);

    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "select with where" {
    var lexer = Lexer.init("SELECT name FROM users WHERE age >= 18");

    try std.testing.expectEqual(TokenType.select, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.from, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.where, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.gte, lexer.nextToken().type);

    const num = lexer.nextToken();
    try std.testing.expectEqual(TokenType.integer, num.type);
    try std.testing.expectEqualStrings("18", num.lexeme);

    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "string literal" {
    var lexer = Lexer.init("'hello world'");

    const str = lexer.nextToken();
    try std.testing.expectEqual(TokenType.string, str.type);
    try std.testing.expectEqualStrings("hello world", str.lexeme);

    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "insert statement" {
    var lexer = Lexer.init("INSERT INTO users VALUES (1, 'alice')");

    try std.testing.expectEqual(TokenType.insert, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.into, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.values, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.lparen, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.integer, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.comma, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.string, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.rparen, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "create table" {
    var lexer = Lexer.init("CREATE TABLE users (id INT, name TEXT)");

    try std.testing.expectEqual(TokenType.create, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.table, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.lparen, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.int_type, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.comma, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.text_type, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.rparen, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "comparison operators" {
    var lexer = Lexer.init("= <> != < > <= >=");

    try std.testing.expectEqual(TokenType.eq, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.neq, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.neq, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.lt, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.gt, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.lte, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.gte, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "empty input" {
    var lexer = Lexer.init("");
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "whitespace only" {
    var lexer = Lexer.init("   \t\n\r  ");
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "unterminated string" {
    var lexer = Lexer.init("'hello");

    const tok = lexer.nextToken();
    try std.testing.expectEqual(TokenType.string, tok.type);
    try std.testing.expectEqualStrings("hello", tok.lexeme);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "case insensitive keywords" {
    var lexer = Lexer.init("SeLeCt FrOm WhErE");

    try std.testing.expectEqual(TokenType.select, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.from, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.where, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "identifier with underscore" {
    var lexer = Lexer.init("_id user_name table_1");

    const tok1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, tok1.type);
    try std.testing.expectEqualStrings("_id", tok1.lexeme);

    const tok2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, tok2.type);
    try std.testing.expectEqualStrings("user_name", tok2.lexeme);

    const tok3 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, tok3.type);
    try std.testing.expectEqualStrings("table_1", tok3.lexeme);

    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "illegal characters" {
    var lexer = Lexer.init("@ # $");

    try std.testing.expectEqual(TokenType.illegal, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.illegal, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.illegal, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "standalone exclamation" {
    var lexer = Lexer.init("! a");

    try std.testing.expectEqual(TokenType.illegal, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "boolean literals" {
    var lexer = Lexer.init("TRUE FALSE true false");

    try std.testing.expectEqual(TokenType.true_lit, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.false_lit, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.true_lit, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.false_lit, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "logical operators" {
    var lexer = Lexer.init("AND OR NOT and or not");

    try std.testing.expectEqual(TokenType.and_op, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.or_op, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.not_op, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.and_op, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.or_op, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.not_op, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "statement with semicolon" {
    var lexer = Lexer.init("SELECT * FROM users;");

    try std.testing.expectEqual(TokenType.select, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.asterisk, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.from, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.semicolon, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "tabs and newlines" {
    var lexer = Lexer.init("SELECT\n*\t FROM\r\nusers");

    try std.testing.expectEqual(TokenType.select, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.asterisk, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.from, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "index and on keywords" {
    var lexer = Lexer.init("CREATE INDEX idx ON users (id)");

    try std.testing.expectEqual(TokenType.create, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.index, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.on, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.lparen, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.rparen, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "join keyword" {
    var lexer = Lexer.init("JOIN orders ON users.id = orders.user_id");

    try std.testing.expectEqual(TokenType.join, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.on, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.dot, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eq, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.dot, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.nextToken().type);
    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}

test "dot token" {
    var lexer = Lexer.init("users.name");

    const tok1 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, tok1.type);
    try std.testing.expectEqualStrings("users", tok1.lexeme);

    try std.testing.expectEqual(TokenType.dot, lexer.nextToken().type);

    const tok2 = lexer.nextToken();
    try std.testing.expectEqual(TokenType.identifier, tok2.type);
    try std.testing.expectEqualStrings("name", tok2.lexeme);

    try std.testing.expectEqual(TokenType.eof, lexer.nextToken().type);
}
