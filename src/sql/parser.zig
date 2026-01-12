const std = @import("std");
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;

const Lexer = @import("lexer.zig").Lexer;
const Token = @import("token.zig").Token;
const TokenType = @import("token.zig").TokenType;
const ast = @import("ast.zig");
const Statement = ast.Statement;
const SelectStatement = ast.SelectStatement;
const InsertStatement = ast.InsertStatement;
const CreateTableStatement = ast.CreateTableStatement;

const ParserPoolError = error{
    UnexpectedToken,
};

pub const Parser = struct {
    lexer: Lexer,
    current: Token,
    allocator: Allocator,

    pub fn init(input: []const u8, allocator: Allocator) Parser {
        var lexer = Lexer.init(input);
        return Parser{
            .current = lexer.nextToken(),
            .lexer = lexer,
            .allocator = allocator,
        };
    }

    pub fn parse(self: *Parser) !Statement {
        switch (self.current.type) {
            TokenType.select => {
                return Statement{
                    .select = try self.parseSelect(),
                };
            },
            TokenType.insert => {
                return Statement{
                    .insert = try self.parseInsert(),
                };
            },
            TokenType.create => {
                return Statement{
                    .create_table = try self.parseCreateTable(),
                };
            },
            else => return error.UnexpectedToken,
        }
    }

    fn parseSelect(self: *Parser) !SelectStatement {
        self.advance();

        var columns = std.ArrayList([]const u8).empty;
        if (self.current.type == TokenType.asterisk) {
            try columns.append(self.allocator, "*");
            self.advance();
        } else {
            while (self.current.type != TokenType.from) {
                if (self.current.type == TokenType.comma) {
                    self.advance();
                    continue;
                }
                try columns.append(self.allocator, self.current.lexeme);
                self.advance();
            }
        }

        try self.expect(TokenType.from);

        const table_name = self.current.lexeme;
        try self.expect(.identifier);

        var where: ?ast.Condition = null;
        if (self.current.type == TokenType.where) {
            self.advance();
            where = try self.parseCondition();
        }

        return SelectStatement{
            .columns = try columns.toOwnedSlice(self.allocator),
            .table_name = table_name,
            .where = where,
        };
    }

    fn parseInsert(self: *Parser) !InsertStatement {
        self.advance();
        return error.UnexpectedToken;
    }
    fn parseCreateTable(self: *Parser) !CreateTableStatement {
        self.advance();
        return error.UnexpectedToken;
    }

    fn parseCondition(self: *Parser) anyerror!ast.Condition {
        const condition = try self.parseAndCondition();
        if (self.current.type == TokenType.or_op) {
            self.advance();
            const left = try self.allocator.create(ast.Condition);
            left.* = condition;
            const right = try self.allocator.create(ast.Condition);
            right.* = try self.parseCondition();
            return ast.Condition{ .or_op = .{
                .left = left,
                .right = right,
            } };
        }
        return condition;
    }

    fn parseAndCondition(self: *Parser) !ast.Condition {
        const condition = try self.parseNotCondition();
        if (self.current.type == TokenType.and_op) {
            self.advance();
            const left = try self.allocator.create(ast.Condition);
            left.* = condition;
            const right = try self.allocator.create(ast.Condition);
            right.* = try self.parseNotCondition();
            return ast.Condition{ .and_op = .{
                .left = left,
                .right = right,
            } };
        }
        return condition;
    }

    fn parseNotCondition(self: *Parser) !ast.Condition {
        if (self.current.type == TokenType.not_op) {
            self.advance();
            const condition = try self.allocator.create(ast.Condition);
            condition.* = try self.parseNotCondition();
            return ast.Condition{ .not_op = condition };
        } else if (self.current.type == TokenType.lparen) {
            self.advance();
            const condition = try self.parseCondition();
            try self.expect(TokenType.rparen);
            return condition;
        }
        return try self.parseSimpleCondition();
    }

    fn parseSimpleCondition(self: *Parser) !ast.Condition {
        const column_name = self.current.lexeme;
        try self.expect(.identifier);

        if (!checkConditionOperator(self.current.type)) {
            return error.UnexpectedToken;
        }
        const operator = try convertTokenToAstOperator(self.current.type);
        self.advance();

        if (!checkLiteral(self.current.type)) {
            return error.UnexpectedToken;
        }
        const value = try convertTokenToAstValue(self.current);
        self.advance();

        return ast.Condition{ .simple = .{
            .column = column_name,
            .op = operator,
            .value = value,
        } };
    }

    fn advance(self: *Parser) void {
        self.current = self.lexer.nextToken();
    }

    fn expect(self: *Parser, expected: TokenType) !void {
        if (self.current.type != expected) {
            return error.UnexpectedToken;
        }
        self.advance();
    }

    fn freeCondition(self: *Parser, condition: ast.Condition) void {
        switch (condition) {
            .or_op => |op| {
                self.freeCondition(op.left.*);
                self.freeCondition(op.right.*);
                self.allocator.destroy(op.left);
                self.allocator.destroy(op.right);
            },
            .and_op => |op| {
                self.freeCondition(op.left.*);
                self.freeCondition(op.right.*);
                self.allocator.destroy(op.left);
                self.allocator.destroy(op.right);
            },
            .not_op => |op| {
                self.freeCondition(op.*);
                self.allocator.destroy(op);
            },
            else => {},
        }
    }
};

fn checkConditionOperator(t: TokenType) bool {
    switch (t) {
        TokenType.eq,
        TokenType.neq,
        TokenType.lt,
        TokenType.gt,
        TokenType.lte,
        TokenType.gte,
        => return true,
        else => return false,
    }
}

fn checkLiteral(t: TokenType) bool {
    switch (t) {
        TokenType.integer,
        TokenType.string,
        TokenType.true_lit,
        TokenType.false_lit,
        => return true,
        else => return false,
    }
}

fn convertTokenToAstOperator(t: TokenType) !ast.Operator {
    switch (t) {
        TokenType.eq => return ast.Operator.eq,
        TokenType.neq => return ast.Operator.neq,
        TokenType.lt => return ast.Operator.lt,
        TokenType.gt => return ast.Operator.gt,
        TokenType.lte => return ast.Operator.lte,
        TokenType.gte => return ast.Operator.gte,
        else => return error.UnexpectedToken,
    }
}

fn convertTokenToAstValue(t: Token) !ast.Value {
    switch (t.type) {
        TokenType.integer => return ast.Value{ .integer = try std.fmt.parseInt(i64, t.lexeme, 10) },
        TokenType.string => return ast.Value{ .string = t.lexeme },
        TokenType.true_lit => return ast.Value{ .boolean = true },
        TokenType.false_lit => return ast.Value{ .boolean = false },
        else => return error.UnexpectedToken,
    }
}

// ============ Tests ============

test "parse SELECT * FROM table" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT * FROM users", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    try std.testing.expectEqual(@as(usize, 1), select.columns.len);
    try std.testing.expectEqualStrings("*", select.columns[0]);
    try std.testing.expectEqualStrings("users", select.table_name);
    try std.testing.expect(select.where == null);

    allocator.free(select.columns);
}

test "parse SELECT columns FROM table" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT name, age FROM users", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    try std.testing.expectEqual(@as(usize, 2), select.columns.len);
    try std.testing.expectEqualStrings("name", select.columns[0]);
    try std.testing.expectEqualStrings("age", select.columns[1]);
    try std.testing.expectEqualStrings("users", select.table_name);

    allocator.free(select.columns);
}

test "parse SELECT with simple WHERE" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT * FROM users WHERE age >= 18", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    try std.testing.expectEqualStrings("users", select.table_name);
    try std.testing.expect(select.where != null);

    const where = select.where.?;
    try std.testing.expectEqual(ast.Condition.simple, std.meta.activeTag(where));

    const simple = where.simple;
    try std.testing.expectEqualStrings("age", simple.column);
    try std.testing.expectEqual(ast.Operator.gte, simple.op);
    try std.testing.expectEqual(@as(i64, 18), simple.value.integer);

    allocator.free(select.columns);
}

test "parse SELECT with AND condition" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT * FROM users WHERE age >= 18 AND name = 'alice'", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    try std.testing.expect(select.where != null);

    const where = select.where.?;
    try std.testing.expectEqual(ast.Condition.and_op, std.meta.activeTag(where));

    parser.freeCondition(where);
    allocator.free(select.columns);
}

test "parse SELECT with OR condition" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT * FROM users WHERE age < 18 OR is_admin = true", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    try std.testing.expect(select.where != null);

    const where = select.where.?;
    try std.testing.expectEqual(ast.Condition.or_op, std.meta.activeTag(where));

    parser.freeCondition(where);
    allocator.free(select.columns);
}

test "parse SELECT with complex condition (AND has higher precedence)" {
    const allocator = std.testing.allocator;
    // a = 1 OR b = 2 AND c = 3  =>  a = 1 OR (b = 2 AND c = 3)
    var parser = Parser.init("SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    const where = select.where.?;
    // Top level should be OR
    try std.testing.expectEqual(ast.Condition.or_op, std.meta.activeTag(where));
    // Right side of OR should be AND
    try std.testing.expectEqual(ast.Condition.and_op, std.meta.activeTag(where.or_op.right.*));

    parser.freeCondition(where);
    allocator.free(select.columns);
}

test "parse SELECT with parentheses in condition" {
    const allocator = std.testing.allocator;
    // (a = 1 OR b = 2) AND c = 3  =>  AND(OR(a=1, b=2), c=3)
    var parser = Parser.init("SELECT * FROM t WHERE (a = 1 OR b = 2) AND c = 3", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    const where = select.where.?;
    // Top level should be AND
    try std.testing.expectEqual(ast.Condition.and_op, std.meta.activeTag(where));
    // Left side of AND should be OR
    try std.testing.expectEqual(ast.Condition.or_op, std.meta.activeTag(where.and_op.left.*));

    parser.freeCondition(where);
    allocator.free(select.columns);
}

test "parse SELECT with nested parentheses" {
    const allocator = std.testing.allocator;
    // ((a = 1))
    var parser = Parser.init("SELECT * FROM t WHERE ((a = 1))", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    const where = select.where.?;
    // Should still be simple condition
    try std.testing.expectEqual(ast.Condition.simple, std.meta.activeTag(where));

    allocator.free(select.columns);
}

test "parse SELECT with NOT condition" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT * FROM t WHERE NOT a = 1", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    const where = select.where.?;
    try std.testing.expectEqual(ast.Condition.not_op, std.meta.activeTag(where));

    parser.freeCondition(where);
    allocator.free(select.columns);
}

test "parse SELECT with NOT and AND" {
    const allocator = std.testing.allocator;
    // NOT a = 1 AND b = 2  =>  AND(NOT(a=1), b=2)
    var parser = Parser.init("SELECT * FROM t WHERE NOT a = 1 AND b = 2", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    const where = select.where.?;
    try std.testing.expectEqual(ast.Condition.and_op, std.meta.activeTag(where));
    try std.testing.expectEqual(ast.Condition.not_op, std.meta.activeTag(where.and_op.left.*));

    parser.freeCondition(where);
    allocator.free(select.columns);
}

test "parse SELECT with string comparison" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT * FROM users WHERE name = 'alice'", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    const where = select.where.?;
    const simple = where.simple;
    try std.testing.expectEqualStrings("name", simple.column);
    try std.testing.expectEqual(ast.Operator.eq, simple.op);
    try std.testing.expectEqualStrings("alice", simple.value.string);

    allocator.free(select.columns);
}

test "parse SELECT with boolean comparison" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT * FROM users WHERE active = true", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    const where = select.where.?;
    const simple = where.simple;
    try std.testing.expectEqualStrings("active", simple.column);
    try std.testing.expectEqual(true, simple.value.boolean);

    allocator.free(select.columns);
}

test "parse SELECT with less than operator" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT * FROM t WHERE age < 18", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    const where = select.where.?;
    try std.testing.expectEqual(ast.Operator.lt, where.simple.op);

    allocator.free(select.columns);
}

test "parse SELECT with greater than operator" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT * FROM t WHERE age > 18", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    const where = select.where.?;
    try std.testing.expectEqual(ast.Operator.gt, where.simple.op);

    allocator.free(select.columns);
}

test "parse SELECT with not equal operator" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT * FROM t WHERE status != 0", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    const where = select.where.?;
    try std.testing.expectEqual(ast.Operator.neq, where.simple.op);

    allocator.free(select.columns);
}

test "parse SELECT with less than or equal operator" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("SELECT * FROM t WHERE age <= 18", allocator);

    const stmt = try parser.parse();
    const select = stmt.select;

    const where = select.where.?;
    try std.testing.expectEqual(ast.Operator.lte, where.simple.op);

    allocator.free(select.columns);
}

test "parse INSERT statement" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("INSERT INTO users VALUES (1, 'alice', true)", allocator);

    const stmt = try parser.parse();
    const insert = stmt.insert;

    try std.testing.expectEqualStrings("users", insert.table_name);
    try std.testing.expectEqual(@as(usize, 3), insert.values.len);
    try std.testing.expectEqual(@as(i64, 1), insert.values[0].integer);
    try std.testing.expectEqualStrings("alice", insert.values[1].string);
    try std.testing.expectEqual(true, insert.values[2].boolean);

    allocator.free(insert.values);
}

test "parse INSERT with single value" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("INSERT INTO t VALUES (42)", allocator);

    const stmt = try parser.parse();
    const insert = stmt.insert;

    try std.testing.expectEqualStrings("t", insert.table_name);
    try std.testing.expectEqual(@as(usize, 1), insert.values.len);
    try std.testing.expectEqual(@as(i64, 42), insert.values[0].integer);

    allocator.free(insert.values);
}

test "parse INSERT with string value" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("INSERT INTO t VALUES ('hello world')", allocator);

    const stmt = try parser.parse();
    const insert = stmt.insert;

    try std.testing.expectEqual(@as(usize, 1), insert.values.len);
    try std.testing.expectEqualStrings("hello world", insert.values[0].string);

    allocator.free(insert.values);
}

test "parse INSERT with false boolean" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("INSERT INTO t VALUES (false)", allocator);

    const stmt = try parser.parse();
    const insert = stmt.insert;

    try std.testing.expectEqual(@as(usize, 1), insert.values.len);
    try std.testing.expectEqual(false, insert.values[0].boolean);

    allocator.free(insert.values);
}

test "parse CREATE TABLE statement" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("CREATE TABLE users (id INT, name TEXT, active BOOL)", allocator);

    const stmt = try parser.parse();
    const create = stmt.create_table;

    try std.testing.expectEqualStrings("users", create.table_name);
    try std.testing.expectEqual(@as(usize, 3), create.columns.len);

    try std.testing.expectEqualStrings("id", create.columns[0].name);
    try std.testing.expectEqual(ast.DataType.INTEGER, create.columns[0].data_type);

    try std.testing.expectEqualStrings("name", create.columns[1].name);
    try std.testing.expectEqual(ast.DataType.TEXT, create.columns[1].data_type);

    try std.testing.expectEqualStrings("active", create.columns[2].name);
    try std.testing.expectEqual(ast.DataType.BOOL, create.columns[2].data_type);

    allocator.free(create.columns);
}

test "parse CREATE TABLE with single column" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("CREATE TABLE t (id INT)", allocator);

    const stmt = try parser.parse();
    const create = stmt.create_table;

    try std.testing.expectEqualStrings("t", create.table_name);
    try std.testing.expectEqual(@as(usize, 1), create.columns.len);
    try std.testing.expectEqualStrings("id", create.columns[0].name);
    try std.testing.expectEqual(ast.DataType.INTEGER, create.columns[0].data_type);

    allocator.free(create.columns);
}

test "parse CREATE TABLE with all types" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("CREATE TABLE t (a INT, b TEXT, c BOOL)", allocator);

    const stmt = try parser.parse();
    const create = stmt.create_table;

    try std.testing.expectEqual(@as(usize, 3), create.columns.len);
    try std.testing.expectEqual(ast.DataType.INTEGER, create.columns[0].data_type);
    try std.testing.expectEqual(ast.DataType.TEXT, create.columns[1].data_type);
    try std.testing.expectEqual(ast.DataType.BOOL, create.columns[2].data_type);

    allocator.free(create.columns);
}

test "parse error on invalid statement" {
    const allocator = std.testing.allocator;
    var parser = Parser.init("INVALID stuff", allocator);

    const result = parser.parse();
    try std.testing.expectError(error.UnexpectedToken, result);
}
