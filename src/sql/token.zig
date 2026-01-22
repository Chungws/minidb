pub const TokenType = enum {
    // Keywords
    select,
    from,
    where,
    insert,
    into,
    values,
    create,
    table,
    index,
    on,

    // Types
    int_type, // INT
    text_type, // TEXT
    bool_type, // BOOL

    // Literals
    integer, // 123
    string, // 'hello'
    true_lit,
    false_lit,
    null_lit,

    // Operators
    eq, // =
    neq, // <> or !=
    lt, // <
    gt, // >
    lte, // <=
    gte, // >=

    // Logical
    and_op,
    or_op,
    not_op,

    // Punctuation
    lparen, // (
    rparen, // )
    comma, // ,
    semicolon, // ;
    asterisk, // *

    // Other
    identifier, // 테이블명, 컬럼명
    eof,
    illegal,
};

pub const Token = struct {
    type: TokenType,
    lexeme: []const u8,
};
