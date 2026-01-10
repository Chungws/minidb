# Phase 2: SQL Parser

SQL 문자열을 AST(Abstract Syntax Tree)로 변환.

## 학습 목표

- Lexer (토큰화) 구현
- Recursive Descent Parser 이해
- AST 설계

## 지원할 SQL 문법 (최소)

```sql
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users VALUES (1, 'alice');
SELECT * FROM users;
SELECT id, name FROM users WHERE age > 20;
```

### 지원 타입
- `INT` - 정수
- `TEXT` - 문자열
- `BOOL` - true/false

### 지원 연산자
- 비교: `=`, `<>`, `<`, `>`, `<=`, `>=`
- 논리: `AND`, `OR`, `NOT`

---

## Step 1: Token 정의

### 목표
SQL의 각 요소를 나타내는 토큰 타입 정의.

### 구현해야 할 것
- `TokenType` enum
  - 키워드: SELECT, FROM, WHERE, INSERT, INTO, VALUES, CREATE, TABLE, AND, OR, NOT
  - 타입: INT, TEXT, BOOL
  - 리터럴: 숫자, 문자열, true, false
  - 연산자: =, <>, <, >, <=, >=
  - 구두점: (, ), ,, ;, *
  - 식별자 (테이블명, 컬럼명)
- `Token` 구조체 (type, lexeme, line, column)

---

## Step 2: Lexer

### 목표
SQL 문자열을 토큰 스트림으로 변환.

### 구현해야 할 것
- `Lexer` 구조체
- `init(source)` - 소스 문자열로 초기화
- `nextToken()` - 다음 토큰 반환

### 처리해야 할 것
1. 공백 건너뛰기
2. 키워드 vs 식별자 구분 (대소문자 무시)
3. 문자열 리터럴 (`'hello'`)
4. 숫자 리터럴
5. 2글자 연산자 (`<=`, `>=`, `<>`, `!=`)

### 테스트 케이스
1. `SELECT * FROM users` → [SELECT, *, FROM, identifier("users"), EOF]
2. `WHERE age >= 18` → [WHERE, identifier("age"), >=, integer(18), EOF]
3. `'hello world'` → string("hello world")

### 힌트
- peek() / advance() 패턴 사용
- 키워드 맵: `std.StaticStringMap` 또는 직접 구현

---

## Step 3: AST 정의

### 목표
파싱 결과를 담을 트리 구조 설계.

### 구현해야 할 것

**Statement** (최상위)
```
Statement = CreateTable | Insert | Select
```

**CreateTable**
```
- table_name: 문자열
- columns: ColumnDef 배열
  - ColumnDef = { name, data_type }
```

**Insert**
```
- table_name: 문자열
- values: Value 배열
```

**Select**
```
- columns: 문자열 배열 (["*"] 또는 ["id", "name"])
- table_name: 문자열
- where: Expr? (optional)
```

**Expr** (WHERE 조건)
```
Expr = Column | Literal | Binary | Not

Binary = { left: Expr, op: BinaryOp, right: Expr }
BinaryOp = eq | neq | lt | gt | lte | gte | and | or
```

### 생각해볼 것
- Zig에서 재귀 타입을 어떻게 표현할까? (포인터 필요)
- 메모리 할당은 누가 담당? (Arena allocator 추천)

---

## Step 4: Parser

### 목표
토큰 스트림을 AST로 변환.

### 구현해야 할 것
- `Parser` 구조체
- `parse(source)` → Statement

### Recursive Descent 구조

```
parseStatement()
├── SELECT로 시작 → parseSelect()
├── CREATE로 시작 → parseCreateTable()
└── INSERT로 시작 → parseInsert()

parseSelect()
├── SELECT
├── parseColumnList()  // * 또는 col1, col2, ...
├── FROM
├── identifier (table)
└── WHERE? → parseExpr()

parseExpr()
└── parseOrExpr()
    └── parseAndExpr()
        └── parseComparison()
            └── parsePrimary()
```

### 테스트 케이스
1. `SELECT * FROM users`
   - columns: ["*"], table: "users", where: null
2. `SELECT id, name FROM users`
   - columns: ["id", "name"]
3. `CREATE TABLE foo (id INT, name TEXT)`
   - table: "foo", columns: [{id, INT}, {name, TEXT}]
4. `INSERT INTO foo VALUES (1, 'bar')`
   - table: "foo", values: [1, "bar"]
5. `SELECT * FROM users WHERE age > 20 AND active = true`
   - where: Binary(Binary(age > 20), AND, Binary(active = true))

### 에러 처리
- 예상치 못한 토큰 → UnexpectedToken 에러
- 파싱 실패 시 위치 정보 포함하면 좋음

---

## 디렉토리 구조

```
src/
└── sql/
    ├── mod.zig
    ├── token.zig
    ├── lexer.zig
    ├── ast.zig
    └── parser.zig
```

## 체크리스트

- [ ] Token 타입 정의
- [ ] Lexer 구현 및 테스트
- [ ] AST 구조 정의
- [ ] Parser 구현 및 테스트
- [ ] 모든 SQL 문법 파싱 동작

## 참고 자료

- Crafting Interpreters (온라인 무료) - Lexer/Parser 구현 참고
- 본인의 이전 컴파일러 경험 활용!

## 다음 단계

Phase 2 완료 후 → [Phase 3: Record & Heap](./phase-3-record-heap.md)
