# MiniDB

Zig로 처음부터 구현하는 관계형 데이터베이스 시스템.

## 개요

교육 목적으로 DBMS의 핵심 컴포넌트를 직접 구현합니다.

- **언어**: Zig
- **방법론**: TDD (Test-Driven Development)
- **목표**: 전통적 DBMS 내부 구조 학습

## 최종 목표

```sql
CREATE TABLE users (id INT, name TEXT, age INT);
INSERT INTO users VALUES (1, 'Alice', 30);
INSERT INTO users VALUES (2, 'Bob', 25);

SELECT * FROM users;
SELECT name FROM users WHERE age > 20;
SELECT * FROM users WHERE id = 1;  -- B+Tree 인덱스 사용
```

## 로드맵

```
Phase 0: Setup           - Zig 환경, 프로젝트 구조
Phase 1: Storage         - Page, DiskManager, BufferPool
Phase 2: SQL Parser      - Lexer, Parser, AST
Phase 3: Record & Heap   - Tuple, SlottedPage, HeapFile
Phase 4: B+Tree Index    - 검색, 삽입, Split
Phase 5: Query Executor  - Volcano Model, Scan/Filter/Project
Phase 6: Transaction     - WAL, Lock Manager, Recovery
```

자세한 내용은 [docs/](./docs/README.md) 참조.

## 빌드 및 실행

```bash
# 빌드
zig build

# 실행
zig build run

# 테스트
zig build test
```

## 프로젝트 구조

```
minidb/
├── build.zig
├── src/
│   ├── main.zig
│   ├── storage/    # Page, DiskManager, BufferPool
│   ├── sql/        # Lexer, Parser, AST
│   ├── record/     # Tuple, SlottedPage, HeapFile
│   ├── index/      # B+Tree
│   ├── query/      # Executor, Planner
│   └── tx/         # Transaction, WAL, Lock
└── docs/
```

## 참고 자료

- Database Internals (Alex Petrov)
- CMU 15-445/645 Database Systems
- https://ziglang.org/documentation

## 진행 상황

- [ ] Phase 0: Setup
- [ ] Phase 1: Storage
- [ ] Phase 2: SQL Parser
- [ ] Phase 3: Record & Heap
- [ ] Phase 4: B+Tree
- [ ] Phase 5: Query Executor
- [ ] Phase 6: Transaction
