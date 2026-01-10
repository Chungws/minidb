# MiniDB - Zig로 만드는 DBMS

교육 목적으로 처음부터 구현하는 관계형 데이터베이스 시스템.

## 기술 스택

- **언어**: Zig
- **방법론**: TDD (Test-Driven Development)
- **목표**: 전통적 DBMS 내부 구조 학습

## 로드맵 개요

```
Phase 0: Setup
    │
    ▼
Phase 1: Storage ──────────────────┐
    │   - Page                     │
    │   - DiskManager              │
    │   - BufferPool               │
    │                              │
    ▼                              │
Phase 2: SQL Parser                │  Core
    │   - Lexer                    │  Foundation
    │   - Parser                   │
    │   - AST                      │
    │                              │
    ▼                              │
Phase 3: Record & Heap ────────────┘
    │   - Tuple
    │   - SlottedPage
    │   - HeapFile
    │
    ▼
Phase 4: B+Tree Index
    │   - Node 구조
    │   - 검색
    │   - 삽입 & Split
    │
    ▼
Phase 5: Query Executor
    │   - Volcano Model
    │   - Scan, Filter, Project
    │   - Join
    │
    ▼
Phase 6: Transaction (선택)
        - WAL
        - Lock Manager
        - Recovery
```

## Phase 상세 문서

| Phase | 문서 | 핵심 학습 내용 |
|-------|------|---------------|
| 0 | [Setup](./phase-0-setup.md) | Zig 환경, 프로젝트 구조 |
| 1 | [Storage](./phase-1-storage.md) | 페이지, 버퍼 관리, 디스크 I/O |
| 2 | [SQL Parser](./phase-2-sql-parser.md) | Lexer, Parser, AST |
| 3 | [Record & Heap](./phase-3-record-heap.md) | 레코드 저장, Slotted Page |
| 4 | [B+Tree](./phase-4-btree.md) | 인덱스 구조, 검색/삽입 |
| 5 | [Query Executor](./phase-5-query-executor.md) | 실행 계획, Iterator 모델 |
| 6 | [Transaction](./phase-6-transaction.md) | ACID, WAL, 동시성 |

## 최종 목표

```sql
-- 이 쿼리들이 동작하는 DB 만들기
CREATE TABLE users (id INT, name TEXT, age INT);
INSERT INTO users VALUES (1, 'Alice', 30);
INSERT INTO users VALUES (2, 'Bob', 25);

SELECT * FROM users;
SELECT name FROM users WHERE age > 20;
SELECT * FROM users WHERE id = 1;  -- B+Tree 인덱스 사용
```

## 참고 자료

### 책
- Database Internals (Alex Petrov)
- Architecture of a Database System (Hellerstein)

### 강의
- CMU 15-445/645 Database Systems

### Zig
- https://ziglearn.org
- https://ziglang.org/documentation

## 진행 상황

- [ ] Phase 0: Setup
- [ ] Phase 1: Storage
- [ ] Phase 2: SQL Parser
- [ ] Phase 3: Record & Heap
- [ ] Phase 4: B+Tree
- [ ] Phase 5: Query Executor
- [ ] Phase 6: Transaction
