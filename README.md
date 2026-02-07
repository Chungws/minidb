# MiniDB

Zig로 처음부터 구현하는 관계형 데이터베이스 시스템.

## 개요

교육 목적으로 DBMS의 핵심 컴포넌트를 직접 구현합니다.

- **언어**: Zig 0.15
- **방법론**: TDD (Test-Driven Development)
- **목표**: 전통적 DBMS 내부 구조 학습
- **테스트**: 232개 유닛 테스트

## 사용 예시

```sql
-- 테이블 생성
CREATE TABLE users (id INT NOT NULL, name TEXT, age INT);
CREATE TABLE orders (order_id INT NOT NULL, user_id INT NOT NULL);

-- 데이터 삽입
INSERT INTO users VALUES (1, 'Alice', 30);
INSERT INTO users VALUES (2, 'Bob', 25);

-- 조회
SELECT * FROM users;
SELECT name, age FROM users WHERE age > 20;

-- 인덱스 생성 및 사용
CREATE INDEX idx_id ON users(id);
SELECT * FROM users WHERE id = 1;  -- IndexScan 사용

-- JOIN
SELECT * FROM users JOIN orders ON id = user_id;

-- 트랜잭션
BEGIN;
INSERT INTO users VALUES (3, 'Charlie', 28);
COMMIT;
```

## 빌드 및 실행

```bash
# 빌드
zig build

# REPL 실행
zig build run

# 테스트
zig build test
```

## 아키텍처

```
┌─────────────────────────────────────────────────────────┐
│                      REPL / Session                      │
├─────────────────────────────────────────────────────────┤
│  SQL Parser          │  Query Planner    │  Executor    │
│  (Lexer → AST)       │  (Plan 생성)       │  (Volcano)   │
├─────────────────────────────────────────────────────────┤
│  Catalog             │  Table            │  B+Tree      │
│  (테이블 관리)         │  (스키마+힙)       │  (인덱스)     │
├─────────────────────────────────────────────────────────┤
│  HeapFile            │  SlottedPage      │  Tuple       │
│  (레코드 저장)         │  (가변길이)        │  (행 표현)    │
├─────────────────────────────────────────────────────────┤
│  BufferPool          │  DiskManager      │  Page        │
│  (페이지 캐싱)         │  (파일 I/O)        │  (4KB)       │
├─────────────────────────────────────────────────────────┤
│  Transaction         │  WAL              │  LockManager │
│  (ACID)              │  (복구)            │  (동시성)     │
└─────────────────────────────────────────────────────────┘
```

## 구현된 기능

### Storage Layer
- **Page**: 4KB 고정 크기 페이지
- **DiskManager**: 파일 기반 페이지 읽기/쓰기
- **BufferPool**: LRU 기반 페이지 캐싱, pin/unpin 관리

### SQL Layer
- **Lexer**: SQL 토큰화
- **Parser**: 재귀 하강 파서
- **지원 구문**: CREATE TABLE, CREATE INDEX, INSERT, SELECT, BEGIN, COMMIT, ABORT
- **조건**: WHERE (=, <, >, <=, >=, !=, AND, OR, NOT)
- **JOIN**: INNER JOIN with ON clause

### Record Layer
- **Tuple**: 행 데이터 표현 (INTEGER, TEXT, BOOLEAN, NULL)
- **Schema**: 컬럼 정의 (이름, 타입, nullable)
- **SlottedPage**: 가변 길이 레코드 저장, 자동 압축
- **HeapFile**: BufferPool 기반 레코드 관리
- **Table**: 테이블별 독립 파일 (PostgreSQL 방식)

### Index Layer
- **B+Tree**: 균형 트리 인덱스
- **연산**: search, insert, range scan
- **자동 분할**: 노드 오버플로우 시 split

### Query Layer
- **Executor**: Volcano Model (iterator 기반)
  - SeqScan: 순차 스캔
  - IndexScan: 인덱스 스캔
  - Filter: WHERE 조건 필터
  - Project: 컬럼 선택
  - NestedLoopJoin: 조인
- **Planner**: 쿼리 계획 수립, 인덱스 선택 최적화
- **Catalog**: 테이블 메타데이터 관리

### Transaction Layer
- **TransactionManager**: 트랜잭션 생명주기 관리
- **WAL**: Write-Ahead Logging (메모리 기반)
- **LockManager**: Shared/Exclusive 락
- **Recovery**: WAL 기반 크래시 복구
- **Session**: SQL 실행 세션, REPL

## 프로젝트 구조

```
minidb/
├── build.zig
├── src/
│   ├── main.zig          # REPL 진입점
│   ├── lib.zig           # 라이브러리 루트
│   ├── storage/
│   │   ├── page.zig      # 4KB 페이지
│   │   ├── disk.zig      # 디스크 매니저
│   │   └── buffer.zig    # 버퍼 풀
│   ├── sql/
│   │   ├── token.zig     # 토큰 정의
│   │   ├── lexer.zig     # 렉서
│   │   ├── ast.zig       # AST 노드
│   │   └── parser.zig    # 파서
│   ├── record/
│   │   ├── tuple.zig     # 튜플, 스키마
│   │   ├── slot.zig      # 슬롯 페이지
│   │   ├── heap.zig      # 힙 파일
│   │   └── table.zig     # 테이블
│   ├── index/
│   │   ├── btree.zig     # B+Tree
│   │   └── node.zig      # 트리 노드
│   ├── query/
│   │   ├── executor.zig  # 실행기
│   │   ├── planner.zig   # 플래너
│   │   └── catalog.zig   # 카탈로그
│   └── tx/
│       ├── transaction.zig  # 트랜잭션
│       ├── wal.zig          # WAL
│       ├── lock.zig         # 락 매니저
│       ├── recovery.zig     # 복구
│       └── session.zig      # 세션
└── docs/
```

## 참고 자료

- Database Internals (Alex Petrov)
- CMU 15-445/645 Database Systems
- Architecture of a Database System (Hellerstein)
- https://ziglang.org/documentation

## 진행 상황

- [x] Phase 1: Storage - Page, DiskManager, BufferPool
- [x] Phase 2: SQL Parser - Lexer, Parser, AST
- [x] Phase 3: Record & Heap - Tuple, SlottedPage, HeapFile, Table
- [x] Phase 4: B+Tree Index - Search, Insert, Range Scan
- [x] Phase 5: Query Executor - Volcano Model, Planner, JOIN
- [x] Phase 6: Transaction - WAL, Lock Manager, Recovery, Session
