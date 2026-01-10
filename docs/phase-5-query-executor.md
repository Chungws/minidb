# Phase 5: Query Executor

SQL 쿼리를 실제로 실행하는 엔진.

## 학습 목표

- Volcano (Iterator) 모델 이해
- 실행 계획 (Plan) 개념
- 다양한 연산자 구현

---

## 이론 배경

### Volcano Model

```
각 연산자가 Iterator 인터페이스 구현:
- open()  : 초기화
- next()  : 다음 튜플 반환 (없으면 null)
- close() : 정리

데이터가 "pull" 방식으로 위로 흐름
```

### 실행 계획 예시

```sql
SELECT name FROM users WHERE age > 20
```

```
      Project (name)
           │
       Filter (age > 20)
           │
      SeqScan (users)
```

```
호출 흐름:
Project.next()
  → Filter.next()
      → SeqScan.next() → tuple
      → age > 20?
        - yes: 반환
        - no: 다시 SeqScan.next()
  → tuple에서 name 추출
→ 결과 반환
```

---

## Step 1: Executor 인터페이스

### 목표
모든 연산자가 구현할 공통 인터페이스.

### 구현해야 할 것

```
Executor interface:
- init()
- next() → Tuple?
- close()
```

### Zig에서 인터페이스

방법 1: 함수 포인터
```zig
const Executor = struct {
    nextFn: *const fn(*Executor) ?Tuple,
    // ...
};
```

방법 2: Tagged union
```zig
const Executor = union(enum) {
    seq_scan: SeqScan,
    filter: Filter,
    // ...
};
```

직접 선택해보세요!

---

## Step 2: SeqScan (Sequential Scan)

### 목표
테이블의 모든 레코드를 순차 조회.

### 구현해야 할 것
- 테이블 (HeapFile) 참조
- 현재 위치 추적
- next() 호출마다 다음 튜플 반환

### 테스트 케이스
1. 빈 테이블 → 즉시 null
2. 3개 레코드 → next() 3번 성공, 4번째 null
3. close() 후 리소스 정리

---

## Step 3: Filter

### 목표
조건에 맞는 튜플만 통과.

### 구현해야 할 것
- child: 하위 Executor
- predicate: 조건 (Expr)
- next(): child에서 가져와서 조건 평가

### 조건 평가

```
evaluate(expr, tuple) → Value

expr이 Column이면: tuple에서 해당 컬럼 값
expr이 Literal이면: 그 값
expr이 Binary이면: left op right 계산
```

### 테스트 케이스
1. 모든 레코드 통과하는 조건
2. 일부만 통과
3. 아무것도 통과 안 함
4. AND/OR 복합 조건

---

## Step 4: Project

### 목표
특정 컬럼만 선택.

### 구현해야 할 것
- child: 하위 Executor
- columns: 선택할 컬럼 목록
- next(): 튜플에서 해당 컬럼만 추출

### 테스트 케이스
1. SELECT * → 모든 컬럼
2. SELECT id, name → 2개 컬럼만
3. 컬럼 순서 변경

---

## Step 5: 실행 계획 생성

### 목표
AST → 실행 계획 (Executor 트리)

### 구현해야 할 것
- `Planner` 또는 `planQuery(stmt)` 함수
- Select AST → SeqScan + Filter + Project 조합

### 계획 생성 로직

```
planSelect(select):
    // 1. 베이스: 테이블 스캔
    plan = SeqScan(select.table_name)

    // 2. WHERE 있으면 Filter 추가
    if select.where:
        plan = Filter(plan, select.where)

    // 3. 컬럼 선택
    if select.columns != ["*"]:
        plan = Project(plan, select.columns)

    return plan
```

---

## Step 6: INSERT/CREATE 실행

### INSERT

```
executeInsert(insert):
    table = catalog.getTable(insert.table_name)
    tuple = Tuple(insert.values)
    rid = table.heap.insert(tuple)
    return "1 row inserted"
```

### CREATE TABLE

```
executeCreate(create):
    schema = Schema(create.columns)
    table = Table(create.table_name, schema)
    catalog.addTable(table)
    return "Table created"
```

---

## Step 7: (도전) NestedLoopJoin

### 목표
두 테이블 조인.

```sql
SELECT * FROM users, orders WHERE users.id = orders.user_id
```

### 알고리즘

```
for each tuple in left:
    for each tuple in right:
        if join_condition(left_tuple, right_tuple):
            emit combined_tuple
```

### 생각해볼 것
- right를 매번 처음부터 스캔해야 함 (비효율)
- 개선: Index Nested Loop Join (인덱스 사용)

---

## Step 8: REPL 완성

### 목표
사용자 입력 → 파싱 → 실행 → 결과 출력

```
minidb> CREATE TABLE users (id INT, name TEXT);
Table created.

minidb> INSERT INTO users VALUES (1, 'Alice');
1 row inserted.

minidb> SELECT * FROM users;
| id | name  |
|----|-------|
| 1  | Alice |
1 row returned.
```

---

## 디렉토리 구조

```
src/
└── query/
    ├── mod.zig
    ├── executor.zig    # 인터페이스 + 기본 연산자
    ├── planner.zig     # AST → Plan
    └── eval.zig        # 표현식 평가
```

## 체크리스트

- [ ] Executor 인터페이스 정의
- [ ] SeqScan 구현
- [ ] Filter 구현
- [ ] Project 구현
- [ ] Planner 구현
- [ ] INSERT/CREATE 실행
- [ ] REPL 완성
- [ ] (도전) Join 구현

## 참고 자료

- CMU 15-445 Lecture 11-12: Query Execution
- "Volcano - An Extensible and Parallel Query Evaluation System" (논문)

## 다음 단계

Phase 5 완료 후 → [Phase 6: Transaction](./phase-6-transaction.md) (선택)
