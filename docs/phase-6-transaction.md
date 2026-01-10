# Phase 6: Transaction (선택)

ACID 보장을 위한 트랜잭션 관리.

## 학습 목표

- ACID 속성 이해
- WAL (Write-Ahead Logging)
- 락 기반 동시성 제어
- 복구 (Recovery) 기초

---

## 이론 배경

### ACID

```
Atomicity   - 전부 성공 또는 전부 실패
Consistency - 트랜잭션 전후 DB 일관성 유지
Isolation   - 동시 트랜잭션 간 간섭 없음
Durability  - 커밋된 데이터는 영구 저장
```

### WAL (Write-Ahead Logging)

```
규칙: 데이터 변경 전에 로그 먼저 기록

1. 로그에 변경 내용 기록
2. 로그 디스크에 flush
3. 그 후에 실제 데이터 페이지 수정

→ 크래시 시 로그로 복구 가능
```

### 동시성 제어

```
여러 트랜잭션이 동시에 실행될 때:
- Lost Update 방지
- Dirty Read 방지
- Non-repeatable Read 방지

방법:
- Lock-based (2PL)
- MVCC (Multi-Version Concurrency Control)
```

---

## Step 1: Transaction Manager

### 목표
트랜잭션 생명주기 관리.

### 구현해야 할 것
- `Transaction` 구조체 (txn_id, state)
- `TransactionManager`
  - begin() → txn_id
  - commit(txn_id)
  - abort(txn_id)

### 트랜잭션 상태

```
ACTIVE → COMMITTED
       ↘ ABORTED
```

---

## Step 2: WAL 기초

### 목표
변경 로그 기록 및 재생.

### Log Record 종류

```
BEGIN(txn_id)
INSERT(txn_id, table, rid, tuple_data)
DELETE(txn_id, table, rid, old_tuple)
UPDATE(txn_id, table, rid, old_tuple, new_tuple)
COMMIT(txn_id)
ABORT(txn_id)
```

### 구현해야 할 것
- `LogRecord` union
- `WAL` 구조체
  - append(record)
  - flush()
  - iterate() → 로그 순회

### 로그 포맷

```
[LSN:8][txn_id:4][type:1][payload_len:2][payload:N][checksum:4]

LSN = Log Sequence Number (증가하는 고유 번호)
```

### 테스트 케이스
1. 로그 기록 → flush → 다시 읽기
2. 여러 트랜잭션 로그 섞여 있을 때 분리
3. checksum 검증

---

## Step 3: 복구 (Recovery)

### 목표
크래시 후 일관된 상태로 복구.

### ARIES 복구 (단순화)

```
1. Analysis: 로그 스캔, 활성 트랜잭션 파악
2. Redo: 커밋된 변경 재적용
3. Undo: 미완료 트랜잭션 롤백
```

### 간단한 버전

```
recovery():
    committed_txns = set()
    changes = []

    # 1. 로그 스캔
    for record in wal:
        if record is COMMIT:
            committed_txns.add(record.txn_id)
        else:
            changes.append(record)

    # 2. 커밋된 것만 Redo
    for change in changes:
        if change.txn_id in committed_txns:
            apply(change)
```

### 테스트 케이스
1. 정상 종료 → 복구 불필요
2. 커밋 후 크래시 → 데이터 유지
3. 커밋 전 크래시 → 변경 사라짐

---

## Step 4: Lock Manager

### 목표
동시 접근 제어.

### 구현해야 할 것
- `LockMode` enum (Shared, Exclusive)
- `LockManager`
  - acquireLock(txn_id, rid, mode)
  - releaseLock(txn_id, rid)

### 2PL (Two-Phase Locking)

```
Growing Phase: 락 획득만 가능
Shrinking Phase: 락 해제만 가능

→ Serializability 보장
```

### 락 호환성

```
        | Shared | Exclusive |
--------|--------|-----------|
Shared  |   O    |     X     |
Exclusive|   X   |     X     |
```

### 생각해볼 것
- 락 대기 어떻게 처리?
- Deadlock 감지?

---

## Step 5: Deadlock 처리

### 방법 1: Timeout
```
락 대기 시간 초과 → 트랜잭션 abort
```

### 방법 2: Wait-Die / Wound-Wait
```
트랜잭션 나이에 따라 결정
- Wait-Die: 늙은 것만 기다림
- Wound-Wait: 늙은 것이 젊은 것 abort 시킴
```

### 방법 3: 그래프 탐지
```
Wait-for 그래프에서 사이클 탐지
사이클 발견 시 하나 abort
```

---

## Step 6: MVCC (고급, 선택)

### 개념

```
읽기가 쓰기를 막지 않음
각 레코드의 여러 버전 유지

트랜잭션은 시작 시점의 "스냅샷"을 봄
```

### 버전 체인

```
Record:
  [v3: txn=5, data] → [v2: txn=3, data] → [v1: txn=1, data]
```

### 구현 복잡도가 높음 - 도전 과제로!

---

## 디렉토리 구조

```
src/
└── tx/
    ├── mod.zig
    ├── transaction.zig
    ├── wal.zig
    ├── recovery.zig
    └── lock.zig
```

## 체크리스트

- [ ] Transaction 상태 관리
- [ ] WAL 기본 구현
- [ ] Recovery 구현
- [ ] Lock Manager 구현
- [ ] (도전) Deadlock 처리
- [ ] (도전) MVCC

## 참고 자료

- Database Internals, Chapter 5 (Transaction Processing)
- CMU 15-445 Lecture 15-17: Concurrency Control
- "ARIES: A Transaction Recovery Method" (논문)

## 축하합니다!

Phase 6까지 완료하면 본격적인 DBMS의 핵심을 모두 구현한 것입니다!

### 추가 도전 과제
- Query Optimizer (cost-based)
- Hash Join, Sort-Merge Join
- 분산 처리 (Raft 합의)
- 네트워크 프로토콜 (PostgreSQL wire protocol)
