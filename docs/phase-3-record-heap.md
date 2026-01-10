# Phase 3: Record & Heap

테이블 데이터를 페이지에 저장하는 방법.

## 학습 목표

- Tuple(레코드) 직렬화/역직렬화
- Slotted Page 구조 이해
- Heap File 구조 이해
- Record ID (RID) 개념

---

## 이론 배경

### Slotted Page 구조

```
┌────────────────────────────────────────────────┐
│ Header │ Slot1 │ Slot2 │ Slot3 │  ...  │ Free │
├────────────────────────────────────────────────┤
│                    Free Space                   │
├────────────────────────────────────────────────┤
│ Record3 │    Record2    │      Record1         │
└────────────────────────────────────────────────┘
         ◀──────── 레코드는 뒤에서부터 ──────────

Slot = { offset, length } - 레코드 위치 정보
```

### 왜 Slotted Page?

- 가변 길이 레코드 지원 (TEXT 컬럼 등)
- 레코드 삭제 시 재정렬 없이 slot만 무효화
- 레코드 이동해도 slot만 업데이트 (외부 참조 유지)

### Record ID (RID)

```
RID = (page_id, slot_id)

테이블 내에서 레코드를 유일하게 식별
인덱스가 가리키는 대상
```

---

## Step 1: Tuple

### 목표
레코드(행)를 바이트로 직렬화/역직렬화.

### 구현해야 할 것
- `Value` union (Int, Text, Bool, Null)
- `Tuple` 구조체
- `serialize()` → 바이트 배열
- `deserialize(bytes, schema)` → Tuple

### 직렬화 포맷 설계 (직접 결정!)

생각해볼 것:
- NULL은 어떻게 표현?
- 가변 길이 TEXT는 어떻게?
- 필드 개수/타입 정보 포함 여부?

예시 (한 가지 방법):
```
[null_bitmap][field1][field2][field3]...

고정 길이: INT(4), BOOL(1)
가변 길이: [length:2][data:N]
```

### 테스트 케이스
1. (INT, TEXT, BOOL) 튜플 직렬화 → 역직렬화 → 동일
2. NULL 값 포함된 튜플
3. 빈 문자열, 긴 문자열

---

## Step 2: Slotted Page

### 목표
한 페이지 내에 여러 레코드 저장/관리.

### 구현해야 할 것
- `SlottedPage` 구조체
- `insert(tuple_bytes)` → slot_id
- `get(slot_id)` → bytes?
- `delete(slot_id)`
- `freeSpace()` → 남은 공간

### 페이지 레이아웃

```
Offset 0:
┌──────────────────────────────────┐
│ Header (고정)                    │
│ - num_slots: u16                │
│ - free_space_start: u16         │
│ - free_space_end: u16           │
├──────────────────────────────────┤
│ Slot Array (앞에서 뒤로 성장)    │
│ [offset:u16, length:u16] × N    │
├──────────────────────────────────┤
│         Free Space               │
├──────────────────────────────────┤
│ Records (뒤에서 앞으로 성장)     │
└──────────────────────────────────┘
                          Offset 4095
```

### 테스트 케이스
1. 빈 페이지에 레코드 삽입 → slot_id 반환
2. 여러 레코드 삽입 → 각각 조회 가능
3. 중간 레코드 삭제 → 해당 slot 조회 시 null
4. 공간 부족 시 에러

### 생각해볼 것
- 삭제된 slot 재사용?
- Compaction (조각 모음) 필요성?

---

## Step 3: Heap File

### 목표
여러 페이지로 구성된 테이블 저장소.

### 구현해야 할 것
- `HeapFile` 구조체
- `insert(tuple)` → RID
- `get(rid)` → Tuple?
- `delete(rid)`
- `scan()` → Iterator (모든 레코드 순회)

### 개념

```
Heap File = 페이지들의 연결

┌────────┐  ┌────────┐  ┌────────┐
│ Page 0 │→ │ Page 1 │→ │ Page 2 │→ ...
└────────┘  └────────┘  └────────┘

RID(1, 3) = Page 1의 Slot 3
```

### 페이지 선택 전략
INSERT 시 어느 페이지에 넣을까?
- 간단: 마지막 페이지, 꽉 차면 새 페이지
- 개선: Free Space Map 유지

### 테스트 케이스
1. 여러 레코드 insert → scan으로 전체 조회
2. RID로 특정 레코드 조회
3. 레코드 삭제 후 scan에서 제외
4. 페이지 가득 참 → 새 페이지 생성

---

## Step 4: Table (메타데이터)

### 목표
테이블 스키마 + HeapFile 연결.

### 구현해야 할 것
- `Schema` 구조체 (컬럼 정의 배열)
- `Table` 구조체
  - name
  - schema
  - heap_file

### 스키마 정보
```
Column = { name, data_type, nullable }
Schema = [Column, Column, ...]
```

### 생각해볼 것
- 스키마 정보는 어디에 저장? (시스템 테이블? 파일 헤더?)
- 테이블 목록 관리 (Catalog)

---

## 디렉토리 구조

```
src/
└── record/
    ├── mod.zig
    ├── tuple.zig
    ├── slot.zig      # SlottedPage
    ├── heap.zig      # HeapFile
    └── table.zig
```

## 체크리스트

- [ ] Tuple 직렬화/역직렬화
- [ ] SlottedPage 구현
- [ ] HeapFile 구현
- [ ] 전체 테스트 통과

## 참고 자료

- Database Internals, Chapter 3 (File Formats)
- CMU 15-445 Lecture 4: Database Storage

## 다음 단계

Phase 3 완료 후 → [Phase 4: B+Tree](./phase-4-btree.md)
