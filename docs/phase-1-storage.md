# Phase 1: Storage

디스크 I/O와 버퍼 관리 - DBMS의 가장 기초가 되는 레이어.

## 학습 목표

- 왜 DB는 OS의 파일 시스템을 그대로 쓰지 않는가
- 페이지(블록) 단위 I/O의 필요성
- Buffer Pool과 캐싱 전략
- LRU 페이지 교체 정책

## 이론 배경

### 왜 페이지 단위인가?

```
디스크 특성:
- Random Access가 느림
- 순차 읽기가 빠름
- 최소 읽기 단위가 있음 (보통 512B ~ 4KB)

→ DB는 4KB 페이지 단위로 읽기/쓰기
→ OS 페이지 캐시 대신 자체 Buffer Pool 사용 (더 똑똑한 관리)
```

### Buffer Pool 개념

```
┌─────────────────────────────────────────┐
│             Buffer Pool                  │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐   │
│  │Frame0│ │Frame1│ │Frame2│ │Frame3│   │
│  │Page5 │ │Page2 │ │ Free │ │Page9 │   │
│  └──────┘ └──────┘ └──────┘ └──────┘   │
└─────────────────────────────────────────┘
        ▲                   │
        │ read              │ write (flush)
        │                   ▼
┌─────────────────────────────────────────┐
│              Disk File                   │
│  [Page0][Page1][Page2]...[Page9]...     │
└─────────────────────────────────────────┘
```

---

## Step 1: Page

### 목표
고정 크기(4KB) 바이트 블록을 표현하는 구조체.

### 구현해야 할 것
- `PAGE_SIZE` 상수 (4096)
- `Page` 구조체
- `init()` - 0으로 초기화된 페이지 생성
- `read(offset, len)` - 특정 위치에서 바이트 읽기
- `write(offset, bytes)` - 특정 위치에 바이트 쓰기

### 테스트 케이스
1. 새 페이지는 모든 바이트가 0이어야 함
2. write 후 같은 위치에서 read하면 동일한 값
3. offset을 다르게 해서 여러 위치에 쓰기/읽기

### 힌트
- Zig의 `@memcpy` 사용
- 배열 슬라이싱: `arr[start..end]`

---

## Step 2: DiskManager

### 목표
파일에 페이지 단위로 읽기/쓰기.

### 구현해야 할 것
- `DiskManager` 구조체
- `init(path)` - 파일 열기/생성
- `deinit()` - 파일 닫기
- `readPage(page_id, *Page)` - page_id 위치에서 읽기
- `writePage(page_id, *Page)` - page_id 위치에 쓰기

### 핵심 개념
```
파일 내 위치 = page_id * PAGE_SIZE

Page 0: offset 0 ~ 4095
Page 1: offset 4096 ~ 8191
Page 2: offset 8192 ~ 12287
...
```

### 테스트 케이스
1. 페이지 쓰고 읽으면 동일한 내용
2. 여러 페이지를 비순차적으로 쓰고 읽기 (예: page 0, page 5, page 2)
3. 파일 닫았다 다시 열어도 데이터 유지

### 힌트
- `std.fs.File`의 `seekTo`, `readAll`, `writeAll` 사용
- 테스트 후 파일 정리: `defer std.fs.cwd().deleteFile(path) catch {};`

---

## Step 3: BufferPool

### 목표
메모리에 페이지 캐싱 + LRU 교체 정책.

### 구현해야 할 것
- `Frame` 구조체 (page, page_id, pin_count, is_dirty)
- `BufferPool` 구조체
- `fetchPage(page_id)` - 페이지 가져오기 (캐시 또는 디스크)
- `unpinPage(page_id, is_dirty)` - 사용 완료 표시
- `flushPage(page_id)` - dirty 페이지를 디스크에 쓰기

### 핵심 개념

**Pin/Unpin:**
```
fetchPage → pin_count++ (사용 중)
unpinPage → pin_count-- (사용 완료)
pin_count > 0인 페이지는 evict 불가
```

**Dirty Flag:**
```
페이지 수정됨 → dirty = true
evict 전에 dirty면 디스크에 flush
```

**LRU (Least Recently Used):**
```
교체 대상 선택: pin_count == 0 중 가장 오래 안 쓴 것
```

### 테스트 케이스
1. fetch → 수정 → unpin(dirty=true) → 다시 fetch → 수정 내용 유지
2. pool 크기보다 많은 페이지 fetch → eviction 발생
3. 모든 페이지가 pinned 상태에서 새 페이지 fetch → 에러

### 생각해볼 것
- page_table (page_id → frame_id 매핑)을 어떤 자료구조로?
- LRU 추적을 어떻게 효율적으로?
- evict할 때 dirty 페이지 처리

---

## 디렉토리 구조

```
src/
└── storage/
    ├── mod.zig      # 모듈 루트
    ├── page.zig
    ├── disk.zig
    └── buffer.zig
```

## 체크리스트

- [ ] Page 구현 및 테스트
- [ ] DiskManager 구현 및 테스트
- [ ] BufferPool 구현 및 테스트
- [ ] `zig build test` 전체 통과

## 참고 자료

- Database Internals, Chapter 3-5
- CMU 15-445 Lecture 5: Buffer Pools
- CMU 15-445 Lecture 6: Hash Tables

## 다음 단계

Phase 1 완료 후 → [Phase 2: SQL Parser](./phase-2-sql-parser.md)
