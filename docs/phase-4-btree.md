# Phase 4: B+Tree Index

효율적인 검색을 위한 인덱스 구조.

## 학습 목표

- B+Tree 구조와 속성 이해
- 검색, 삽입, (선택) 삭제 구현
- 디스크 기반 트리의 특성

---

## 이론 배경

### B+Tree vs B-Tree

```
B-Tree: 모든 노드에 데이터 저장
B+Tree: Leaf 노드에만 데이터, Internal은 키만

B+Tree 장점:
- Leaf 연결 → 범위 스캔 효율적
- Internal 노드가 더 많은 키 보유 → 트리 높이 낮음
- 모든 검색이 Leaf까지 → 예측 가능한 성능
```

### B+Tree 구조

```
                    [30|60]              ← Internal (Root)
                   /   |   \
            [10|20] [40|50] [70|80]      ← Internal
            /  |  \
         [5,8][12,15,18][22,25,28]       ← Leaf (연결됨)
              ↓
         각 Leaf는 RID를 가리킴
```

### 속성 (Order = M일 때)

```
Internal Node:
- 최대 M개 자식
- 최소 ⌈M/2⌉개 자식 (root 제외)
- 키 개수 = 자식 수 - 1

Leaf Node:
- 최대 M-1개 키
- 최소 ⌈(M-1)/2⌉개 키
- 다음 Leaf 포인터
```

---

## Step 1: Node 구조

### 목표
Internal/Leaf 노드를 페이지에 저장.

### 구현해야 할 것
- `NodeType` enum (Internal, Leaf)
- `Node` 구조체
- 페이지 ↔ 노드 변환

### 노드 레이아웃 설계

**Internal Node:**
```
┌─────────────────────────────────────────┐
│ Header: type(1) | num_keys(2) | ...     │
├─────────────────────────────────────────┤
│ [child_0] [key_0] [child_1] [key_1] ... │
└─────────────────────────────────────────┘
```

**Leaf Node:**
```
┌─────────────────────────────────────────┐
│ Header: type(1) | num_keys(2) | next(4) │
├─────────────────────────────────────────┤
│ [key_0, rid_0] [key_1, rid_1] ...       │
└─────────────────────────────────────────┘
```

### 생각해볼 것
- 키 타입을 어떻게 일반화? (INT만 먼저?)
- Order(M)를 어떻게 결정? (페이지 크기 기반)

---

## Step 2: 검색 (Search)

### 목표
키로 RID 찾기.

### 알고리즘

```
search(key):
    node = root
    while node is Internal:
        i = findChildIndex(node, key)
        node = getChild(node, i)

    # node is Leaf
    return findInLeaf(node, key)
```

### findChildIndex 로직

```
keys: [10, 20, 30]
children: [c0, c1, c2, c3]

key < 10  → c0
10 ≤ key < 20 → c1
20 ≤ key < 30 → c2
key ≥ 30 → c3
```

### 테스트 케이스
1. 존재하는 키 검색 → RID 반환
2. 존재하지 않는 키 → null
3. 빈 트리 검색

---

## Step 3: 삽입 (Insert)

### 목표
키-RID 쌍 삽입, 필요시 분할.

### 알고리즘 (단순화)

```
insert(key, rid):
    leaf = findLeaf(key)

    if leaf has space:
        insertInLeaf(leaf, key, rid)
    else:
        # Split!
        (new_leaf, middle_key) = splitLeaf(leaf, key, rid)
        insertInParent(leaf, middle_key, new_leaf)
```

### Split 동작

**Leaf Split:**
```
Before: [10, 20, 30, 40] (꽉 참)
Insert: 25

After:
  [10, 20]  [25, 30, 40]
       ↑
    middle_key = 25가 부모로
```

**Internal Split:**
```
부모도 꽉 차면 재귀적으로 split
Root가 split되면 새 Root 생성 (트리 높이 증가)
```

### 테스트 케이스
1. 빈 트리에 삽입 → Leaf 하나
2. 순차 삽입 (1, 2, 3, ...) → 분할 발생
3. 역순 삽입 → 분할 발생
4. 랜덤 삽입 → 트리 구조 유지

---

## Step 4: 범위 스캔 (Range Scan)

### 목표
범위 내 모든 키-RID 조회.

### 알고리즘

```
rangeScan(start_key, end_key):
    leaf = findLeaf(start_key)
    results = []

    while leaf != null:
        for (key, rid) in leaf:
            if key > end_key: return results
            if key >= start_key:
                results.append((key, rid))
        leaf = leaf.next

    return results
```

### 테스트 케이스
1. 전체 범위 스캔
2. 부분 범위 스캔
3. 빈 범위 (결과 없음)

---

## Step 5: 삭제 (선택)

삭제는 복잡합니다. 기본 구현 후 도전해보세요.

### 개념만
- Leaf에서 키 삭제
- Underflow 시: 형제에서 빌리기 또는 병합
- 병합이 전파될 수 있음

---

## 디렉토리 구조

```
src/
└── index/
    ├── mod.zig
    └── btree.zig
```

## 체크리스트

- [ ] Node 구조 및 직렬화
- [ ] Search 구현
- [ ] Insert 구현 (split 포함)
- [ ] Range Scan 구현
- [ ] (선택) Delete 구현

## 디버깅 팁

트리 시각화 함수 만들기:
```
printTree():
       [30]
      /    \
   [10,20] [40,50]
```

## 참고 자료

- Database Internals, Chapter 2 (B-Tree 전체)
- CMU 15-445 Lecture 7: Tree Indexes
- 시각화: https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html

## 다음 단계

Phase 4 완료 후 → [Phase 5: Query Executor](./phase-5-query-executor.md)
