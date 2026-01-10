# Phase 0: Setup

Zig 개발 환경 구성과 프로젝트 초기 설정.

## 목표

- [ ] Zig 설치
- [ ] 프로젝트 구조 생성
- [ ] 빌드 시스템 이해
- [ ] 첫 테스트 실행

---

## Zig 설치

### macOS
```bash
brew install zig
```

### 버전 확인
```bash
zig version
# 0.11.0 이상 권장
```

---

## 프로젝트 구조

```
minidb/
├── build.zig           # 빌드 설정
├── src/
│   ├── main.zig        # 실행 진입점
│   └── lib.zig         # 라이브러리 루트
└── docs/
```

---

## 해야 할 것

### 1. build.zig 작성

Zig 공식 문서의 Build System 참고해서 작성:
- 라이브러리 타겟 (`addStaticLibrary`)
- 실행 파일 타겟 (`addExecutable`)
- 테스트 타겟 (`addTest`)

### 2. src/main.zig

간단한 REPL 껍데기:
- "MiniDB v0.1.0" 출력
- 사용자 입력 받기 (나중에 SQL 처리)

### 3. 첫 테스트

`src/lib.zig`에 간단한 테스트 작성해서 `zig build test` 동작 확인.

---

## 명령어

```bash
zig build          # 빌드
zig build run      # 실행
zig build test     # 테스트
```

---

## Zig 기초 문법 참고

### 변수
```zig
const x: i32 = 42;    // 상수
var y: i32 = 10;      // 변수
```

### 에러 처리
```zig
fn mayFail() !void {
    return error.Something;
}

// try로 전파
try mayFail();

// catch로 처리
mayFail() catch |err| { ... };
```

### 테스트
```zig
test "example" {
    try std.testing.expect(true);
    try std.testing.expectEqual(@as(i32, 42), 42);
}
```

### Allocator
```zig
const allocator = std.heap.page_allocator;
const slice = try allocator.alloc(u8, 100);
defer allocator.free(slice);
```

---

## 체크리스트

- [ ] `zig version` 동작 확인
- [ ] build.zig 작성
- [ ] `zig build` 성공
- [ ] `zig build test` 성공
- [ ] `zig build run` 동작

---

## 참고 자료

- https://ziglang.org/documentation
- https://ziglearn.org

## 다음 단계

Phase 0 완료 후 → [Phase 1: Storage](./phase-1-storage.md)
