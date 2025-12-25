# NanoDB

NanoDB is an experimental embedded document-oriented database written in Go,
built to explore how modern storage engines work at a low level.

The project focuses on page-based storage, slot directories, in-memory indexing,
and byte-level record layout rather than production readiness. It is intended as
a learning-driven systems project inspired by embedded databases like SQLite.

---

## 🧪 Informal Benchmarks (Development Tests)

> ⚠️ These benchmarks were collected during development using different test
> harnesses (Go / Node.js / Rust) and are intended for rough comparison only.
> NanoDB does **not** yet implement WAL, crash recovery, or durability guarantees.

### Throughput
- **Batch Insert:** ~77k docs/sec  
  *(Rust harness, ~100k records, append-heavy workload)*

- **Point Lookup by ID:** ~357k ops/sec  
  *(Node.js FFI test, in-memory hash index)*

- **Full Collection Scan:** ~256k ops/sec  
  *(Sequential page scan)*

### Latency & Complexity
- **Index Lookup:** O(1) average-case (in-memory hash map)
- **Insert Path:** Amortized constant-time under append-friendly workloads
- **Startup Time:** < 10ms (index rebuilt at startup)

---

## 🔒 Concurrency Model

- **Locking Strategy:** Per-collection `sync.RWMutex`
- **Concurrent Reads:** Allowed
- **Writes:** Serialized per collection
- **Stress Testing:** No inconsistencies observed during multi-worker tests

---

## ✨ Features

- **Embedded:** Runs as a library inside your application (no server process).
- **Document-Oriented:** Stores schemaless documents using MessagePack encoding.
- **Page-Based Storage Engine:**
  - Fixed-size 4KB pages
  - Slot directory layout
  - Page chaining for collection growth
- **In-Memory Primary Index:**
  - Maps `_id → {PageID, SlotID}`
  - Rebuilt on startup
- **Deletion Model:** Tombstone-based deletes (space reclaimed via future compaction).
- **Concurrency Safe:** Thread-safe collections with fine-grained locking.
- **Portable:** Written in pure Go and can be compiled as a C shared library
  for use with Node.js, Rust, Python, and other languages via FFI.

---

## 📦 Installation

```bash
git clone https://github.com/kichu12348/NanoDB.git
cd NanoDB
go mod download
