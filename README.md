# NanoDB

NanoDB is a high-performance, embedded, document-oriented database written in Go. It is designed for low-latency applications, offering O(1) read speeds and efficient append-only writes. It supports schemaless data storage using MessagePack serialization and provides fine-grained concurrency control.

## 🚀 Performance Benchmarks

### Throughput
*   **Write Speed (Batch Insert):** 77,544 docs/sec
    *(Source: Rust Benchmark, 100k records)*
*   **Read Speed (Point Lookup by ID):** 357,143 ops/sec
    *(Source: Node.js Benchmark, In-Memory Hash Index)*
*   **Scan Speed (Full Collection):** 256,410 ops/sec
    *(Source: Node.js Benchmark, Sequential Read)*

### ⚡ Latency & Efficiency
*   **Index Access:** O(1) (Constant Time)
*   **Write Complexity:** O(1) (Tail-Append Optimization)
*   **Startup Time:** < 10ms (Lazy Loading)

### 🛡️ Concurrency
*   **Locking Model:** Fine-Grained Collection Locks + Reference Counting
*   **Stress Test Result:** 0% Data Loss (4 Concurrent Workers, 2000 Ops)

## ✨ Features

*   **Embedded:** Runs as a library within your application; no separate server process required.
*   **Document-Oriented:** Stores data as schemaless documents (maps) using efficient MessagePack serialization.
*   **High Performance:**
    *   **In-Memory Indexing:** Uses a hash map for instant O(1) lookups by document ID.
    *   **Page-Based Storage:** Custom paging system optimized for modern SSDs.
*   **Concurrency Safe:** Thread-safe collections with `sync.RWMutex` allowing concurrent reads and safe writes.
*   **Portable:** Written in pure Go. Can be compiled to a C-shared library (`.so`/`.dll`) for use with Node.js, Rust, Python, etc.

## 📦 Installation

```bash
git clone https://github.com/kichu12348/NanoDB.git
cd nanodb
go mod download
```

## 🛠️ Usage

NanoDB is currently designed as a low-level embedded database engine.

### Running the Demo
You can run the internal demo/test entry point:

```bash
go run cmd/nanodb/main.go
```

### Building the Shared Library (FFI)
To use NanoDB from other languages (like Node.js or Rust), build it as a shared library:

```bash
go build -buildmode=c-shared -o nanodb.so ./cmd/nanodb-lib
```

### Go Example (Internal API)

*Note: The current API is low-level and resides in the `internal` package. A public API wrapper is planned.*

```go
package main

import (
    "fmt"
    "nanodb/internal/collection"
    "nanodb/internal/storage"
)

func main() {
    // 1. Open the Pager
    pager, _ := storage.OpenPager("data.db")
    defer pager.Close()

    // 2. Initialize Header (Simplified)
    header, _ := storage.ReadHeader(pager)

    // 3. Open a Collection
    // Assuming root page for "users" is known (e.g., page 2)
    users, _ := collection.NewCollection("users", 2, pager, header)

    // 4. Insert a Document
    doc := map[string]any{
        "id":   101,
        "name": "Alice",
        "role": "admin",
    }
    users.Insert(doc)

    // 5. Find a Document
    result, _ := users.FindOne(101)
    fmt.Println("Found:", result)
}
```

## 🏗️ Architecture

*   **Storage Engine:** Uses a paged storage model (4KB pages) with a free-list for space reclamation.
*   **Data Format:** Records are serialized using **MessagePack** for compactness and speed.
*   **Indexing:** Primary keys are indexed in-memory for O(1) access. The index maps IDs to `{PageID, SlotID}`.
*   **Durability:** Direct file I/O with `os.File`.

## 📄 License

MIT
