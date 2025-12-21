package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"sync"
	"unsafe"

	"nanodb/internal/collection"
	"nanodb/internal/record"
	"nanodb/internal/storage"
)

// Global state to keep the DB alive in memory between calls
var (
	openCollections = make(map[string]*collection.Collection)
	pager           *storage.Pager
	header          *storage.DBHeader

	catalog *collection.Collection
	mu      sync.Mutex
)

//export NanoInit
func NanoInit(path *C.char) {
	goPath := C.GoString(path)

	p, err := storage.OpenPager(goPath)
	if err != nil {
		panic(err)
	}
	pager = p

	// Read header
	h, err := storage.ReadHeader(pager)
	if err != nil {
		h = &storage.DBHeader{
			Magic:     [4]byte{'A', 'A', 'M', 'N'},
			Version:   1,
			PageSize:  storage.PageSize,
			PageCount: 1,
		}
		storage.WriteHeader(pager, h)

		// Allocate Catalog (Page 1)
		catalogPage, _ := storage.AllocatePage(pager, h)
		rawCatalog := make([]byte, storage.PageSize)
		storage.InitDataPage(rawCatalog) // Wipes page to be safe
		pager.WritePage(catalogPage, rawCatalog)

	}

	header = h

	// Load Catalog
	cat, err := collection.NewCollection("_catalog", 1, pager, header)
	if err != nil {
		panic(err)
	}
	catalog = cat

	loadExistingCollections()
}

func loadExistingCollections() {
	mu.Lock()
	defer mu.Unlock()
	collections, err := record.GetAllCollections(pager)

	if err != nil {
		panic(err)
	}

	for _, col := range collections {
		loadedCol, err := collection.NewCollection(col.Name, col.RootPage, pager, header)
		if err != nil {
			continue
		}
		openCollections[col.Name] = loadedCol
	}
}

//export NanoInsert
// func NanoInsert(jsonStr *C.char) C.longlong {

// }

// //export NanoFind
// func NanoFind(id C.longlong) *C.char {

// }

//export NanoFree
func NanoFree(ptr *C.char) {
	C.free(unsafe.Pointer(ptr))
}

// Main is required for buildmode=c-shared, but it is ignored
func main() {}
