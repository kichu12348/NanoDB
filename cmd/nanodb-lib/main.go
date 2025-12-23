package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"encoding/binary"
	"encoding/json"
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

	catalog  *collection.Collection
	globalMu sync.RWMutex
)

//export NanoInit
func NanoInit(path *C.char) {
	goPath := C.GoString(path)

	p, err := storage.OpenPager(goPath)
	if err != nil {
		panic(err)
	}
	pager = p

	h, err := storage.ReadHeader(pager)
	if err != nil {
		h = &storage.DBHeader{
			Magic:     [4]byte{'A', 'A', 'M', 'N'},
			Version:   1,
			PageSize:  storage.PageSize,
			PageCount: 1,
		}
		storage.WriteHeader(pager, h)

		catalogPage, _ := storage.AllocatePage(pager, h)
		rawCatalog := make([]byte, storage.PageSize)
		storage.InitDataPage(rawCatalog)
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
	globalMu.Lock()
	defer globalMu.Unlock()
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

//export NanoCreateCollection
func NanoCreateCollection(colName *C.char) C.longlong {
	globalMu.Lock()
	defer globalMu.Unlock()

	cName := C.GoString(colName)

	_, ok := openCollections[cName]
	if ok {
		return 0
	}

	newColPageNum, err := storage.AllocatePage(pager, header)
	if err != nil {
		return -1
	}

	empty := make([]byte, header.PageSize)
	storage.InitDataPage(empty)
	if err := pager.WritePage(newColPageNum, empty); err != nil {
		return -1
	}

	var currentPageNum uint32 = 1
	for {
		entry := record.EncodeCollectionEntry(cName, newColPageNum)
		page, _ := pager.ReadPage(uint32(currentPageNum))

		success, _ := record.InsertRecord(page, 0, entry)

		// if success record inserted
		if success {
			if err := pager.WritePage(currentPageNum, page); err != nil {
				return -1
			}
			newCol, _ := collection.NewCollection(cName, newColPageNum, pager, header)
			openCollections[cName] = newCol
			return 1
		}

		//move to next page
		nextPage := binary.LittleEndian.Uint32(page[4:8])

		if nextPage != 0 {
			currentPageNum = nextPage
			continue
		}

		// if no page then allocate new page for
		newPageId, err := storage.AllocatePage(pager, header)
		if err != nil {
			return -1
		}

		emptyCatPage := make([]byte, header.PageSize)
		storage.InitDataPage(emptyCatPage)

		if err := pager.WritePage(newPageId, emptyCatPage); err != nil {
			return -1
		}

		binary.LittleEndian.PutUint32(page[4:8], newPageId)

		if err := pager.WritePage(currentPageNum, page); err != nil {
			return -1
		}
		currentPageNum = newPageId
	}
}

//export NanoInsert
func NanoInsert(colName *C.char, jsonStr *C.char) C.longlong {

	globalMu.RLock()
	cName := C.GoString(colName)

	col, ok := openCollections[cName]
	globalMu.RUnlock()

	if !ok {
		return -1
	}
	data := C.GoString(jsonStr)
	var doc map[string]any
	if err := json.Unmarshal([]byte(data), &doc); err != nil {
		return -1
	}

	docId, err := col.Insert(doc)

	if err != nil {
		return -1
	}

	return C.longlong(docId)
}

//export NanoInsertMany
func NanoInsertMany(colName *C.char, jsonStr *C.char) *C.char {
	globalMu.RLock()
	cName := C.GoString(colName)

	col, ok := openCollections[cName]
	globalMu.RUnlock()

	if !ok {
		return nil
	}
	data := C.GoString(jsonStr)
	var docs []map[string]any
	if err := json.Unmarshal([]byte(data), &docs); err != nil {
		return nil
	}

	var docIds []uint64

	for _, doc := range docs {
		docId, err := col.Insert(doc)

		if err != nil {
			break
		}
		docIds = append(docIds, docId)
	}

	bytes, _ := json.Marshal(docIds)
	return C.CString(string(bytes))
}

//export NanoFind
func NanoFind(colName *C.char, queryJson *C.char) *C.char {
	globalMu.RLock()
	cName := C.GoString(colName)
	col, ok := openCollections[cName]
	globalMu.RUnlock()

	if !ok {
		return nil
	}
	qStr := C.GoString(queryJson)

	var query map[string]any
	if err := json.Unmarshal([]byte(qStr), &query); err != nil {
		return nil
	}

	docs, err := col.Find(query)
	if err != nil {
		return nil
	}
	bytes, _ := json.Marshal(docs)
	return C.CString(string(bytes))
}

//export NanoFindOne
func NanoFindOne(colName *C.char, queryJson *C.char) *C.char {
	globalMu.RLock()
	cName := C.GoString(colName)
	col, ok := openCollections[cName]
	globalMu.RUnlock()

	if !ok {
		return nil
	}
	qStr := C.GoString(queryJson)

	var query map[string]any
	if err := json.Unmarshal([]byte(qStr), &query); err != nil {
		return nil
	}

	doc, err := col.Find(query)
	if err != nil {
		return nil
	}
	bytes, _ := json.Marshal(doc)
	return C.CString(string(bytes))
}

// NanoFindById
func NanoFindById(colName *C.char, docId C.longlong) *C.char {
	globalMu.RLock()
	cName := C.GoString(colName)
	col, ok := openCollections[cName]
	globalMu.RUnlock()

	if !ok {
		return nil
	}
	doc, err := col.FindById(uint64(docId))

	if err != nil {
		return nil
	}

	bytes, _ := json.Marshal(doc)
	return C.CString(string(bytes))
}

//export NanoUpdateById
func NanoUpdateById(colName *C.char, docId C.longlong, jsonStr *C.char) *C.char {
	globalMu.Lock()
	defer globalMu.Unlock()

	//cName := C.GoString(colName)
	return C.CString("nUH")
}

//export NanoFree
func NanoFree(ptr *C.char) {
	C.free(unsafe.Pointer(ptr))
}

//export NanoClose
func NanoClose() C.longlong {
	globalMu.Lock()
	defer globalMu.Unlock()

	if pager == nil {
		return 1
	}

	err := pager.Close()
	if err != nil {
		return -1
	}

	pager = nil
	catalog = nil
	openCollections = make(map[string]*collection.Collection)

	return 1
}

// Main is required for buildmode=c-shared, but it is ignored
func main() {}
