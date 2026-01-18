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

	"nanodb/internal/btree"
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

	activeUsers uint
)

//export NanoInit
func NanoInit(path *C.char) {
	globalMu.Lock()
	defer globalMu.Unlock()

	activeUsers++

	if pager != nil {
		return
	}

	goPath := C.GoString(path)

	p, err := storage.OpenPager(goPath)
	if err != nil {
		panic(err)
	}
	pager = p

	h, err := pager.ReadHeader()
	if err != nil {
		h = &storage.DBHeader{
			Magic:     [4]byte{'A', 'A', 'M', 'N'},
			Version:   1,
			PageSize:  storage.PageSize,
			PageCount: 1,
		}
		if err := pager.WriteHeader(h); err != nil {
			panic(err)
		}

		catalogPage, err := pager.AllocatePage(h)
		if err != nil {
			panic(err)
		}
		rawCatalog := storage.GetBuff()
		defer storage.ReleasePageBuffer(rawCatalog)

		storage.InitDataPage(rawCatalog)
		if err := pager.WritePage(catalogPage, rawCatalog); err != nil {
			panic(err)
		}
	}

	header = h

	// Load Catalog
	cat, err := collection.NewCollection("_catalog", 1, 0, pager, header) //collection insert bypasses the btree so this is fine
	if err != nil {
		panic(err)
	}
	catalog = cat

	loadExistingCollectionsInternal()
}

func loadExistingCollectionsInternal() {

	collections, err := record.GetAllCollections(pager)

	if err != nil {
		panic(err)
	}

	for _, col := range collections {
		loadedCol, err := collection.NewCollection(col.Name, col.RootPage, col.IndexRoot, pager, header)
		if err != nil {
			continue
		}
		loadedCol.LoadVectorIndex()
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

	newColPageNum, err := pager.AllocatePage(header)
	if err != nil {
		return -1
	}

	empty := storage.GetBuff()

	storage.InitDataPage(empty)
	if err := pager.WritePage(newColPageNum, empty); err != nil {
		storage.ReleasePageBuffer(empty)
		return -1
	}

	storage.ReleasePageBuffer(empty)

	newIndexRootPage, err := pager.AllocatePage(header)
	if err != nil {
		return -1
	}

	newIndexData := storage.GetBuff()

	node := btree.NewNode(newIndexData)

	node.SetHeader(btree.NodeTypeLeaf, true)
	node.SetNumCells(0)

	if err := pager.WritePage(newIndexRootPage, newIndexData); err != nil {
		storage.ReleasePageBuffer(newIndexData)
		return -1
	}

	storage.ReleasePageBuffer(newIndexData)

	var currentPageNum uint32 = 1
	for {
		entry := record.EncodeCollectionEntry(cName, newColPageNum, newIndexRootPage)
		page, err := pager.ReadPage(currentPageNum)
		if err != nil {
			return -1
		}

		success, err := record.InsertRecord(page, 0, entry)
		if err != nil {
			storage.ReleasePageBuffer(page)
			return -1
		}

		// if success record inserted
		if success {
			if err := pager.WritePage(currentPageNum, page); err != nil {
				storage.ReleasePageBuffer(page)
				return -1
			}
			newCol, _ := collection.NewCollection(cName, newColPageNum, newIndexRootPage, pager, header)
			openCollections[cName] = newCol
			storage.ReleasePageBuffer(page)
			return 1
		}

		//move to next page
		nextPage := binary.LittleEndian.Uint32(page[4:8])

		if nextPage != 0 {
			currentPageNum = nextPage
			storage.ReleasePageBuffer(page)
			continue
		}

		// if no page then allocate new page for
		newPageId, err := pager.AllocatePage(header)
		if err != nil {
			storage.ReleasePageBuffer(page)
			return -1
		}

		emptyCatPage := storage.GetBuff()
		storage.InitDataPage(emptyCatPage)

		if err := pager.WritePage(newPageId, emptyCatPage); err != nil {
			storage.ReleasePageBuffer(page)
			storage.ReleasePageBuffer(emptyCatPage)
			return -1
		}

		binary.LittleEndian.PutUint32(page[4:8], newPageId)

		if err := pager.WritePage(currentPageNum, page); err != nil {
			storage.ReleasePageBuffer(page)
			storage.ReleasePageBuffer(emptyCatPage)
			return -1
		}
		currentPageNum = newPageId
		storage.ReleasePageBuffer(page)
		storage.ReleasePageBuffer(emptyCatPage)
	}
}

//export NanoGetCollections
func NanoGetCollections() *C.char {
	globalMu.RLock()
	defer globalMu.RUnlock()

	var cols []string
	for key := range openCollections {
		cols = append(cols, key)
	}
	bytes, _ := json.Marshal(cols)

	return C.CString(string(bytes))
}

//export NanoInsert
func NanoInsert(colName *C.char, jsonStr *C.char) C.longlong {

	cName := C.GoString(colName)

	globalMu.RLock()
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
	cName := C.GoString(colName)

	globalMu.RLock()
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

	docsIds, err := col.InsertMany(docs)

	if err != nil {
		return nil
	}

	ids := *docsIds

	if ids == nil {
		ids = []uint64{}
	}

	bytes, _ := json.Marshal(ids)
	return C.CString(string(bytes))
}

//export NanoFind
func NanoFind(colName *C.char, queryJson *C.char, limit C.longlong, skip C.longlong) *C.char {

	cName := C.GoString(colName)

	globalMu.RLock()
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

	var optLimit uint
	var skipCount uint
	if limit > 0 {
		optLimit = uint(limit)
	}
	if skip > 0 {
		skipCount = uint(skip)
	}

	docs, _, err := col.Find(query, &collection.FindOptions{Limit: optLimit, Skip: skipCount})
	if err != nil {
		return nil
	}
	bytes, _ := json.Marshal(docs)
	return C.CString(string(bytes))
}

//export NanoFindOne
func NanoFindOne(colName *C.char, queryJson *C.char) *C.char {

	cName := C.GoString(colName)

	globalMu.RLock()
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

	doc, err := col.FindOne(query)
	if err != nil {
		return nil
	}
	bytes, _ := json.Marshal(doc)
	return C.CString(string(bytes))
}

//export NanoVectorSearch
func NanoVectorSearch(colName *C.char, queryJson *C.char, topK C.longlong) *C.char {
	cName := C.GoString(colName)

	globalMu.RLock()
	col, ok := openCollections[cName]
	globalMu.RUnlock()

	if !ok {
		return nil
	}

	qStr := C.GoString(queryJson)
	var query []float32

	if err := json.Unmarshal([]byte(qStr), &query); err != nil {
		return nil
	}

	ids, err := col.SearchVector(query, int(topK))

	if err != nil {
		return nil
	}

	bytes, _ := json.Marshal(ids)

	return C.CString(string(bytes))
}

//export NanoFindById
func NanoFindById(colName *C.char, docId C.longlong) *C.char {
	cName := C.GoString(colName)

	globalMu.RLock()
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
	cName := C.GoString(colName)

	globalMu.RLock()
	col, ok := openCollections[cName]
	globalMu.RUnlock()

	if !ok {
		return nil
	}

	jstr := C.GoString(jsonStr)

	var jsonData map[string]any
	err := json.Unmarshal([]byte(jstr), &jsonData)

	if err != nil {
		return nil
	}
	doc, err := col.FindById(uint64(docId))

	if err != nil {
		return nil
	}
	if doc == nil {
		return nil
	}

	for key, val := range jsonData {
		if key == "_id" {
			continue
		}
		doc[key] = val
	}

	errOnUpdate := col.UpdateById(uint64(docId), doc)

	if errOnUpdate != nil {
		panic(errOnUpdate)
		//return nil
	}

	bytes, _ := json.Marshal(doc)

	return C.CString(string(bytes))
}

//export NanoUpdateMany
func NanoUpdateMany(colName *C.char, queryJson *C.char, jsonStr *C.char) *C.char {

	cName := C.GoString(colName)
	globalMu.RLock()
	col, ok := openCollections[cName]
	globalMu.RUnlock()

	if !ok {
		return nil
	}
	qStr := C.GoString(queryJson)

	jstr := C.GoString(jsonStr)

	var jsonData map[string]any
	err := json.Unmarshal([]byte(jstr), &jsonData)

	var query map[string]any
	if err := json.Unmarshal([]byte(qStr), &query); err != nil {
		return nil
	}

	docs, docIds, err := col.Find(query, nil)
	if err != nil {
		return nil
	}

	for idx, doc := range docs {
		for key, val := range jsonData {
			if key == "_id" {
				continue
			}
			doc[key] = val
		}
		errOnUpdate := col.UpdateById(uint64(docIds[idx]), doc)

		if errOnUpdate != nil {
			break
		}
	}

	bytes, _ := json.Marshal(docs)
	return C.CString(string(bytes))
}

//export NanoDeleteById
func NanoDeleteById(colName *C.char, docId C.longlong) C.longlong {

	cName := C.GoString(colName)
	globalMu.RLock()
	col, ok := openCollections[cName]
	globalMu.RUnlock()

	if !ok {
		return -1
	}

	err := col.DeleteById(uint64(docId))

	if err != nil {
		return -1
	}

	return 1
}

//export NanoDeleteMany
func NanoDeleteMany(colName *C.char, query *C.char) C.longlong {

	cName := C.GoString(colName)

	globalMu.RLock()
	col, ok := openCollections[cName]
	globalMu.RUnlock()

	if !ok {
		return -1
	}

	jstr := C.GoString(query)

	var jsonData map[string]any

	if err := json.Unmarshal([]byte(jstr), &jsonData); err != nil {
		return -1
	}

	success, err := col.FindAndDelete(jsonData)

	if err != nil || !success {
		return -1
	}

	return 1
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

	if activeUsers > 0 {
		activeUsers--
	}

	if activeUsers == 0 {
		err := pager.Close()
		if err != nil {
			return -1
		}

		pager = nil
		catalog = nil
		openCollections = make(map[string]*collection.Collection)
	}
	return 1

}

// Main is required for buildmode=c-shared, but it is ignored
func main() {}
