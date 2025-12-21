package main

import (
	"fmt"

	"nanodb/internal/collection"
	"nanodb/internal/record"
	"nanodb/internal/storage"
)

func main() {
	// Clean restart
	//os.Remove("test.db")

	// 1. Low-Level Init
	p, err := storage.OpenPager("test.db")
	if err != nil {
		panic(err)
	}
	defer p.Close()

	// 2. Initialize Headers
	// We need to create a default header since it's a new file
	h := &storage.DBHeader{
		Magic:     [4]byte{'A', 'A', 'M', 'N'},
		Version:   1,
		PageSize:  storage.PageSize,
		PageCount: 1, // Page 0 is header
	}
	storage.WriteHeader(p, h)

	// create catalog page which will hold all collections
	catalogPage, err := storage.AllocatePage(p, h)
	if err != nil {
		panic(err)
	}

	//initialize catalog page
	rawCatalogPage := make([]byte, storage.PageSize)
	storage.InitDataPage(rawCatalogPage)
	p.WritePage(catalogPage, rawCatalogPage)

	// 3. Create Root Page for our collection page 2
	userRootPage, err := storage.AllocatePage(p, h)
	if err != nil {
		panic(err)
	}

	metaEntry := record.EncodeCollectionEntry("users", userRootPage)

	//write collection entry to catalog page
	catalogData, err := p.ReadPage(catalogPage)
	if err != nil {
		panic(err)
	}
	record.InsertRecord(catalogData, 0, metaEntry)
	p.WritePage(catalogPage, catalogData)

	// Initialize root page
	rawPage := make([]byte, storage.PageSize)
	storage.InitDataPage(rawPage)
	p.WritePage(userRootPage, rawPage)

	// 4. Create High-Level Collection
	users, err := collection.NewCollection("users", userRootPage, p, h)
	if err != nil {
		panic(err)
	}

	// 5. STRESS TEST: Insert 100 users
	// A page is 4096 bytes. Each user is ~50 bytes.
	// 100 users = ~5000 bytes. This GUARANTEES we need at least 2 pages.
	//fmt.Println("Starting insert loop...")

	//for i := range 10 {
	// user := map[string]any{
	// 	"id":    i,
	// 	"name":  fmt.Sprintf("User %d", i),
	// 	"email": fmt.Sprintf("rmaha%d@example.com", i),
	// }

	// _, err := users.Insert(user)
	// if err != nil {
	// 	panic(err)
	// }
	//fmt.Printf("Inserted user %d with docId %d\n", i, docId)

	// doc, err := users.FindById(docId)
	// if err != nil {
	// 	panic(err)
	// }
	//fmt.Printf("Verified inserted document: %+v\n", doc)
	//}

	//find doc by id
	doc, err := users.FindById(16264638516676103845)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Found document with id 42: %+v\n", doc)

	// Check file size to prove it grew

	//fmt.Printf("Database size: %d bytes (Should be > 8192)\n", stat.Size())
}
