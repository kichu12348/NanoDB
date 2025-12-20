package main

import (
	"fmt"
	//"os"

	"nanodb/internal/collection"
	"nanodb/internal/storage"
)

func main() {
	// Clean restart
	// os.Remove("test.db")

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

	// 3. Create Root Page for our collection
	rootPage, err := storage.AllocatePage(p, h)
	if err != nil {
		panic(err)
	}

	// Initialize root page
	rawPage := make([]byte, storage.PageSize)
	storage.InitDataPage(rawPage)
	p.WritePage(rootPage, rawPage)

	// 4. Create High-Level Collection
	users, err := collection.NewCollection("users", rootPage, p, h)
	if err != nil {
		panic(err)
	}

	// 5. STRESS TEST: Insert 100 users
	// A page is 4096 bytes. Each user is ~50 bytes.
	// 100 users = ~5000 bytes. This GUARANTEES we need at least 2 pages.
	fmt.Println("Starting insert loop...")

	// for i := range 100 {
	// 	user := map[string]any{
	// 		"id":   i,
	// 		"name": fmt.Sprintf("User_%d", i),
	// 		"bio":  "This is some extra data to fill up space faster...",
	// 	}

	// 	err := users.Insert(user)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	//find doc by id
	doc, err := users.FindById(42)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Found document with id 42: %+v\n", doc)

	// Check file size to prove it grew

	//fmt.Printf("Database size: %d bytes (Should be > 8192)\n", stat.Size())
}
