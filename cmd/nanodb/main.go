package main

// import (
// 	"os"

// 	"nanodb/internal/record"
// 	"nanodb/internal/storage"
// )

// func main() {
// 	dbFile := "test.db"

// 	// 1. Check if DB exists
// 	_, err := os.Stat(dbFile)
// 	dbExists := !os.IsNotExist(err)

// 	p, err := storage.OpenPager(dbFile)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer p.Close()

// 	var h *storage.DBHeader
// 	var userRootPage uint32

// 	if !dbExists {
// 		// --- BOOTSTRAP MODE (Run once) ---
// 		//fmt.Println("Initializing new database...")

// 		// A. Write Header
// 		h = &storage.DBHeader{
// 			Magic:     [4]byte{'A', 'A', 'M', 'N'},
// 			Version:   1,
// 			PageSize:  storage.PageSize,
// 			PageCount: 1,
// 		}
// 		storage.WriteHeader(p, h)

// 		// B. Allocate Catalog (Page 1)
// 		catalogPage, _ := storage.AllocatePage(p, h)
// 		rawCatalog := make([]byte, storage.PageSize)
// 		storage.InitDataPage(rawCatalog) // Wipes page to be safe
// 		p.WritePage(catalogPage, rawCatalog)

// 		// C. Allocate Users Collection (Page 2)
// 		userRootPage, _ = storage.AllocatePage(p, h)
// 		rawUserPage := make([]byte, storage.PageSize)
// 		storage.InitDataPage(rawUserPage) // Wipes page to be safe
// 		p.WritePage(userRootPage, rawUserPage)

// 		// D. Register "users" in Catalog
// 		metaEntry := record.EncodeCollectionEntry("users", userRootPage)
// 		record.InsertRecord(rawCatalog, 0, metaEntry)
// 		p.WritePage(catalogPage, rawCatalog)

// 	} else {
// 		// --- LOAD MODE (Run on restart) ---
// 		//fmt.Println("Loading existing database...")

// 		// A. Read Header
// 		h, err = storage.ReadHeader(p)
// 		if err != nil {
// 			panic(err)
// 		}

// 		// B. Read Catalog to find "users" root page
// 		// (For now, we know it's Page 2 because we built it that way,
// 		// but in the future you should search Page 1 for name="users")
// 		userRootPage = 2
// 	}

// 	// 4. Create Collection Wrapper
// 	// Now this will read the EXISTING pages instead of wiping them
// 	//users, err := collection.NewCollection("users", userRootPage, p, h)
// 	// if err != nil {
// 	// 	panic(err)
// 	// }

// 	// 5. Query Test
// 	// Try to find the document you inserted last time
// 	//fmt.Println("Attempting to find existing record...")

// 	// Replace with a known ID from your previous run
// 	// doc, err := users.FindById(2095559075886514214)
// 	// if err != nil {
// 	// 	panic(err)
// 	// }

// 	// if doc == nil {
// 	// 	fmt.Println("Document not found (Did you insert it?)")
// 	// } else {
// 	// 	fmt.Printf("Found document: %+v\n", doc)
// 	// }

// 	// Optional: Insert new data to prove we append, not overwrite
// 	// for i := range 3 {
// 	// 	newDoc := map[string]any{
// 	// 		"name": fmt.Sprintf("User%d", i),
// 	// 		"age":  20 + i,
// 	// 	}
// 	// 	newId, err := users.Insert(newDoc)
// 	// 	if err != nil {
// 	// 		panic(err)
// 	// 	}
// 	// 	fmt.Printf("Inserted new document with _id=%d\n", newId)
// 	// }

// 	// 2. Read it back
// 	// oldDoc, _ := users.FindById(2095559075886514214)
// 	// fmt.Printf("Before Update: %+v\n", oldDoc)

// 	// // 3. Update it
// 	// fmt.Println("Updating user 500...")
// 	// users.Update(2095559075886514214, map[string]any{
// 	// 	"name": "New Name",
// 	// 	"role": "CEO",
// 	// })

// 	// // 4. Read it back AGAIN
// 	// newDoc, _ := users.FindById(2095559075886514214)
// 	// fmt.Printf("After Update: %+v\n", newDoc)

// 	//open catalog collection
// 	// cat, err := collection.NewCollection("_catalog", 1, p, h)
// 	// if err != nil {
// 	// 	panic(err)
// 	// }

// 	//record, _ := record.GetAllCollections(p)

// 	//fmt.Println("--- STARTING CONCURRENCY TEST ---")

// 	//var wg sync.WaitGroup // Waits for threads to finish

// 	// Launch 100 concurrent writers
// 	// for i := 0; i < 100; i++ {
// 	// 	wg.Add(1)
// 	// 	go func(id int) {
// 	// 		defer wg.Done()

// 	// 		// Each thread inserts a user
// 	// 		doc := map[string]any{
// 	// 			"name": fmt.Sprintf("Concurrent_User_%d", id),
// 	// 		}
// 	// 		users.Insert(doc)
// 	// 		// fmt.Printf("Thread %d finished\n", id)
// 	// 	}(i)
// 	// }

// 	// fmt.Println("Waiting for threads...")
// 	// wg.Wait() // Blocks until all 100 threads are done
// 	// fmt.Println("All threads finished safely!")
// }
