package collection

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"nanodb/internal/btree"
	"nanodb/internal/record"
	"nanodb/internal/storage"
	"sync"
)

type Bucket struct {
	Centroid []float32
	RootPage uint32
}

type Collection struct {
	Name           string
	RootPage       uint32
	LastPage       uint32
	BucketMetaPage uint32
	Buckets        []Bucket
	Pager          *storage.Pager
	Header         *storage.DBHeader
	BTree          *btree.Btree
	mu             sync.RWMutex
}

type FindOptions struct {
	Limit uint
	Skip  uint
}

func NewCollection(name string, root uint32, indexRoot uint32, pager *storage.Pager, header *storage.DBHeader) (*Collection, error) {

	b := &btree.Btree{
		Pager:    pager,
		Header:   header,
		RootPage: indexRoot,
	}

	lastPage := root
	curr := lastPage

	for curr != 0 {
		page, err := pager.ReadPage(curr)

		if err != nil {
			return nil, err
		}

		nextPage := binary.LittleEndian.Uint32(page[4:8])
		if nextPage == 0 {
			lastPage = curr
			storage.ReleasePageBuffer(page)
			break
		}
		curr = nextPage
		storage.ReleasePageBuffer(page)
	}

	return &Collection{
		Name:     name,
		RootPage: root,
		Pager:    pager,
		Header:   header,
		BTree:    b,
		LastPage: lastPage,
	}, nil
}

func GenerateRandomId(n int) uint64 {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	var id uint64
	for i := range b {
		id = (id << 8) | uint64(b[i])
	}
	return id
}

func (c *Collection) Insert(doc map[string]any) (uint64, error) {
	c.mu.Lock()         // lock for writing
	defer c.mu.Unlock() // unlock after function ends
	docId := GenerateRandomId(6)

	doc["_id"] = docId

	data, err := record.EncodeDoc(doc)

	var embedding []float32
	if val, ok := doc["_embeddings"]; ok {
		if vecInterface, ok := val.([]any); ok {
			embedding = make([]float32, len(vecInterface))
			for i, v := range vecInterface {
				embedding[i] = float32(convertToFloat(v))
			}
		}

		delete(doc, "_embeddings")
	}

	if err != nil {
		return 0, err
	}

	currentPageId := c.LastPage

	for {
		//read the current page
		pageData, err := c.Pager.ReadPage(currentPageId)
		if err != nil {
			return 0, err
		}

		//try to insert the record
		success, err := record.InsertRecord(pageData, docId, data)
		if err != nil {
			storage.ReleasePageBuffer(pageData)
			return 0, err
		}

		if success {
			//update index
			slotCount := binary.LittleEndian.Uint16(pageData[0:2])

			err := c.BTree.Insert(docId, currentPageId, slotCount-1)
			if err != nil {
				storage.ReleasePageBuffer(pageData)
				return 0, err
			}

			// write back the page if insertion successful
			err = c.Pager.WritePage(currentPageId, pageData)
			storage.ReleasePageBuffer(pageData)
			return docId, err
		}

		//move to next page if insertion failed
		nextPage := binary.LittleEndian.Uint32(pageData[4:8])
		if nextPage != 0 {
			// if there is a next page in chain, move to it
			currentPageId = nextPage
			storage.ReleasePageBuffer(pageData)
			continue
		}

		// allocate a new page if no next page

		newPageId, err := c.Pager.AllocatePage(c.Header)

		if err != nil {
			storage.ReleasePageBuffer(pageData)
			return 0, err
		}

		newPageData := storage.GetBuff()
		storage.InitDataPage(newPageData)

		if err := c.Pager.WritePage(newPageId, newPageData); err != nil {
			storage.ReleasePageBuffer(newPageData)
			storage.ReleasePageBuffer(pageData)
			return 0, err
		}

		//link old page to new page
		binary.LittleEndian.PutUint32(pageData[4:8], newPageId)
		if err := c.Pager.WritePage(currentPageId, pageData); err != nil {
			storage.ReleasePageBuffer(pageData)
			storage.ReleasePageBuffer(newPageData)
			return 0, err
		}

		storage.ReleasePageBuffer(pageData)
		storage.ReleasePageBuffer(newPageData)

		c.LastPage = newPageId

		currentPageId = newPageId
	}
}

func (c *Collection) InsertWithId(doc map[string]any, docId uint64) error {
	c.mu.Lock()         // lock for writing
	defer c.mu.Unlock() // unlock after function ends

	data, err := record.EncodeDoc(doc)

	if err != nil {
		return err
	}

	currentPageId := c.LastPage

	for {
		//read the current page
		pageData, err := c.Pager.ReadPage(currentPageId)
		if err != nil {
			return err
		}

		//try to insert the record
		success, err := record.InsertRecord(pageData, docId, data)
		if err != nil {
			storage.ReleasePageBuffer(pageData)
			return err
		}

		if success {
			//update index
			slotCount := binary.LittleEndian.Uint16(pageData[0:2])

			err := c.BTree.Insert(docId, currentPageId, slotCount-1)
			if err != nil {
				storage.ReleasePageBuffer(pageData)
				return err
			}

			// write back the page if insertion successful
			err = c.Pager.WritePage(currentPageId, pageData)
			storage.ReleasePageBuffer(pageData)
			return err
		}

		//move to next page if insertion failed
		nextPage := binary.LittleEndian.Uint32(pageData[4:8])
		if nextPage != 0 {
			// if there is a next page in chain, move to it
			currentPageId = nextPage
			storage.ReleasePageBuffer(pageData)
			continue
		}

		// allocate a new page if no next page

		newPageId, err := c.Pager.AllocatePage(c.Header)

		if err != nil {
			storage.ReleasePageBuffer(pageData)
			return err
		}

		newPageData := storage.GetBuff()
		storage.InitDataPage(newPageData)

		if err := c.Pager.WritePage(newPageId, newPageData); err != nil {
			storage.ReleasePageBuffer(newPageData)
			storage.ReleasePageBuffer(pageData)
			return err
		}

		//link old page to new page
		binary.LittleEndian.PutUint32(pageData[4:8], newPageId)
		if err := c.Pager.WritePage(currentPageId, pageData); err != nil {
			storage.ReleasePageBuffer(pageData)
			storage.ReleasePageBuffer(newPageData)
			return err
		}

		storage.ReleasePageBuffer(pageData)
		storage.ReleasePageBuffer(newPageData)

		c.LastPage = newPageId

		currentPageId = newPageId
	}
}

func (c *Collection) InsertMany(docs []map[string]any) (*[]uint64, error) {
	c.mu.Lock()         // lock for writing
	defer c.mu.Unlock() // unlock after function ends
	//docId := GenerateRandomId(6)

	// doc["_id"] = docId

	// data, err := record.EncodeDoc(doc)
	var docIds []uint64

	docsLen := len(docs)

	currentPageId := c.LastPage

	i := 0
	for i < docsLen {
		docId := GenerateRandomId(6)
		doc := docs[i]

		doc["_id"] = docId

		data, err := record.EncodeDoc(doc)

		if err != nil {
			return &[]uint64{0}, err
		}
		//read the current page
		pageData, err := c.Pager.ReadPage(currentPageId)
		if err != nil {
			return &[]uint64{0}, err
		}

		//try to insert the record
		success, err := record.InsertRecord(pageData, docId, data)
		if err != nil {
			storage.ReleasePageBuffer(pageData)
			return &[]uint64{0}, err
		}

		if success {
			//update index
			slotCount := binary.LittleEndian.Uint16(pageData[0:2])

			err := c.BTree.Insert(docId, currentPageId, slotCount-1)
			if err != nil {
				storage.ReleasePageBuffer(pageData)
				return &[]uint64{0}, err
			}

			// write back the page if insertion successful
			err = c.Pager.WritePage(currentPageId, pageData)
			storage.ReleasePageBuffer(pageData)
			if err != nil {
				return &[]uint64{0}, err
			}
			docIds = append(docIds, docId)
			i++
			continue
		}

		//move to next page if insertion failed
		nextPage := binary.LittleEndian.Uint32(pageData[4:8])
		if nextPage != 0 {
			// if there is a next page in chain, move to it
			currentPageId = nextPage
			storage.ReleasePageBuffer(pageData)
			continue
		}

		// allocate a new page if no next page

		newPageId, err := c.Pager.AllocatePage(c.Header)

		if err != nil {
			storage.ReleasePageBuffer(pageData)
			return &[]uint64{0}, err
		}

		newPageData := storage.GetBuff()
		storage.InitDataPage(newPageData)

		if err := c.Pager.WritePage(newPageId, newPageData); err != nil {
			storage.ReleasePageBuffer(newPageData)
			storage.ReleasePageBuffer(pageData)
			return &[]uint64{0}, err
		}

		//link old page to new page
		binary.LittleEndian.PutUint32(pageData[4:8], newPageId)
		if err := c.Pager.WritePage(currentPageId, pageData); err != nil {
			storage.ReleasePageBuffer(pageData)
			storage.ReleasePageBuffer(newPageData)
			return &[]uint64{0}, err
		}

		storage.ReleasePageBuffer(pageData)
		storage.ReleasePageBuffer(newPageData)

		c.LastPage = newPageId

		currentPageId = newPageId
	}
	return &docIds, nil
}

func (c *Collection) FindById(docId uint64) (map[string]any, error) {
	c.mu.RLock()         // lock for reading
	defer c.mu.RUnlock() // unlock after function ends

	res, err := c.BTree.SearchKey(docId)

	if err != nil {
		return nil, err
	}

	if !res.Found {
		return nil, nil
	}

	pageData, err := c.Pager.ReadPage(res.PageNum)
	if err != nil {
		return nil, err
	}

	defer storage.ReleasePageBuffer(pageData)

	_, data, deleted := record.ReadRecord(pageData, res.SlotNum)

	if deleted {
		return nil, nil
	}

	doc, err := record.DecodeDoc(data)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

func (c *Collection) UpdateById(id uint64, newData map[string]any) error {
	c.mu.Lock()         // lock for writing
	defer c.mu.Unlock() // unlock after function ends

	res, err := c.BTree.SearchKey(id)

	if err != nil {
		return err
	}

	if !res.Found {
		return fmt.Errorf("document with ID %d does not exist", id)
	}

	newData["_id"] = id

	//serialize new data
	data, err := record.EncodeDoc(newData)
	if err != nil {
		return err
	}

	currPageId := c.LastPage

	for {
		pageData, err := c.Pager.ReadPage(currPageId)
		if err != nil {
			return err
		}

		success, err := record.InsertRecord(pageData, id, data)
		if err != nil {
			storage.ReleasePageBuffer(pageData)
			return err
		}
		//if update successful, write back and update index
		if success {

			if currPageId == res.PageNum {
				record.MarkSlotDeleted(pageData, res.SlotNum)
			} else {
				oldPageData, err := c.Pager.ReadPage(res.PageNum)
				if err != nil {
					storage.ReleasePageBuffer(pageData)
					return err
				}
				record.MarkSlotDeleted(oldPageData, res.SlotNum)
				if err := c.Pager.WritePage(res.PageNum, oldPageData); err != nil {
					storage.ReleasePageBuffer(oldPageData)
					storage.ReleasePageBuffer(pageData)
					return err
				}
			}

			slotCount := binary.LittleEndian.Uint16(pageData[0:2])

			if err := c.BTree.Update(id, currPageId, slotCount-1); err != nil {
				storage.ReleasePageBuffer(pageData)
				return err
			}
			c.LastPage = currPageId
			err := c.Pager.WritePage(currPageId, pageData)

			storage.ReleasePageBuffer(pageData)

			return err
		}

		// move to next page if update failed
		nextPage := binary.LittleEndian.Uint32(pageData[4:8])

		if nextPage != 0 {
			currPageId = nextPage
			storage.ReleasePageBuffer(pageData)
			continue
		}

		// allocate new page if no next page
		newPageId, err := c.Pager.AllocatePage(c.Header)
		if err != nil {
			storage.ReleasePageBuffer(pageData)
			return err
		}

		newPageData := storage.GetBuff()
		storage.InitDataPage(newPageData)
		if err := c.Pager.WritePage(newPageId, newPageData); err != nil {
			storage.ReleasePageBuffer(newPageData)
			storage.ReleasePageBuffer(pageData)
			return err
		}

		// link old page to new page
		binary.LittleEndian.PutUint32(pageData[4:8], newPageId)
		if err := c.Pager.WritePage(currPageId, pageData); err != nil {
			storage.ReleasePageBuffer(pageData)
			storage.ReleasePageBuffer(newPageData)
			return err
		}
		currPageId = newPageId
		storage.ReleasePageBuffer(pageData)
		storage.ReleasePageBuffer(newPageData)
	}
}

func (c *Collection) DeleteById(id uint64) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	res, err := c.BTree.SearchKey(id)

	if err != nil {
		return err
	}

	if !res.Found {
		return fmt.Errorf("document with ID %d does not exist", id)
	}

	page, err := c.Pager.ReadPage(res.PageNum)
	if err != nil {
		return err
	}

	defer storage.ReleasePageBuffer(page)

	record.MarkSlotDeleted(page, res.SlotNum)

	if err := c.Pager.WritePage(res.PageNum, page); err != nil {
		return err
	}

	return c.BTree.Delete(id)
}

func (c *Collection) FindAndDelete(query map[string]any) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentPageId := c.RootPage
	for currentPageId != 0 {
		pageData, err := c.Pager.ReadPage(currentPageId)

		if err != nil {
			return false, err
		}

		slotCount := binary.LittleEndian.Uint16(pageData[0:2])

		isDirty := false
		for slot := range slotCount {

			docId, data, deleted := record.ReadRecord(pageData, slot)

			if deleted {
				continue
			}

			doc, err := record.DecodeDoc(data)
			if err != nil {
				storage.ReleasePageBuffer(pageData)
				return false, err
			}
			if match(doc, query) {
				isDirty = true
				record.MarkSlotDeleted(pageData, slot)
				c.BTree.Delete(docId)
			}
		}
		if isDirty {
			if err := c.Pager.WritePage(currentPageId, pageData); err != nil {
				storage.ReleasePageBuffer(pageData)
				return false, err
			}
		}
		currentPageId = binary.LittleEndian.Uint32(pageData[4:8])
		storage.ReleasePageBuffer(pageData)
	}
	return true, nil
}

func (c *Collection) Find(query map[string]any, opts *FindOptions) ([]map[string]any, []uint64, error) {
	var results []map[string]any = make([]map[string]any, 0)
	var docIds []uint64

	isThereLimit := false
	var limit uint = 0
	var skip uint = 0

	if opts != nil {
		if opts.Limit > 0 {
			isThereLimit = true
			limit = opts.Limit
		}
		if opts.Skip > 0 {
			skip = opts.Skip
		}
	}

	currentPageId := c.RootPage
	for currentPageId != 0 {
		pageData, err := c.Pager.ReadPage(currentPageId)

		if err != nil {
			return nil, []uint64{0}, err
		}

		slotCount := binary.LittleEndian.Uint16(pageData[0:2])

		for slot := range slotCount {

			docId, data, deleted := record.ReadRecord(pageData, slot)

			if deleted {
				continue
			}

			doc, err := record.DecodeDoc(data)
			if err != nil {
				storage.ReleasePageBuffer(pageData)
				return nil, []uint64{0}, err
			}
			if match(doc, query) {

				if skip > 0 {
					skip--
					continue
				}

				results = append(results, doc)
				docIds = append(docIds, docId)
				if isThereLimit {
					limit--
					if limit == 0 {
						storage.ReleasePageBuffer(pageData)
						return results, docIds, nil
					}
				}
			}
		}
		currentPageId = binary.LittleEndian.Uint32(pageData[4:8])
		storage.ReleasePageBuffer(pageData)
	}

	return results, docIds, nil
}

func (c *Collection) FindAllDocIds(query map[string]any) ([]uint64, error) {
	var results []uint64

	currentPageId := c.RootPage
	for currentPageId != 0 {
		pageData, err := c.Pager.ReadPage(currentPageId)

		if err != nil {
			return []uint64{0}, err
		}

		slotCount := binary.LittleEndian.Uint16(pageData[0:2])

		for slot := range slotCount {

			docId, data, deleted := record.ReadRecord(pageData, slot)

			if deleted {
				continue
			}

			doc, err := record.DecodeDoc(data)
			if err != nil {
				storage.ReleasePageBuffer(pageData)
				return []uint64{0}, err
			}
			if match(doc, query) {
				results = append(results, docId)
			}
		}
		currentPageId = binary.LittleEndian.Uint32(pageData[4:8])
		storage.ReleasePageBuffer(pageData)
	}

	return results, nil
}

func (c *Collection) FindOne(query map[string]any) (map[string]any, error) {

	currentPageId := c.RootPage
	for currentPageId != 0 {
		pageData, err := c.Pager.ReadPage(currentPageId)

		if err != nil {
			return nil, err
		}

		slotCount := binary.LittleEndian.Uint16(pageData[0:2])

		for slot := range slotCount {

			_, data, deleted := record.ReadRecord(pageData, slot)

			if deleted {
				continue
			}

			doc, err := record.DecodeDoc(data)
			if err != nil {
				storage.ReleasePageBuffer(pageData)
				return nil, err
			}
			if match(doc, query) {
				storage.ReleasePageBuffer(pageData)
				return doc, nil
			}
		}
		currentPageId = binary.LittleEndian.Uint32(pageData[4:8])
		storage.ReleasePageBuffer(pageData)
	}

	return nil, nil
}
