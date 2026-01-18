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
	docId := GenerateRandomId(6)

	doc["_id"] = docId

	data, err := record.EncodeDoc(doc)

	if err != nil {
		return 0, err
	}

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

	c.mu.Lock()
	err, _, _ = c.insertDocInternal(docId, data)
	c.mu.Unlock()

	if err != nil {
		return 0, err
	}

	if embedding != nil {
		if err := c.InsertVector(docId, embedding); err != nil {
			return docId, err
		}
	}

	return docId, nil
}

func (c *Collection) InsertMany(docs []map[string]any) (*[]uint64, error) {
	var docIds []uint64

	type VecReq struct {
		id  uint64
		vec []float32
	}

	var vectorsToInsert []VecReq

	for _, doc := range docs {
		id := GenerateRandomId(6)
		doc["_id"] = id
		docIds = append(docIds, id)

		if val, ok := doc["_embeddings"]; ok {

			if vectorInterface, ok := val.([]any); ok {
				vec := make([]float32, len(vectorInterface))
				for i, v := range vectorInterface {
					vec[i] = float32(convertToFloat(v))
				}
				vectorsToInsert = append(vectorsToInsert, VecReq{id, vec})
			}
			delete(doc, "_embeddings")
		}
	}

	c.mu.Lock()

	docLen := len(docs)

	currentPageId := c.LastPage
	i := 0

	for i < docLen {
		page, err := c.Pager.ReadPage(currentPageId)
		if err != nil {
			c.mu.Unlock()
			return &[]uint64{}, err
		}

		isDirty := false

		type PendingIndexUpdate struct {
			docId uint64
			slot  uint16
		}
		var batchUpdates []PendingIndexUpdate

		for i < docLen {
			doc := docs[i]
			docId := docIds[i]

			data, err := record.EncodeDoc(doc)

			if err != nil {
				storage.ReleasePageBuffer(page)
				c.mu.Unlock()
				return &[]uint64{}, err
			}

			success, err := record.InsertRecord(page, docId, data)

			if err != nil {
				storage.ReleasePageBuffer(page)
				c.mu.Unlock()
				return &[]uint64{}, err
			}

			if !success {
				break
			}

			isDirty = true

			slotCount := binary.LittleEndian.Uint16(page[0:2])
			batchUpdates = append(batchUpdates, PendingIndexUpdate{docId: docId, slot: slotCount - 1})

			i++
		}
		nextPage := binary.LittleEndian.Uint32(page[4:8])

		if isDirty {
			if err := c.Pager.WritePage(currentPageId, page); err != nil {
				storage.ReleasePageBuffer(page)
				c.mu.Unlock()
				return &[]uint64{}, err
			}
		}

		for _, update := range batchUpdates {
			if err := c.BTree.Insert(update.docId, currentPageId, update.slot); err != nil {
				storage.ReleasePageBuffer(page)
				c.mu.Unlock()
				return &[]uint64{}, err
			}
		}

		if i >= docLen {
			storage.ReleasePageBuffer(page)
			break
		}

		if nextPage != 0 {
			currentPageId = nextPage
			storage.ReleasePageBuffer(page)
			continue
		}

		newPageId, err := c.Pager.AllocatePage(c.Header)

		if err != nil {
			storage.ReleasePageBuffer(page)
			c.mu.Unlock()
			return &[]uint64{}, err
		}

		newPageData := storage.GetBuff()
		storage.InitDataPage(newPageData)

		if err := c.Pager.WritePage(newPageId, newPageData); err != nil {
			storage.ReleasePageBuffer(newPageData)
			storage.ReleasePageBuffer(page)
			c.mu.Unlock()
			return &[]uint64{}, err
		}

		storage.ReleasePageBuffer(newPageData)

		binary.LittleEndian.PutUint32(page[4:8], newPageId)

		if err := c.Pager.WritePage(currentPageId, page); err != nil {
			storage.ReleasePageBuffer(page)
			c.mu.Unlock()
			return &[]uint64{}, err
		}

		storage.ReleasePageBuffer(page)

		c.LastPage = newPageId
		currentPageId = newPageId
	}

	c.mu.Unlock()

	for _, req := range vectorsToInsert {
		c.InsertVector(req.id, req.vec)
	}

	return &docIds, nil
}

func (c *Collection) FindById(docId uint64) (map[string]any, error) {
	c.mu.RLock()         // lock for reading
	defer c.mu.RUnlock() // unlock after function ends

	return c.findByIdInternal(docId)
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

			if err := c.Pager.WritePage(currPageId, pageData); err != nil {
				storage.ReleasePageBuffer(pageData)
				return err
			}

			slotCount := binary.LittleEndian.Uint16(pageData[0:2])

			if err := c.BTree.Update(id, currPageId, slotCount-1); err != nil {
				storage.ReleasePageBuffer(pageData)
				return err
			}

			if currPageId == res.PageNum {
				record.MarkSlotDeleted(pageData, res.SlotNum)
				c.Pager.WritePage(currPageId, pageData)
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

				storage.ReleasePageBuffer(oldPageData)
			}

			c.LastPage = currPageId

			storage.ReleasePageBuffer(pageData)

			return nil
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

	return c.deleteDocInternal(id)
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
