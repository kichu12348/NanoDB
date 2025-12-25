package collection

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"nanodb/internal/index"
	"nanodb/internal/record"
	"nanodb/internal/storage"
	"sync"
)

type Collection struct {
	Name     string
	RootPage uint32
	Pager    *storage.Pager
	Header   *storage.DBHeader
	Index    index.Index
	mu       sync.RWMutex
}

type FindOptions struct {
	Limit uint
	Skip  uint
}

func NewCollection(name string, root uint32, pager *storage.Pager, header *storage.DBHeader) (*Collection, error) {
	idx, err := index.BuildIndex(pager, root)
	if err != nil {
		return nil, err
	}
	return &Collection{
		Name:     name,
		RootPage: root,
		Pager:    pager,
		Header:   header,
		Index:    idx,
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
	docId := GenerateRandomId(8)

	doc["_id"] = docId

	data, err := record.EncodeDoc(doc)

	if err != nil {
		return 0, err
	}

	currentPageId := c.RootPage

	for {
		//read the current page
		pageData, err := c.Pager.ReadPage(currentPageId)
		if err != nil {
			return 0, err
		}

		//try to insert the record
		success, err := record.InsertRecord(pageData, docId, data)
		if err != nil {
			return 0, err
		}

		if success {
			//update index
			slotCount := binary.LittleEndian.Uint16(pageData[0:2])

			c.Index[docId] = index.DocLocation{Page: currentPageId, Slot: slotCount - 1}
			// write back the page if insertion successful

			return docId, c.Pager.WritePage(currentPageId, pageData)
		}

		//move to next page if insertion failed
		nextPage := binary.LittleEndian.Uint32(pageData[4:8])
		if nextPage != 0 {
			// if there is a next page in chain, move to it
			currentPageId = nextPage
			continue
		}

		// allocate a new page if no next page

		newPageId, err := storage.AllocatePage(c.Pager, c.Header)

		if err != nil {
			return 0, err
		}

		newPageData := make([]byte, storage.PageSize)
		storage.InitDataPage(newPageData)

		if err := c.Pager.WritePage(newPageId, newPageData); err != nil {
			return 0, err
		}

		//link old page to new page
		binary.LittleEndian.PutUint32(pageData[4:8], newPageId)
		if err := c.Pager.WritePage(currentPageId, pageData); err != nil {
			return 0, err
		}

		currentPageId = newPageId
	}

}

func (c *Collection) FindById(docId uint64) (map[string]any, error) {
	c.mu.RLock()         // lock for reading
	defer c.mu.RUnlock() // unlock after function ends
	loc, exists := c.Index[docId]

	if !exists {
		return nil, nil
	}

	fmt.Println(loc.Page, loc.Slot)

	pageData, err := c.Pager.ReadPage(loc.Page)
	if err != nil {
		return nil, err
	}
	_, data, deleted := record.ReadRecord(pageData, loc.Slot)

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
	if _, exists := c.Index[id]; !exists {
		return fmt.Errorf("document with ID %d does not exist", id)
	}

	newData["_id"] = id

	//serialize new data
	data, err := record.EncodeDoc(newData)
	if err != nil {
		return err
	}

	currPageId := c.RootPage

	for {
		pageData, err := c.Pager.ReadPage(currPageId)
		if err != nil {
			return err
		}

		success, err := record.InsertRecord(pageData, id, data)
		if err != nil {
			return err
		}
		//if update successful, write back and update index
		if success {
			slotCount := binary.LittleEndian.Uint16(pageData[0:2])
			prevLoc := c.Index[id]
			record.MarkSlotDeleted(pageData, prevLoc.Slot)
			c.Index[id] = index.DocLocation{Page: currPageId, Slot: slotCount - 1}
			return c.Pager.WritePage(currPageId, pageData)
		}

		// move to next page if update failed
		nextPage := binary.LittleEndian.Uint32(pageData[4:8])

		if nextPage != 0 {
			currPageId = nextPage
			continue
		}

		// allocate new page if no next page
		newPageId, err := storage.AllocatePage(c.Pager, c.Header)
		if err != nil {
			return err
		}

		newPageData := make([]byte, storage.PageSize)
		storage.InitDataPage(newPageData)
		if err := c.Pager.WritePage(newPageId, newPageData); err != nil {
			return err
		}

		// link old page to new page
		binary.LittleEndian.PutUint32(pageData[4:8], newPageId)
		if err := c.Pager.WritePage(currPageId, pageData); err != nil {
			return err
		}
		currPageId = newPageId
	}
}

func (c *Collection) DeleteById(id uint64) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	loc, exists := c.Index[id]
	if !exists {
		return fmt.Errorf("document with ID %d does not exist", id)
	}

	page, err := c.Pager.ReadPage(loc.Page)
	if err != nil {
		return err
	}

	record.MarkSlotDeleted(page, loc.Slot)

	if err := c.Pager.WritePage(loc.Page, page); err != nil {
		return err
	}

	delete(c.Index, id)
	return nil
}

func (c *Collection) Find(query map[string]any, opts *FindOptions) ([]map[string]any, []uint64, error) {
	var results []map[string]any
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
						return results, docIds, nil
					}
				}
			}
		}
		currentPageId = binary.LittleEndian.Uint32(pageData[4:8])
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
				return []uint64{0}, err
			}
			if match(doc, query) {
				results = append(results, docId)
			}
		}
		currentPageId = binary.LittleEndian.Uint32(pageData[4:8])
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
				return nil, err
			}
			if match(doc, query) {
				return doc, nil
			}
		}
		currentPageId = binary.LittleEndian.Uint32(pageData[4:8])
	}

	return nil, nil
}
