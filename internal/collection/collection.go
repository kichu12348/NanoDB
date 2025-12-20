package collection

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"nanodb/internal/index"
	"nanodb/internal/record"
	"nanodb/internal/storage"
)

type Collection struct {
	Name     string
	RootPage uint32
	Pager    *storage.Pager
	Header   *storage.DBHeader
	Index    index.Index
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

func (c *Collection) Insert(doc map[string]any) error {

	data, err := record.EncodeDoc(doc)

	if err != nil {
		return err
	}

	docId := GenerateRandomId(8)

	currentPageId := c.RootPage

	for {
		//read the current page
		pageData, err := c.Pager.ReadPage(currentPageId)
		if err != nil {
			return err
		}

		//try to insert the record
		success, err := record.InsertRecord(pageData, docId, data)
		if err != nil {
			return err
		}

		if success {
			//update index
			slotCount := binary.LittleEndian.Uint16(pageData[0:2])

			c.Index[docId] = index.DocLocation{Page: currentPageId, Slot: slotCount - 1}
			// write back the page if insertion successful

			return c.Pager.WritePage(currentPageId, pageData)
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
			return err
		}

		newPageData := make([]byte, storage.PageSize)
		storage.InitDataPage(newPageData)

		if err := c.Pager.WritePage(newPageId, newPageData); err != nil {
			return err
		}

		//link old page to new page
		binary.LittleEndian.PutUint32(pageData[4:8], newPageId)
		if err := c.Pager.WritePage(currentPageId, pageData); err != nil {
			return err
		}

		currentPageId = newPageId
	}

}

func (c *Collection) FindById(docId uint64) (map[string]any, error) {
	loc, exists := c.Index[docId]

	if !exists {
		return nil, nil
	}

	fmt.Println(loc.Page, loc.Slot)

	pageData, err := c.Pager.ReadPage(loc.Page)
	if err != nil {
		return nil, err
	}
	_, data := record.ReadRecord(pageData, loc.Slot)

	doc, err := record.DecodeDoc(data)
	fmt.Println(doc)
	if err != nil {
		return nil, err
	}

	return doc, nil
}
