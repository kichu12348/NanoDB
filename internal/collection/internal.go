package collection

import (
	"encoding/binary"
	"fmt"
	"nanodb/internal/record"
	"nanodb/internal/storage"
)

func (c *Collection) insertDocInternal(docId uint64, data []byte) error {
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

func (c *Collection) deleteDocInternal(id uint64) error {
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

func (c *Collection) findByIdInternal(docId uint64) (map[string]any, error) {
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
