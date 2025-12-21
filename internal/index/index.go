package index

import (
	"encoding/binary"
	"nanodb/internal/storage"
)

type DocLocation struct {
	Page uint32
	Slot uint16
}

type Index map[uint64]DocLocation

func BuildIndex(p *storage.Pager, root uint32) (Index, error) {
	index := make(Index)
	pageNum := root

	for pageNum != 0 {
		page, err := p.ReadPage(pageNum)
		if err != nil {
			return nil, err
		}

		slotCount := binary.LittleEndian.Uint16(page[0:2])

		for slot := range slotCount {
			offset := binary.LittleEndian.Uint16(page[4+slot*4 : 6+slot*4])
			length := binary.LittleEndian.Uint16(page[6+slot*4 : 8+slot*4])

			if length == 0 {
				continue // deleted record
			}

			docId := binary.LittleEndian.Uint64(page[offset:])

			index[docId] = DocLocation{Page: pageNum, Slot: slot}
		}

		pageNum = binary.LittleEndian.Uint32(page[4:8])
	}
	return index, nil
}
