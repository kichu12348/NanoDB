package storage

import (
	"encoding/binary"
)

type DBHeader struct {
	Magic     [4]byte // 4-byte
	Version   uint16  // 2-byte
	PageSize  uint32
	PageCount uint32
	FreeList  uint32
}

type PageHeader struct {
	SlotCount uint16
	FreeStart uint16
	NextPage  uint32
}

func WriteHeader(p *Pager, h *DBHeader) error {
	buff := make([]byte, PageSize)
	copy(buff[0:4], h.Magic[:])

	binary.LittleEndian.PutUint16(buff[4:6], h.Version)
	binary.LittleEndian.PutUint32(buff[6:10], h.PageSize)
	binary.LittleEndian.PutUint32(buff[10:14], h.PageCount)
	binary.LittleEndian.PutUint32(buff[14:18], h.FreeList)

	return p.WritePage(0, buff)
}

func ReadHeader(p *Pager) (*DBHeader, error) {
	buff, err := p.ReadPage(0)
	if err != nil {
		return nil, err
	}
	h := &DBHeader{}
	copy(h.Magic[:], buff[0:4])
	h.Version = binary.LittleEndian.Uint16(buff[4:6])
	h.PageSize = binary.LittleEndian.Uint32(buff[6:10])
	h.PageCount = binary.LittleEndian.Uint32(buff[10:14])
	h.FreeList = binary.LittleEndian.Uint32(buff[14:18])
	return h, nil
}

func AllocatePage(p *Pager, h *DBHeader) (uint32, error) {

	if h.FreeList != 0 {
		pageNum := h.FreeList
		buff, err := p.ReadPage(pageNum)
		if err != nil {
			return 0, err
		}

		h.FreeList = binary.LittleEndian.Uint32(buff[0:4])
		err = WriteHeader(p, h)
		if err != nil {
			return 0, err
		}

		return pageNum, nil
	}

	pageNum := h.PageCount

	h.PageCount++
	err := WriteHeader(p, h)
	if err != nil {
		return 0, err
	}

	emptyPage := make([]byte, PageSize)
	err = p.WritePage(pageNum, emptyPage)

	if err != nil {
		return 0, err
	}

	return pageNum, nil
}

func FreePage(p *Pager, h *DBHeader, pageNum uint32) error {
	buff := make([]byte, PageSize)
	binary.LittleEndian.PutUint32(buff[0:4], h.FreeList)

	if err := p.WritePage(pageNum, buff); err != nil {
		return err
	}
	h.FreeList = pageNum
	return WriteHeader(p, h)
}

func InitDataPage(page []byte) {
	binary.LittleEndian.PutUint16(page[0:2], 0)                // slot count
	binary.LittleEndian.PutUint16(page[2:4], uint16(PageSize)) // free start
}
