package record

import (
	"encoding/binary"
	"nanodb/internal/storage"

	"github.com/vmihailenco/msgpack/v5"
)

const DeletedFlag uint16 = 0x8000 // 1000 0000 0000 0000

type Slot struct {
	Offset uint16
	Length uint16
}

type CollectionEntry struct {
	Name      string
	RootPage  uint32
	IndexRoot uint32
}

func isDeleted(len uint16) bool {
	return (len & DeletedFlag) != 0
}

func GetActualLen(len uint16) uint16 {
	return len & ^DeletedFlag
}

func writeUint16(b []byte, v uint16) {
	binary.LittleEndian.PutUint16(b, v)
}
func writeUint32(b []byte, v uint32) {
	binary.LittleEndian.PutUint32(b, v)
}
func writeUint64(b []byte, v uint64) {
	binary.LittleEndian.PutUint64(b, v)
}

func EncodeDoc(doc map[string]any) ([]byte, error) {
	return msgpack.Marshal(doc)
}

func DecodeDoc(data []byte) (map[string]any, error) {
	var doc map[string]any
	err := msgpack.Unmarshal(data, &doc)
	return doc, err
}

func InsertRecord(
	page []byte,
	docId uint64,
	data []byte,
) (bool, error) {

	slotCount := binary.LittleEndian.Uint16(page[0:2])
	freeStart := binary.LittleEndian.Uint16(page[2:4])

	recordSize := uint16(12 + len(data))
	slotLength := uint16(4)

	requiredSpace := recordSize + slotLength

	headeEnd := uint16(8 + slotCount*4)

	if freeStart < headeEnd+requiredSpace {
		return false, nil // page full
	}

	//write record
	recordOffset := freeStart - recordSize
	writeUint64(page[recordOffset:], docId)
	writeUint32(page[recordOffset+8:], uint32(len(data)))
	copy(page[recordOffset+12:], data)

	//write slot
	slotOffset := 8 + slotCount*4
	writeUint16(page[slotOffset:], recordOffset)
	writeUint16(page[slotOffset+2:], recordSize)

	//update page header
	slotCount++
	freeStart = recordOffset
	writeUint16(page[0:2], slotCount)
	writeUint16(page[2:4], freeStart)

	return true, nil
}

func ReadRecord(page []byte, slot uint16) (uint64, []byte, bool) {
	slotOffset := 8 + slot*4

	offset := binary.LittleEndian.Uint16(page[slotOffset:])
	recordLen := binary.LittleEndian.Uint16(page[slotOffset+2:])

	if isDeleted(recordLen) {
		return 0, nil, true
	}

	docId := binary.LittleEndian.Uint64(page[offset:])
	dataLen := binary.LittleEndian.Uint32(page[offset+8:])

	data := make([]byte, dataLen)
	copy(data, page[offset+12:offset+12+uint16(dataLen)])

	return docId, data, false
}

func EncodeCollectionEntry(name string, root uint32, indexRoot uint32) []byte { // [name length (1 byte), name (n bytes), root page (4 bytes), index page (4 bytes)]
	buff := make([]byte, len(name)+9)
	buff[0] = byte(len(name))    // name length
	copy(buff[1:], []byte(name)) // name
	writeUint32(buff[1+len(name):], root)
	writeUint32(buff[5+len(name):], indexRoot)
	return buff
}

func DecodeCollectionEntry(data []byte) CollectionEntry {
	nameLen := int(data[0])
	name := string(data[1 : 1+nameLen])
	root := binary.LittleEndian.Uint32(data[1+nameLen : 5+nameLen])
	indexRoot := binary.LittleEndian.Uint32(data[5+nameLen:])
	return CollectionEntry{name, root, indexRoot}
}

func GetAllCollections(p *storage.Pager) ([]CollectionEntry, error) {

	pageData, err := p.ReadPage(1)
	if err != nil {
		return nil, err
	}

	slotCount := binary.LittleEndian.Uint16(pageData[0:2])

	var collections []CollectionEntry

	for slot := range slotCount {
		// 1. Read the Record
		_, data, deleted := ReadRecord(pageData, slot)

		if deleted {
			continue
		}
		// 2. Decode the specific CollectionEntry format
		// [NameLen (1)] [Name] [RootPage (4)]
		entry := DecodeCollectionEntry(data)
		collections = append(collections, entry)
	}

	return collections, nil
}

func MarkSlotDeleted(page []byte, slotIdx uint16) {
	lenOffset := 10 + slotIdx*4

	currLen := binary.LittleEndian.Uint16(page[lenOffset:])

	if isDeleted(currLen) {
		return
	}

	newLen := currLen | DeletedFlag // flip the bit

	binary.LittleEndian.PutUint16(page[lenOffset:], newLen)
}
