package btree

import (
	"encoding/binary"
)

// first 8 bytes is header

const LEAF_CELL_SIZE = 14 // 8 byte ID + 4 byte Page + 2 byte Slot

func (n *Node) GetLeafCell(index uint16) (uint64, uint32, uint16) {
	offset := 12 + LEAF_CELL_SIZE*index

	id := binary.LittleEndian.Uint64(n.bytes[offset : offset+8])
	pageNum := binary.LittleEndian.Uint32(n.bytes[offset+8 : offset+12])
	slotNum := binary.LittleEndian.Uint16(n.bytes[offset+12 : offset+14])

	return id, pageNum, slotNum
}

func (n *Node) InsertLeafCell(index uint16, id uint64, pageNum uint32, slotNum uint16) {

	numCells := n.NumCells()

	if index < numCells {
		offset := 12 + index*LEAF_CELL_SIZE
		end := 12 + numCells*LEAF_CELL_SIZE
		copy(n.bytes[offset+LEAF_CELL_SIZE:], n.bytes[offset:end])
	}

	offset := 12 + index*LEAF_CELL_SIZE

	binary.LittleEndian.PutUint64(n.bytes[offset:offset+8], id)
	binary.LittleEndian.PutUint32(n.bytes[offset+8:offset+12], pageNum)
	binary.LittleEndian.PutUint16(n.bytes[offset+12:offset+14], slotNum)

	n.SetNumCells(numCells + 1)
}
