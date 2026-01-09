package btree

import "encoding/binary"

const INTERNAL_CELL_SIZE = 12

func (n *Node) GetInternalCell(index uint16) (uint64, uint32) {
	offset := 12 + (index * INTERNAL_CELL_SIZE)

	id := binary.LittleEndian.Uint64(n.bytes[offset : offset+8])
	childPage := binary.LittleEndian.Uint32(n.bytes[offset+8 : offset+12])

	return id, childPage
}

func (n *Node) InsertInternalCell(index uint16, id uint64, childPage uint32) {

	numCells := n.NumCells()

	if index < numCells {
		offset := 12 + (index * INTERNAL_CELL_SIZE)
		end := 12 + (numCells * INTERNAL_CELL_SIZE)
		copy(n.bytes[offset+INTERNAL_CELL_SIZE:], n.bytes[offset:end])
	}

	offset := 12 + (index * INTERNAL_CELL_SIZE)

	binary.LittleEndian.PutUint64(n.bytes[offset:offset+8], id)
	binary.LittleEndian.PutUint32(n.bytes[offset+8:offset+12], childPage)

	n.SetNumCells(numCells + 1)
}
