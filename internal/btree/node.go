package btree

import (
	"encoding/binary"
)

// []

type Node struct {
	bytes []byte
}

func NewNode(data []byte) *Node {
	return &Node{bytes: data}
}

func (n *Node) SetHeader(nodeType uint8, isRoot bool) {
	n.bytes[0] = nodeType // 2 leaf, 1 internal
	if isRoot {
		n.bytes[1] = 1
	} else {
		n.bytes[1] = 0
	}
}

func (n *Node) IsLeaf() bool {
	return n.bytes[0] == 2
}

func (n *Node) NumCells() uint16 {
	return binary.LittleEndian.Uint16(n.bytes[6:8])
}

func (n *Node) SetNumCells(num uint16) {
	binary.LittleEndian.AppendUint16(n.bytes[6:8], num)
}

func (n *Node) Parent() uint32 {
	return binary.LittleEndian.Uint32(n.bytes[2:6])
}

func (n *Node) SetParent(pid uint32) {
	binary.LittleEndian.PutUint32(n.bytes[2:6], pid)
}

const LEAF_CELL_SIZE = 14 // 8 byte ID + 4 byte Page + 2 byte Slot

func (n *Node) GetLeafCell(index uint16) (uint64, uint32, uint16) {
	offset := 8 + LEAF_CELL_SIZE*index

	id := binary.LittleEndian.Uint64(n.bytes[offset : offset+8])
	pageNum := binary.LittleEndian.Uint32(n.bytes[offset+8 : offset+12])
	slotNum := binary.LittleEndian.Uint16(n.bytes[offset+12 : offset+14])

	return id, pageNum, slotNum
}

func (n *Node) InsertLeafCell(index uint16, id uint64, pageNum uint32, slotNum uint16) {

	numCells := n.NumCells()

	if index < numCells {
		offset := 8 + index*LEAF_CELL_SIZE
		end := 8 + numCells*LEAF_CELL_SIZE
		copy(n.bytes[offset+LEAF_CELL_SIZE:], n.bytes[offset:end])
	}

	offset := 8 + index*LEAF_CELL_SIZE

	binary.LittleEndian.PutUint64(n.bytes[offset:offset+8], id)
	binary.LittleEndian.PutUint32(n.bytes[offset+8:offset+12], pageNum)
	binary.LittleEndian.PutUint16(n.bytes[offset+12:offset+14], slotNum)

	n.SetNumCells(numCells + 1)
}
