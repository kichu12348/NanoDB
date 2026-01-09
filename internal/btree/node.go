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
	binary.LittleEndian.PutUint16(n.bytes[6:8], num)
}

func (n *Node) Parent() uint32 {
	return binary.LittleEndian.Uint32(n.bytes[2:6])
}

func (n *Node) SetParent(pid uint32) {
	binary.LittleEndian.PutUint32(n.bytes[2:6], pid)
}

func (n *Node) RightChild() uint32 {
	return binary.LittleEndian.Uint32(n.bytes[8:12])
}

func (n *Node) SetRightChild(pageNum uint32) {
	binary.LittleEndian.PutUint32(n.bytes[8:12], pageNum)
}
