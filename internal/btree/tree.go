package btree

import (
	"encoding/binary"
	"fmt"
	"nanodb/internal/storage"
)

type Btree struct {
	Pager    *storage.Pager
	Header   *storage.DBHeader
	RootPage uint32
}

type SearchResult struct {
	PageNum uint32
	SlotNum uint16
	Found   bool
}

const MAX_LEAF_CELLS = (storage.PageSize - 8) / LEAF_CELL_SIZE
const MAX_INTERNAL_CELLS = (storage.PageSize - 12) / INTERNAL_CELL_SIZE

func (t *Btree) SearchKey(key uint64) (SearchResult, error) {
	currPageNum := t.RootPage

	for {
		page, err := t.Pager.ReadPage(currPageNum)

		if err != nil {
			return SearchResult{}, err
		}

		node := NewNode(page)

		if node.IsLeaf() {
			return t.searchLeafNode(node, key)
		}

		currPageNum = t.searchInternalNode(node, key)
	}
}

func (t *Btree) searchLeafNode(n *Node, key uint64) (SearchResult, error) {
	numCells := n.NumCells()

	low := uint16(0)
	high := numCells

	for low < high {
		mid := (low + high) / 2
		cellKey, pageNum, slotNum := n.GetLeafCell(mid)
		if cellKey == key {
			return SearchResult{
				PageNum: pageNum,
				SlotNum: slotNum,
				Found:   true,
			}, nil
		}

		if key > cellKey {
			low = mid + 1
		} else if key < cellKey {
			high = mid
		}
	}

	return SearchResult{Found: false}, nil
}

func (t *Btree) searchInternalNode(n *Node, key uint64) uint32 {
	numCells := n.NumCells()

	for i := range numCells {
		cellKey, PageNum := n.GetInternalCell(i)

		if key < cellKey {
			return PageNum
		}
	}
	return n.RightChild()
}

func (t *Btree) Insert(key uint64, recPage uint32, recSlot uint16) error {
	splitKey, splitPage, err := t.insertRecursive(t.RootPage, key, recPage, recSlot)
	if err != nil {
		return err
	}

	// no split occured
	if splitPage == 0 {
		return nil
	}

	// split occurred then create a brand new root

	newPageId, err := t.Pager.AllocatePage(t.Header)
	if err != nil {
		return err
	}

	newNodeData := make([]byte, storage.PageSize)
	newRoot := NewNode(newNodeData)

	newRoot.SetHeader(NodeTypeInternal, true)

	//old root becomes left child
	newRoot.InsertInternalCell(0, splitKey, t.RootPage)

	//split page becomes right child
	newRoot.SetRightChild(splitPage)

	if err := t.Pager.WritePage(newPageId, newRoot.bytes); err != nil {
		return err
	}

	t.RootPage = newPageId
	return nil
}

func (t *Btree) insertRecursive(pageId uint32, key uint64, recPage uint32, recSlot uint16) (uint64, uint32, error) {
	page, err := t.Pager.ReadPage(pageId)
	if err != nil {
		return 0, 0, err
	}
	node := NewNode(page)

	//if leaf insert into leaf
	if node.IsLeaf() {
		return t.insertIntoLeaf(node, pageId, key, recPage, recSlot)
	}

	childPageId := node.RightChild()

	for i := range node.NumCells() {
		k, child := node.GetInternalCell(i)
		if key < k {
			childPageId = child
			break
		}
	}

	splitKey, splitPageId, err := t.insertRecursive(childPageId, key, recPage, recSlot)
	if err != nil {
		return 0, 0, err
	}

	//child didnt split do nothing
	if splitPageId == 0 {
		return 0, 0, nil
	}

	//child had split insert seperator into this node
	return t.insertIntoInternal(node, pageId, splitKey, splitPageId)
}

func (t *Btree) insertIntoLeaf(n *Node, pageId uint32, key uint64, recPage uint32, recSlot uint16) (uint64, uint32, error) {

	// there is space in leaf

	if n.NumCells() < MAX_LEAF_CELLS {
		// sorted postion
		insertIdx := uint16(0)
		for i := range n.NumCells() {
			k, _, _ := n.GetLeafCell(i)
			if key < k {
				break
			}
			insertIdx++
		}

		n.InsertLeafCell(insertIdx, key, recPage, recSlot)

		if err := t.Pager.WritePage(pageId, n.bytes); err != nil {
			return 0, 0, err
		}

		return 0, 0, nil
	}

	// there is no space left

	// allocate new page
	newPageId, err := t.Pager.AllocatePage(t.Header)

	if err != nil {
		return 0, 0, err
	}

	newPageData := make([]byte, storage.PageSize)
	newNode := NewNode(newPageData)

	// mark as leaf not root
	newNode.SetHeader(NodeTypeLeaf, false)

	//split cells in half

	splitPoint := n.NumCells() / 2

	newIdx := uint16(0)

	for i := splitPoint; i < n.NumCells(); i++ {
		k, p, s := n.GetLeafCell(i)
		newNode.InsertLeafCell(newIdx, k, p, s)
		newIdx++
	}

	n.SetNumCells(splitPoint)

	firstKeyOnRight, _, _ := newNode.GetLeafCell(0)

	if key < firstKeyOnRight {
		// insert into left node (old)
		insertIdx := uint16(0)
		for i := range n.NumCells() {
			k, _, _ := n.GetLeafCell(i)
			if key < k {
				break
			}
			insertIdx++
		}

		n.InsertLeafCell(insertIdx, key, recPage, recSlot)
	} else {
		// insert into (new) node right
		insertIdx := uint16(0)
		for i := range newNode.NumCells() {
			k, _, _ := newNode.GetLeafCell(i)
			if key < k {
				break
			}
			insertIdx++
		}

		newNode.InsertLeafCell(insertIdx, key, recPage, recSlot)
	}

	if err := t.Pager.WritePage(newPageId, newNode.bytes); err != nil {
		return 0, 0, err
	}

	if err := t.Pager.WritePage(pageId, n.bytes); err != nil {
		return 0, 0, err
	}

	// first key of right node is the seperator key

	splitKey, _, _ := newNode.GetLeafCell(0)

	return splitKey, newPageId, nil
}

func (t *Btree) insertIntoInternal(n *Node, pageId uint32, key uint64, childPage uint32) (uint64, uint32, error) {

	// fits in node
	if n.NumCells() < MAX_INTERNAL_CELLS {
		insertIdx := uint16(0)
		for i := range n.NumCells() {
			k, _ := n.GetInternalCell(i)
			if key < k {
				break
			}
			insertIdx++
		}
		n.InsertInternalCell(insertIdx, key, childPage)

		if err := t.Pager.WritePage(pageId, n.bytes); err != nil {
			return 0, 0, err
		}

		return 0, 0, nil
	}

	//doesnt fit in node

	newPageId, err := t.Pager.AllocatePage(t.Header)

	if err != nil {
		return 0, 0, err
	}

	newPageData := make([]byte, storage.PageSize)
	newNode := NewNode(newPageData)

	newNode.SetHeader(NodeTypeInternal, false)

	type cell struct {
		key       uint64
		childPage uint32
	}

	var cells []cell

	for i := range n.NumCells() {
		k, p := n.GetInternalCell(i)
		cells = append(cells, cell{key: k, childPage: p})
	}

	inserted := false
	for i := range len(cells) {
		if key < cells[i].key {
			cells = append(cells[:i], append([]cell{{key, childPage}}, cells[i:]...)...)
			inserted = true
		}
	}

	if !inserted {
		cells = append(cells, cell{key, childPage})
	}

	oldRightChild := n.RightChild()

	mid := len(cells) / 2

	promotedKey := cells[mid].key

	// Left node keeps: cells[0 : mid]
	// Right node gets: cells[mid+1 : ]

	n.SetNumCells(0)

	for i := range mid {
		n.InsertInternalCell(uint16(i), cells[i].key, cells[i].childPage)
	}

	n.SetRightChild(cells[mid].childPage)

	rightIdx := uint16(0)
	for i := mid + 1; i < len(cells); i++ {
		newNode.InsertInternalCell(rightIdx, cells[i].key, cells[i].childPage)
		rightIdx++
	}

	newNode.SetRightChild(oldRightChild)

	if err := t.Pager.WritePage(pageId, n.bytes); err != nil {
		return 0, 0, err
	}

	if err := t.Pager.WritePage(newPageId, newNode.bytes); err != nil {
		return 0, 0, err
	}

	return promotedKey, newPageId, nil
}

func (t *Btree) Update(key uint64, recPage uint32, recSlot uint16) error {
	currPageNum := t.RootPage

	for {
		page, err := t.Pager.ReadPage(currPageNum)

		if err != nil {
			return err
		}

		node := NewNode(page)

		if node.IsLeaf() {
			err := t.updateLeafNode(node, key, currPageNum, recPage, recSlot)

			if err != nil {
				return err
			}

			return nil
		}

		currPageNum = t.searchInternalNode(node, key)
	}
}

func (t *Btree) updateLeafNode(n *Node, key uint64, pageId uint32, recPage uint32, recSlot uint16) error {
	numCells := n.NumCells()

	low := uint16(0)
	high := numCells

	for low < high {
		mid := (low + high) / 2
		cellKey, _, _ := n.GetLeafCell(mid)
		if cellKey == key {

			offset := 8 + mid*LEAF_CELL_SIZE

			binary.LittleEndian.PutUint32(n.bytes[offset+8:offset+12], recPage)
			binary.LittleEndian.PutUint16(n.bytes[offset+12:offset+14], recSlot)

			t.Pager.WritePage(pageId, n.bytes)

			return nil
		}

		if key > cellKey {
			low = mid + 1
		} else if key < cellKey {
			high = mid
		}
	}

	return fmt.Errorf("key %d not found", key)
}

func (t *Btree) Delete(key uint64) error {
	currPageNum := t.RootPage

	for {
		page, err := t.Pager.ReadPage(currPageNum)

		if err != nil {
			return err
		}

		node := NewNode(page)

		if node.IsLeaf() {
			err := t.deleteFromLeaf(node, currPageNum, key)

			if err != nil {
				return err
			}

			return nil
		}

		currPageNum = t.searchInternalNode(node, key)
	}
}

func (t *Btree) deleteFromLeaf(n *Node, pageId uint32, key uint64) error {
	numCells := n.NumCells()

	foundIdx := -1

	low := uint16(0)
	high := numCells
	for low < high {
		mid := (low + high) / 2

		cellKey, _, _ := n.GetLeafCell(mid)
		if key == cellKey {
			foundIdx = int(mid)
			break
		}

	}

	if foundIdx == -1 {
		return fmt.Errorf("key %d not found", key)
	}

	offsetStart := 8 + (foundIdx * LEAF_CELL_SIZE)
	offsetEnd := 8 + (foundIdx + 1*LEAF_CELL_SIZE)
	totalEnd := 8 + (numCells * LEAF_CELL_SIZE)

	copy(n.bytes[offsetStart:], n.bytes[offsetEnd:totalEnd])

	n.SetNumCells(numCells - 1)

	if err := t.Pager.WritePage(pageId, n.bytes); err != nil {
		return nil
	}

	return nil
}
