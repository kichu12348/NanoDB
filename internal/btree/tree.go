package btree

import "nanodb/internal/storage"

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
