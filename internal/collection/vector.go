package collection

import (
	"container/heap"
	"encoding/binary"
	"math"
	"nanodb/internal/record"
	"nanodb/internal/storage"
	"nanodb/internal/vector"
)

const MAX_BUCKETS = 256

type SearchResult struct {
	DocId uint64
	Dist  float32
}

type ResultHeap []SearchResult

func (h ResultHeap) Len() int { return len(h) }
func (h ResultHeap) Less(i, j int) bool {
	return h[i].Dist > h[j].Dist
}
func (h ResultHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *ResultHeap) Push(x any) {
	*h = append(*h, x.(SearchResult))
}

func (h *ResultHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (c *Collection) InsertVector(docId uint64, v []float32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var targetPageNum uint32

	bucketLen := len(c.Buckets)

	if bucketLen < MAX_BUCKETS {
		newPageId, err := c.Pager.AllocatePage(c.Header)
		if err != nil {
			return err
		}
		buff := storage.GetBuff()

		vector.InitVectorPage(buff)

		newBucket := Bucket{
			Centroid: v,
			RootPage: newPageId,
		}
		c.Buckets = append(c.Buckets, newBucket)

		err = c.Pager.WritePage(newPageId, buff)

		storage.ReleasePageBuffer(buff)
		targetPageNum = newPageId
		if err != nil {
			return err
		}

		if err := c.saveBuckets(); err != nil {
			return err
		}
	} else {
		bestDist := float32(math.MaxFloat32)
		bestIdx := -1

		for i, b := range c.Buckets {
			d := vector.Dist(v, b.Centroid)
			if d < bestDist {
				bestDist = d
				bestIdx = i
			}
		}
		targetPageNum = c.Buckets[bestIdx].RootPage
	}
	return c.writeVectorToPageChain(targetPageNum, docId, v)
}

const HEADER_SIZE = 6

func (c *Collection) writeVectorToPageChain(rootPage uint32, docID uint64, v []float32) error {
	currPage := rootPage

	vecBytes := vector.VectorToBytes(v)

	itemSize := 8 + len(vecBytes)

	isNewPage := false

	for {
		page, err := c.Pager.ReadPage(currPage)

		if err != nil {
			return err
		}

		if isNewPage {
			vector.InitVectorPage(page)
		}

		count := binary.LittleEndian.Uint16(page[4:6])

		offset := HEADER_SIZE + (itemSize * int(count))

		if offset+itemSize > storage.PageSize {
			//cant fit in this page
			nextPage := binary.LittleEndian.Uint32(page[0:4])

			if nextPage != 0 {
				// if there is a next page then
				currPage = nextPage
				storage.ReleasePageBuffer(page)
				continue
			}

			// if there is no linked page then allocate one

			newPageId, err := c.Pager.AllocatePage(c.Header)

			if err != nil {
				storage.ReleasePageBuffer(page)
				return err
			}

			isNewPage = true

			binary.LittleEndian.PutUint32(page[0:4], newPageId)

			err = c.Pager.WritePage(currPage, page)

			storage.ReleasePageBuffer(page)

			currPage = newPageId

			if err != nil {
				return err
			}

			continue
		}

		// it fits

		binary.LittleEndian.PutUint64(page[offset:offset+8], docID)

		copy(page[offset+8:], vecBytes)

		binary.LittleEndian.PutUint16(page[4:6], count+1)

		err = c.Pager.WritePage(currPage, page)

		storage.ReleasePageBuffer(page)

		return err
	}
}

func (c *Collection) saveBuckets() error {
	var bucketList []map[string]any

	for _, b := range c.Buckets {
		centroidInterface := make([]any, len(b.Centroid))
		for i, v := range b.Centroid {
			centroidInterface[i] = v
		}

		bucketList = append(bucketList, map[string]any{
			"root": b.RootPage,
			"vec":  centroidInterface,
		})
	}

	configDoc := map[string]any{
		"_id":     uint64(1),
		"type":    "bucket_config",
		"buckets": bucketList,
	}

	data, err := record.EncodeDoc(configDoc)

	if err != nil {
		return err
	}

	c.deleteDocInternal(1)

	err, _, _ = c.insertDocInternal(1, data)

	return err
}

func (c *Collection) SearchVector(query []float32, topK int) ([]uint64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.Buckets) == 0 {
		return []uint64{}, nil
	}

	bestDist := float32(math.MaxFloat32)
	bestBucket := c.Buckets[0]

	for _, b := range c.Buckets {
		d := vector.Dist(query, b.Centroid)

		if d < bestDist {
			bestDist = d
			bestBucket = b
		}
	}

	results := &ResultHeap{}
	heap.Init(results)

	currPageId := bestBucket.RootPage

	vecSize := len(query) * 4
	itemSize := 8 + vecSize

	for currPageId != 0 {
		pageData, err := c.Pager.ReadPage(currPageId)

		if err != nil {
			return []uint64{}, err
		}

		count := binary.LittleEndian.Uint16(pageData[4:6])

		for i := range int(count) {
			offset := HEADER_SIZE + (i * itemSize)

			docId := binary.LittleEndian.Uint64(pageData[offset : offset+8])

			vecBytes := pageData[offset+8 : offset+8+vecSize]

			vec := vector.VectorFromBytes(vecBytes)
			d := vector.Dist(query, vec)

			if results.Len() < topK {
				heap.Push(results, SearchResult{
					DocId: docId,
					Dist:  d,
				})
			} else if d < (*results)[0].Dist {
				heap.Pop(results)
				heap.Push(results, SearchResult{
					DocId: docId,
					Dist:  d,
				})
			}
		}

		currPageId = binary.LittleEndian.Uint32(pageData[0:4])
		storage.ReleasePageBuffer(pageData)
	}

	finalIds := make([]uint64, results.Len())
	for i := results.Len() - 1; i >= 0; i-- {
		sr := heap.Pop(results).(SearchResult)
		finalIds[i] = sr.DocId
	}

	return finalIds, nil
}

func (c *Collection) LoadVectorIndex() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	doc, err := c.findByIdInternal(1)
	if err != nil {
		return err
	}

	if doc == nil {
		return nil
	}

	rawBuckets, ok := doc["buckets"].([]any)

	if !ok {
		return nil
	}

	for _, item := range rawBuckets {
		bMap := item.(map[string]any)

		root := uint32(convertToInt(bMap["root"]))

		rawVec := bMap["vec"].([]any)
		centroid := make([]float32, len(rawVec))

		for i, val := range rawVec {
			centroid[i] = float32(convertToFloat(val))
		}

		c.Buckets = append(c.Buckets, Bucket{
			RootPage: root,
			Centroid: centroid,
		})
	}

	return nil
}

func convertToInt(v any) uint32 {
	switch t := v.(type) {
	case uint32:
		return t
	case int:
		return uint32(t)
	case int64:
		return uint32(t)
	case uint64:
		return uint32(t)
	case float64:
		return uint32(t) // JSON often makes everything float64
	case int8:
		return uint32(t)
	default:
		return 0
	}
}

func convertToFloat(v any) float64 {
	switch t := v.(type) {
	case float32:
		return float64(t)
	case float64:
		return t
	case int:
		return float64(t)
	default:
		return 0.0
	}
}
