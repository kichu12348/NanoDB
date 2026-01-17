package collection

import (
	"encoding/binary"
	"math"
	"nanodb/internal/storage"
	"nanodb/internal/vector"
)

const MAX_BUCKETS = 10

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
		centroidInterface := make([]interface{}, len(b.Centroid))
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

	exist, err := c.FindById(1)

	if err != nil {
		return err
	}

	if exist == nil {
		return c.InsertWithId(configDoc, 1)
	} else {
		c.DeleteById(1)
		return c.InsertWithId(configDoc, 1)
	}
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
		d := vector.Dist(query, bestBucket.Centroid)

		if d < bestDist {
			bestDist = d
			bestBucket = b
		}
	}

	var results []struct {
		id   uint64
		dist float32
	}

	resLen := 0

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
			results = append(results, struct {
				id   uint64
				dist float32
			}{docId, d})

			resLen++
		}

		currPageId = binary.LittleEndian.Uint32(pageData[0:4])
		storage.ReleasePageBuffer(pageData)
	}

	for i := range resLen {
		for j := range resLen - i - 1 {
			if results[j].dist > results[j+1].dist {
				results[j], results[j+1] = results[j+1], results[j]
			}
		}
	}

	finalIds := make([]uint64, 0)
	for i := 0; i < resLen && i < topK; i++ {
		finalIds = append(finalIds, results[i].id)
	}

	return finalIds, nil
}
