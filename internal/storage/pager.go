package storage

import (
	"fmt"
	"os"
	"sync"
)

const PageSize = 4096

type Pager struct {
	file *os.File
	mu   sync.Mutex
}

var pagePool = sync.Pool{
	New: func() interface{} {
		return make([]byte, PageSize)
	},
}

func OpenPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	return &Pager{file: file}, nil
}

func (p *Pager) ReadPage(pageNum uint32) ([]byte, error) {
	buff := pagePool.Get().([]byte)
	_, err := p.file.ReadAt(buff, int64(pageNum)*PageSize)
	if err != nil {
		pagePool.Put(buff)
		return nil, err
	}
	return buff, nil
}

func ReleasePageBuffer(b []byte) {
	if cap(b) != PageSize {
		panic(fmt.Sprintf("ReleasePageBuffer: attempting to release invalid buffer with cap %d", cap(b)))
	}
	pagePool.Put(b)
}

func GetBuff() []byte {
	b := pagePool.Get().([]byte)
	clear(b)
	return b
}

func (p *Pager) WritePage(pageNum uint32, data []byte) error {
	_, err := p.file.WriteAt(data, int64(pageNum)*PageSize)
	return err
}

func (p *Pager) Close() error {
	return p.file.Close()
}
