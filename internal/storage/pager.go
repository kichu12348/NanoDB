package storage

import (
	"os"
)

const PageSize = 4096

type Pager struct {
	file *os.File
}

func OpenPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	return &Pager{file: file}, nil
}

func (p *Pager) ReadPage(pageNum uint32) ([]byte, error) {
	buff := make([]byte, PageSize)
	_, err := p.file.ReadAt(buff, int64(pageNum)*PageSize)
	if err != nil {
		return nil, err
	}
	return buff, nil
}

func (p *Pager) WritePage(pageNum uint32, data []byte) error {
	_, err := p.file.WriteAt(data, int64(pageNum)*PageSize)
	return err
}

func (p *Pager) Close() error {
	return p.file.Close()
}
