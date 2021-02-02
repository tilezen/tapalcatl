package buffer

import "bytes"

type BufferManager interface {
	Get() *bytes.Buffer
	Put(*bytes.Buffer)
}

type OnDemandBufferManager struct{}

func (bm *OnDemandBufferManager) Get() *bytes.Buffer {
	return &bytes.Buffer{}
}

func (bm *OnDemandBufferManager) Put(buf *bytes.Buffer) {
}
