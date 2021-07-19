package ioque

import (
	"fmt"
	asyncio "iomaster/src/async_io"
)

type WriterQueue struct {
	PartSize int

	C chan [][]byte

	writerBuilder func() *asyncio.AsyncWriter
}

func NewWriteQueue() *WriterQueue {
	return &WriterQueue{
		PartSize: 0,

		C: make(chan [][]byte, asyncio.DefaultCHBuffer),
	}
}

func (q *WriterQueue) WithPartitionSize(size int) *WriterQueue { q.PartSize = size; return q }
func (q *WriterQueue) WithWriterBuilder(builder func() *asyncio.AsyncWriter) *WriterQueue {
	q.writerBuilder = builder
	return q
}

func (q *WriterQueue) Run() {
	for func() (needNewPart bool) {
		wtr := q.writerBuilder()
		wtr.Run()
		defer wtr.Close()

		counter := 0
		for bs := range q.C {
			wtr.C <- bs
			counter++
			if q.PartSize > 0 {
				if counter == q.PartSize {
					return true
				} else if counter > q.PartSize {
					panic(fmt.Errorf("internal error: write item over size of partittion"))
				}
			}
		}
		return false
	}() {
		// do nothing
	}
}

func (j *WriterQueue) Close() {
	close(j.C)
}
