package asyncio

import (
	"fmt"
)

type WriteQueue struct {
	PartSize int

	C chan [][]byte

	writerBuilder func() *AsyncWriter
}

func NewWriteQueue() *WriteQueue {
	return &WriteQueue{
		PartSize: 0,

		C: make(chan [][]byte, DefaultCHBuffer),
	}
}

func (q *WriteQueue) WithPartitionSize(size int) *WriteQueue { q.PartSize = size; return q }
func (q *WriteQueue) WithWriterBuilder(builder func() *AsyncWriter) *WriteQueue {
	q.writerBuilder = builder
	return q
}

func (q *WriteQueue) Run() {
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

func (j *WriteQueue) Close() {
	close(j.C)
}
