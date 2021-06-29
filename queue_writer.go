package asyncio

import "io"

type WriteQueue struct {
	PartSize int

	C chan []byte
}

func NewWriteQueue(partitionSize int) *WriteQueue {
	return &WriteQueue{
		PartSize: partitionSize,

		C: make(chan []byte, DefaultCHBuffer),
	}
}

func (q *WriteQueue) WithPartitionSize(size int) *WriteQueue { q.PartSize = size; return q }

func (q *WriteQueue) Run(writerBuilder func() io.Writer) {
	for func() (notDone bool) {
		wtr := NewAsyncWriter(writerBuilder())
		wtr.Run()
		defer wtr.Close()

		counter := 0
		for bs := range q.C {
			wtr.C <- bs
			counter++
			if counter == q.PartSize {
				return true
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
