package ioque

import (
	asyncio "iomaster/src/async_io"
)

type ReaderQueue struct {
	rdrs []*asyncio.AsyncReader
	exit chan struct{}
	C    chan []byte
}

func NewReadQueue() *ReaderQueue {
	return &ReaderQueue{
		exit: make(chan struct{}),
	}
}

func (q *ReaderQueue) Add(reader ...*asyncio.AsyncReader) *ReaderQueue {
	q.rdrs = append(q.rdrs, reader...)
	return q
}

func (q *ReaderQueue) Close() {
	close(q.exit)
	for i := range q.rdrs {
		q.rdrs[i].Close()
	}
}

func (q *ReaderQueue) Run() {
	defer close(q.C)

	for i := range q.rdrs {
		func(r *asyncio.AsyncReader) {
			go r.Run()
			defer r.Close()
			for {
				select {
				case <-q.exit:
					return
				case data, open := <-r.C:
					if !open {
						return
					}
					q.C <- data
				}
			}
		}(q.rdrs[i])
	}
}
