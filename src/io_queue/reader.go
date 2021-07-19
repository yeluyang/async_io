package ioque

import (
	asyncio "iomaster/src/async_io"
)

type ReaderQueue struct {
	rdrs [][]*asyncio.AsyncReader
	exit chan struct{}
	C    chan []byte
}

func NewReadQueue() *ReaderQueue {
	return &ReaderQueue{
		exit: make(chan struct{}),
	}
}

func (q *ReaderQueue) Queue(reader ...*asyncio.AsyncReader) *ReaderQueue {
	q.rdrs = append(q.rdrs, reader)
	return q
}

func (q *ReaderQueue) Close() {
	close(q.exit)
	for i := range q.rdrs {
		for j := range q.rdrs[i] {
			q.rdrs[i][j].Close()
		}
	}
}

func (q *ReaderQueue) Run() {
	defer close(q.C)
	for i := range q.rdrs {
		go func(rq []*asyncio.AsyncReader) {
			for i := range rq {
				go rq[i].Run()
				defer rq[i].Close()
				for {
					select {
					case <-q.exit:
						return
					case data, open := <-rq[i].C:
						if !open {
							return
						}
						q.C <- data
					}
				}
			}
		}(q.rdrs[i])
	}
}
