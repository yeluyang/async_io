package queueio

import (
	asyncio "iomaster/async_io"
)

type ReadQueue struct {
	rdrs [][]*asyncio.AsyncReader
	exit chan struct{}
	C    chan []byte
}

func NewReadQueue() *ReadQueue {
	return &ReadQueue{
		exit: make(chan struct{}),
	}
}

func (q *ReadQueue) Queue(reader ...*asyncio.AsyncReader) *ReadQueue {
	q.rdrs = append(q.rdrs, reader)
	return q
}

func (q *ReadQueue) Close() {
	close(q.exit)
	for i := range q.rdrs {
		for j := range q.rdrs[i] {
			q.rdrs[i][j].Close()
		}
	}
}

func (q *ReadQueue) Run() {
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
