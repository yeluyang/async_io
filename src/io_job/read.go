package iojob

import (
	asyncio "iomaster/src/async_io"
	ioque "iomaster/src/io_queue"
)

type ReadJob struct {
	queues map[string]*ioque.ReaderQueue
	size   int
	ch     chan []byte
	exit   chan struct{}
}

func NewAsyncReadJob() *ReadJob {
	return &ReadJob{
		size: 0,
		exit: make(chan struct{}),
	}
}

func (j *ReadJob) WithSize(size int) *ReadJob { j.size = size; return j }

func (j *ReadJob) Add(key string, reader ...*asyncio.AsyncReader) *ReadJob {
	if _, ok := j.queues[key]; !ok {
		j.queues[key] = ioque.NewReadQueue().Add(reader...)
	} else {
		j.queues[key].Add(reader...)
	}
	return j
}

func (j *ReadJob) Close() {
	close(j.exit)
}

func (j *ReadJob) BatchCH(batch int) chan [][]byte {
	ch := make(chan [][]byte, 1)
	go func() {
		defer close(ch)
		buffer := make([][]byte, 0, batch)
		for data := range j.CH() {
			buffer = append(buffer, data)
			if len(buffer) >= batch {
				ch <- buffer
				buffer = make([][]byte, 0, batch)
			}
		}
		if len(buffer) > 0 {
			ch <- buffer
		}
	}()
	return ch
}

func (j *ReadJob) CH() chan []byte {
	j.runAllQueue()
	ch := make(chan []byte, 1)
	go func() {
		defer close(ch)
		remain := j.size
		for b := range j.ch {
			ch <- b
			if j.size > 0 {
				remain--
				if remain <= 0 {
					return
				}
			}
		}
	}()
	return ch
}

func (j *ReadJob) runAllQueue() {
	for i := range j.queues {
		go func(q *ioque.ReaderQueue) {
			go q.Run()
			for {
				select {
				case <-j.exit:
					return
				case b, ok := <-q.C:
					if !ok {
						return
					}
					j.ch <- b
				}
			}
		}(j.queues[i])
	}
}
