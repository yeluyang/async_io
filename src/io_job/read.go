package iojob

import (
	asyncio "iomaster/src/async_io"
	ioque "iomaster/src/io_queue"
)

type ReadJob struct {
	queues map[string]*ioque.ReaderQueue
	size   int
	exit   chan struct{}
}

func NewAsyncReadJob() *ReadJob {
	return &ReadJob{
		size: 0,
		exit: make(chan struct{}),
	}
}

func (j *ReadJob) WithSize(size int) *ReadJob { j.size = size; return j }

func (j *ReadJob) Queue(queName string, reader ...*asyncio.AsyncReader) *ReadJob {
	if _, ok := j.queues[queName]; !ok {
		j.queues[queName] = ioque.NewReadQueue()
	}
	j.queues[queName].Queue(reader...)
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
	ch := make(chan []byte, 1)
	go func() {
		defer close(ch)
		remain := j.size
		for {
			for i := range j.queues {
				func(q *ioque.ReaderQueue) {
					go q.Run()
					defer q.Close()
					for {
						select {
						case <-j.exit:
							return
						case data, open := <-q.C:
							if !open {
								return
							}
							ch <- data
							if j.size > 0 {
								remain--
								if remain <= 0 {
									return
								}
							}
						}
					}
				}(j.queues[i])
				if j.size > 0 && remain <= 0 {
					return
				}
			}
			if remain <= 0 {
				return
			}
		}
	}()
	return ch
}
