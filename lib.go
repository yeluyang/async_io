package iomaster

import (
	asyncio "iomaster/async_io"
	queueio "iomaster/queue_io"
)

type AsyncReadJob struct {
	queues map[string]*queueio.ReadQueue
	size   int
	exit   chan struct{}
}

func NewAsyncReadJob() *AsyncReadJob {
	return &AsyncReadJob{
		size: 0,
		exit: make(chan struct{}),
	}
}

func (j *AsyncReadJob) WithSize(size int) *AsyncReadJob { j.size = size; return j }

func (j *AsyncReadJob) Queue(queName string, reader ...*asyncio.AsyncReader) *AsyncReadJob {
	if _, ok := j.queues[queName]; !ok {
		j.queues[queName] = queueio.NewReadQueue()
	}
	j.queues[queName].Queue(reader...)
	return j
}

func (j *AsyncReadJob) Close() {
	close(j.exit)
}

func (j *AsyncReadJob) BatchCH(batch int) chan [][]byte {
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

func (j *AsyncReadJob) CH() chan []byte {
	ch := make(chan []byte, 1)
	go func() {
		defer close(ch)
		remain := j.size
		for {
			for i := range j.queues {
				func(q *queueio.ReadQueue) {
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

type AsyncWriteJob struct {
	queues map[string]*queueio.WriteQueue
	Batch  int
	C      chan []byte
}

func NewAsyncWriteJob() *AsyncWriteJob {
	return &AsyncWriteJob{
		Batch: 1,
		C:     make(chan []byte),
	}
}

func (j *AsyncWriteJob) WithBatch(batch int) *AsyncWriteJob { j.Batch = batch; return j }

func (j *AsyncWriteJob) Queue(queName string) *AsyncWriteJob {
	if _, ok := j.queues[queName]; !ok {
		j.queues[queName] = queueio.NewWriteQueue()
	}
	return j
}

func (j *AsyncWriteJob) Run() {
	buffer := make([][]byte, 0, j.Batch)

	for _, q := range j.queues {
		go q.Run()
		defer q.Close()
	}

	for data := range j.C {
		buffer = append(buffer, data)
		if len(buffer) >= j.Batch {
			j.broadCast(buffer)
			buffer = make([][]byte, 0, j.Batch)
		}
	}
	if len(buffer) > 0 {
		j.broadCast(buffer)
	}
}

func (j *AsyncWriteJob) broadCast(b [][]byte) {
	for i := range j.queues {
		j.queues[i].C <- b
	}
}

func (j *AsyncWriteJob) Close() {
	close(j.C)
}
