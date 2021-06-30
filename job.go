package asyncio

type AsyncReadJob struct {
	queues []*ReadQueue
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

func (j *AsyncReadJob) Add(q ...*ReadQueue) {
	j.queues = append(j.queues, q...)
}

func (j *AsyncReadJob) Close() {
	close(j.exit)
}

func (j *AsyncReadJob) BatchReader(batch int) chan [][]byte {
	ch := make(chan [][]byte, 1)
	go func() {
		defer close(ch)
		buffer := make([][]byte, 0, batch)
		for data := range j.Reader() {
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

func (j *AsyncReadJob) Reader() chan []byte {
	ch := make(chan []byte, 1)
	go func() {
		defer close(ch)
		remain := j.size
		for {
			for i := range j.queues {
				func(q *ReadQueue) {
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
	path      string
	delimiter byte
	size      int
	exit      chan struct{}
}

func NewAsyncWriteJob(path string) *AsyncWriteJob {
	return &AsyncWriteJob{
		path:      path,
		delimiter: DefaultDelimiter,
		size:      1024,
		exit:      make(chan struct{}),
	}
}
func (j *AsyncWriteJob) WithDelimiter(delim byte) *AsyncWriteJob { j.delimiter = delim; return j }
func (j *AsyncWriteJob) WithItemSize(size int) *AsyncWriteJob    { j.size = size; return j }

func (j *AsyncWriteJob) BatchWrite(batch int, ch chan []byte) {
	buffer := make([]byte, batch*j.size)
	counter := 0

	put := make(chan []byte, 1)
	inner := newAsyncIO(j.path, false)
	defer inner.close()
	inner.runWriter(put)

	for {
		select {
		case <-j.exit:
			goto EXIT
		case data, open := <-ch:
			if open {
				counter++
				copy(buffer[len(buffer):], data)
				buffer = append(buffer, j.delimiter)
				if counter >= batch {
					put <- buffer
					counter = 0
					buffer = make([]byte, batch*j.size)
				}
			} else {
				goto EXIT
			}
		}
	}

EXIT:
	if len(buffer) > 0 {
		put <- buffer
	}
}

func (j *AsyncWriteJob) Close() {
	close(j.exit)
}
