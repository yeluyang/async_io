package asyncio

type ReadQueue struct {
	rdrs []*AsyncReader
	exit chan struct{}
}

func NewReadQueue() *ReadQueue {
	return &ReadQueue{
		exit: make(chan struct{}),
	}
}

func (j *ReadQueue) Add(reader ...*AsyncReader) *ReadQueue {
	j.rdrs = append(j.rdrs, reader...)
	return j
}

func (j *ReadQueue) Close() {
	for i := range j.rdrs {
		j.rdrs[i].Close()
	}
	close(j.exit)
}

func (j *ReadQueue) Reader() chan []byte {
	ch := make(chan []byte, 1)
	go func() {
		defer close(ch)
		for i := range j.rdrs {
			func(r *AsyncReader) {
				defer r.Close()
				for {
					select {
					case <-j.exit:
						return
					case data, open := <-r.C:
						if !open {
							return
						}
						ch <- data
					}
				}
			}(j.rdrs[i])
		}
	}()
	return ch
}

type WriteQueue struct {
	path      string
	delimiter byte
	size      int
	exit      chan struct{}
}

func NewWriteQueue(path string) *WriteQueue {
	return &WriteQueue{
		path:      path,
		delimiter: DefaultDelimiter,
		size:      1024,
		exit:      make(chan struct{}),
	}
}
func (j *WriteQueue) WithDelimiter(delim byte) *WriteQueue { j.delimiter = delim; return j }
func (j *WriteQueue) WithItemSize(size int) *WriteQueue    { j.size = size; return j }

func (j *WriteQueue) BatchWrite(batch int, ch chan []byte) {
	buffer := make([]byte, batch*j.size)
	counter := 0

	put := make(chan []byte, 1)
	inner := NewAsyncWriter(j.path, false)
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

func (j *WriteQueue) Close() {
	close(j.exit)
}
