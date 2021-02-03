package async_io

type AsyncReadJob struct {
	paths     []string
	batch     int
	size      int
	delimiter byte
	exit      chan struct{}
}

func NewAsyncReadJob(paths ...string) *AsyncReadJob {
	return &AsyncReadJob{
		paths:     paths,
		batch:     1,
		size:      0,
		delimiter: DefaultDelimiter,
		exit:      make(chan struct{}),
	}
}
func (j *AsyncReadJob) WithDelimiter(delim byte) *AsyncReadJob { j.delimiter = delim; return j }
func (j *AsyncReadJob) WithSize(size int) *AsyncReadJob        { j.size = size; return j }
func (j *AsyncReadJob) WithBatch(batch int) *AsyncReadJob      { j.batch = batch; return j }

func (j *AsyncReadJob) Close() {
	close(j.exit)
}

func (j *AsyncReadJob) Run() chan [][]byte {
	ch := make(chan [][]byte, 1)
	go func() {
		defer close(ch)
		buffer := make([][]byte, 0, j.batch)
		for data := range j.read() {
			buffer = append(buffer, data)
			if len(buffer) >= j.batch {
				ch <- buffer
				buffer = make([][]byte, 0, j.batch)
			}
		}
		if len(buffer) > 0 {
			ch <- buffer
		}
	}()
	return ch
}

func (j *AsyncReadJob) RunWithFunc(fn func([]byte) interface{}) chan []interface{} {
	ch := make(chan []interface{}, 1)
	go func() {
		defer close(ch)
		buffer := make([]interface{}, 0, j.batch)
		for data := range j.read() {
			v := fn(data)
			buffer = append(buffer, v)
			if len(buffer) >= j.batch {
				ch <- buffer
				buffer = make([]interface{}, 0, j.batch)
			}
		}
		if len(buffer) > 0 {
			ch <- buffer
		}
	}()
	return ch
}

func (j *AsyncReadJob) read() chan []byte {
	ch := make(chan []byte, 1)
	go func() {
		defer close(ch)
		remain := j.size
		for {
			for i := range j.paths {
				func(file string) {
					inner := newAsyncIO(file, true)
					fileCH := inner.runReader(j.delimiter)
					defer func() {
						inner.close()
						for range fileCH {
						}
					}()
					for {
						select {
						case <-j.exit:
							return
						case data, open := <-fileCH:
							if !open {
								return
							}
							ch <- data
							if j.size > 0 {
								remain--
								if remain == 0 {
									return
								}
							}
						}
					}
				}(j.paths[i])
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
	batch     int
	delimiter byte
	size      int
	exit      chan struct{}
}

func NewAsyncWriteJob(path string) *AsyncWriteJob {
	return &AsyncWriteJob{
		path:      path,
		batch:     1,
		delimiter: DefaultDelimiter,
		size:      1024,
		exit:      make(chan struct{}),
	}
}
func (j *AsyncWriteJob) WithDelimiter(delim byte) *AsyncWriteJob { j.delimiter = delim; return j }
func (j *AsyncWriteJob) WithBatch(batch int) *AsyncWriteJob      { j.batch = batch; return j }
func (j *AsyncWriteJob) WithItemSize(size int) *AsyncWriteJob    { j.size = size; return j }

func (j *AsyncWriteJob) Run(ch chan []byte) {
	buffer := make([]byte, j.batch*j.size)
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
				if counter >= j.batch {
					put <- buffer
					counter = 0
					buffer = make([]byte, j.batch*j.size)
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
