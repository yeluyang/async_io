package async_io

import "fmt"

type AsyncReadJob struct {
	path      string
	batch     int
	size      int
	delimiter byte
	exit      chan struct{}
}

func NewAsyncReadJob(path string) *AsyncReadJob {
	return &AsyncReadJob{
		path:      path,
		batch:     1,
		size:      0,
		delimiter: DefaultDelimiter,
		exit:      make(chan struct{}),
	}
}
func (j *AsyncReadJob) WithDelimiter(delim byte) *AsyncReadJob { j.delimiter = delim; return j }
func (j *AsyncReadJob) WithSize(size int) *AsyncReadJob        { j.size = size; return j }
func (j *AsyncReadJob) WithBatch(batch int) *AsyncReadJob      { j.batch = batch; return j }

func (j *AsyncReadJob) Run() chan [][]byte {
	ch := make(chan [][]byte, 1)
	go func() {
		defer close(ch)
		if j.size <= 0 {
			j.ReadOnce(0, ch)
		} else {
			remain := j.size
			for remain != 0 {
				count := j.ReadOnce(remain, ch)
				remain -= count
				if remain < 0 {
					panic(fmt.Sprintf("size remain underflow: size=%d, read=%d, remain=%d", j.size, count, remain))
				}
			}
		}
	}()
	return ch
}

func (j *AsyncReadJob) Close() {
	close(j.exit)
}

func (j *AsyncReadJob) ReadOnce(size int, ch chan [][]byte) int {
	buffer := make([][]byte, 0, j.batch)
	inner := newAsyncIO(j.path, true)
	fileCH := inner.runReader(j.delimiter)
	count := 0
	defer func() {
		inner.close()
		<-fileCH
	}()
	for {
		select {
		case <-j.exit:
			return count
		case data, open := <-fileCH:
			if open {
				buffer = append(buffer, data)
				count++
				if size > 0 && count >= size {
					if count > size {
						panic(fmt.Sprintf("read(%d) over limit(%d)", count, size))
					}
					ch <- buffer
					return count
				} else if len(buffer) >= j.batch {
					ch <- buffer
					buffer = make([][]byte, 0, j.batch)
				}
			} else {
				if len(buffer) > 0 {
					ch <- buffer
				}
				return count
			}
		}
	}
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
