package asyncio

type ReadQueue struct {
	rdrs [][]*AsyncReader
	exit chan struct{}
}

func NewReadQueue() *ReadQueue {
	return &ReadQueue{
		exit: make(chan struct{}),
	}
}

func (q *ReadQueue) Queue(reader ...*AsyncReader) *ReadQueue {
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

func (q *ReadQueue) BatchReaderCH(batch int) chan [][]byte {
	ch := make(chan [][]byte, 1)
	go func() {
		defer close(ch)
		buffer := make([][]byte, 0, batch)
		for data := range q.ReaderCH() {
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

func (q *ReadQueue) ReaderCH() chan []byte {
	ch := make(chan []byte)
	go func() {
		defer close(ch)
		for i := range q.rdrs {
			go func(rq []*AsyncReader) {
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
							ch <- data
						}
					}
				}
			}(q.rdrs[i])
		}
	}()
	return ch
}
