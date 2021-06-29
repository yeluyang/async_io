package asyncio

type ReadQueue struct {
	rdrs []*AsyncReader
	exit chan struct{}
	C    chan []byte
}

func NewReadQueue() *ReadQueue {
	return &ReadQueue{
		C: make(chan []byte, DefaultCHBuffer),

		exit: make(chan struct{}),
	}
}

func (q *ReadQueue) Add(reader ...*AsyncReader) *ReadQueue {
	q.rdrs = append(q.rdrs, reader...)
	return q
}

func (q *ReadQueue) Close() {
	close(q.exit)
	for i := range q.rdrs {
		q.rdrs[i].Close()
	}
}

func (q *ReadQueue) Run() {
	go func() {
		defer close(q.C)
		for i := range q.rdrs {
			func(r *AsyncReader) {
				go r.Run()
				defer r.Close()
				for {
					select {
					case <-q.exit:
						return
					case data, open := <-r.C:
						if !open {
							return
						}
						q.C <- data
					}
				}
			}(q.rdrs[i])
		}
	}()
}
