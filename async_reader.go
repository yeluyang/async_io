package asyncio

import (
	"bufio"
	"io"

	log "github.com/sirupsen/logrus"
)

type AsyncReader struct {
	Name      string
	Delimiter byte

	C chan []byte

	inner *bufio.Reader
	exit  chan struct{}
	log   log.Ext1FieldLogger
}

func NewAsyncReader(reader io.Reader) *AsyncReader {
	r := &AsyncReader{
		Delimiter: DefaultDelimiter,
		inner:     bufio.NewReader(reader),
		exit:      make(chan struct{}),
		log:       log.StandardLogger(),
	}
	return r
}

func (r *AsyncReader) WithName(name string) *AsyncReader {
	r.Name = name
	r.log = log.WithField("name", name)
	return r
}
func (r *AsyncReader) WithDelimiter(delimiter byte) *AsyncReader { r.Delimiter = delimiter; return r }

func (r *AsyncReader) Close() {
	close(r.exit)
	for range r.C {
	}
}

func (r *AsyncReader) Run() {
	defer close(r.C)
	counter := 0
	for {
		select {
		case <-r.exit:
			return
		default:
			if data, err := r.inner.ReadBytes(r.Delimiter); err != nil {
				if err == io.EOF {
					r.log.Debugf("read %d total items from %s", counter, r.Name)
					return
				} else {
					r.log.Fatalf("failed to read from %s: %s", r.Name, err)
				}
			} else {
				r.log.Tracef("read data={len=%d, offset=%d} from %s", len(data), counter, r.Name)
				counter++
				r.C <- data[:len(data)-1] // remove `delimiter` at the end of bytes
			}
		}
	}
}
