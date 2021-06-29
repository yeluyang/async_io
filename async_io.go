package asyncio

import (
	"bufio"
	"io"

	log "github.com/sirupsen/logrus"
)

type AsyncWriter struct {
	name  string
	inner *bufio.Writer
	C     chan []byte
}

func NewAsyncWriter(name string, writer io.Writer) *AsyncWriter {
	w := &AsyncWriter{
		name:  name,
		inner: bufio.NewWriter(writer),
	}
	go w.run()
	return w
}

func (w *AsyncWriter) Close() {
	close(w.C)
}

func (w *AsyncWriter) run() {
	counter := 0
	for bs := range w.C {
		if n, err := w.inner.Write(bs); err != nil {
			log.Fatalf("failed to write into %s: %s", w.name, err)
		} else if n != len(bs) {
			log.Fatalf("failed to write into %s: incompelete write, bytes written(%d/%d)", w.name, n, len(bs))
		} else {
			log.Tracef("write %d bytes into %s", n, w.name)
			counter += n
		}
	}
	if err := w.inner.Flush(); err != nil {
		log.Fatalf("failed to flush bytes: %s", err)
	}
	log.Debugf("write %d total bytes into %s", counter, w.name)
}

type AsyncReader struct {
	name      string
	inner     *bufio.Reader
	delimiter byte
	exit      chan struct{}
	C         chan []byte
}

func NewAsyncReader(reader io.Reader) *AsyncReader {
	r := &AsyncReader{
		inner:     bufio.NewReader(reader),
		delimiter: DefaultDelimiter,
		exit:      make(chan struct{}),
	}
	go r.run()
	return r
}

func (r *AsyncReader) WithName(name string) *AsyncReader         { r.name = name; return r }
func (r *AsyncReader) WithDelimiter(delimiter byte) *AsyncReader { r.delimiter = delimiter; return r }

func (r *AsyncReader) Close() {
	close(r.exit)
	for range r.C {
	}
}

func (r *AsyncReader) run() {
	defer close(r.C)
	counter := 0
	for {
		select {
		case <-r.exit:
			return
		default:
			if data, err := r.inner.ReadBytes(r.delimiter); err != nil {
				if err == io.EOF {
					log.Debugf("read %d total items from %s", counter, r.name)
					return
				} else {
					log.Fatalf("failed to read from %s: %s", r.name, err)
				}
			} else {
				log.Tracef("read data={len=%d, offset=%d} from %s", len(data), counter, r.name)
				counter++
				r.C <- data[:len(data)-1] // remove `delimiter` at the end of bytes
			}
		}
	}
}
