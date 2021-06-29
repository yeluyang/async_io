package asyncio

import (
	"bufio"
	"io"

	log "github.com/sirupsen/logrus"
)

type AsyncWriter struct {
	name      string
	inner     io.Writer
	delimiter byte
	C         chan []byte
	exit      chan struct{}
}

func NewAsyncWriter(name string, writer io.Writer) *AsyncWriter {
	w := &AsyncWriter{
		name:  name,
		inner: writer,
		exit:  make(chan struct{}),
	}
	go w.run()
	return w
}

func (w *AsyncWriter) close() {
	close(w.exit)
}

func (w *AsyncWriter) run() {
	wtr := bufio.NewWriter(w.inner)
	defer func() {
		if err := wtr.Flush(); err != nil {
			log.Fatalf("failed to flush bytes: %s", err)
		}
	}()
	counter := 0
	for {
		select {
		case <-w.exit:
			log.Debugf("received exit signal, already write %d bytes", counter)
			return
		case bytes, ok := <-w.C:
			if !ok {
				log.Debugf("all bytes have been wrote, total %d bytes", counter)
				return
			}
			if n, err := wtr.Write(bytes); err != nil {
				log.Fatalf("failed to write into %s: %s", w.name, err)
			} else if n != len(bytes) {
				log.Fatalf("failed to write into %s: incompelete write, bytes written(%d/%d)", w.name, n, len(bytes))
			} else {
				counter += n
				log.Tracef("write %d bytes into %s", n, w.name)
			}
		}
	}
}

func (w *AsyncWriter) write() {
}

type AsyncReader struct {
	name      string
	inner     io.Reader
	delimiter byte
	exit      chan struct{}
	C         chan []byte
}

func NewAsyncReader(reader io.Reader) *AsyncReader {
	r := &AsyncReader{
		inner:     reader,
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
	rdr := bufio.NewReader(r.inner)
	for {
		select {
		case <-r.exit:
			return
		default:
			if data, err := rdr.ReadBytes(r.delimiter); err != nil {
				if err == io.EOF {
					log.Debugf("read %d items from %s", counter, r.name)
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
