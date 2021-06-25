package async_io

import (
	"bufio"
	"io"

	log "github.com/sirupsen/logrus"
)

type asyncWriter struct {
	name  string
	inner io.Writer
	exit  chan struct{}
}

func newAsyncWriter(name string, writer io.Writer) *asyncWriter {
	return &asyncWriter{
		name:  name,
		inner: writer,
		exit:  make(chan struct{}),
	}
}

func (w *asyncWriter) close() {
	close(w.exit)
}

func (w *asyncWriter) runWriter(ch chan []byte) {
	go func() {
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
			case bytes, ok := <-ch:
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
	}()
}

type asyncReader struct {
	name  string
	inner io.Reader
	delim byte
	exit  chan struct{}
}

func newAsyncReader(name string, reader io.Reader, delim byte) *asyncReader {
	return &asyncReader{
		name:  name,
		inner: reader,
		delim: delim,
		exit:  make(chan struct{}),
	}
}

func (r *asyncReader) close() {
	close(r.exit)
}

func (r *asyncReader) runReader() chan []byte {
	ch := make(chan []byte, 1)
	go func() {
		defer close(ch)
		counter := 0
		rdr := bufio.NewReader(r.inner)
		for {
			select {
			case <-r.exit:
				return
			default:
				if data, err := rdr.ReadBytes(r.delim); err != nil {
					if err == io.EOF {
						log.Debugf("read %d items from %s", counter, r.name)
						return
					} else {
						log.Fatalf("failed to read from %s: %s", r.name, err)
					}
				} else {
					log.Tracef("read data={len=%d, offset=%d} from %s", len(data), counter, r.name)
					counter++
					ch <- data[:len(data)-1] // remove `delimiter` at the end of bytes
				}
			}
		}
	}()
	return ch
}
