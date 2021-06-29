package asyncio

import (
	"bufio"
	"io"

	log "github.com/sirupsen/logrus"
)

type AsyncWriter struct {
	Name string

	C chan []byte

	inner *bufio.Writer
	log   log.Ext1FieldLogger
}

func NewAsyncWriter(writer io.Writer) *AsyncWriter {
	w := &AsyncWriter{
		inner: bufio.NewWriter(writer),
		log:   log.StandardLogger(),
	}
	return w
}

func (w *AsyncWriter) WithName(name string) *AsyncWriter {
	w.Name = name
	w.log = log.WithField("name", name)
	return w
}

func (w *AsyncWriter) Close() {
	close(w.C)
}

func (w *AsyncWriter) Run() {
	counter := 0
	for bs := range w.C {
		if n, err := w.inner.Write(bs); err != nil {
			w.log.Fatalf("failed to write into %s: %s", w.Name, err)
		} else if n != len(bs) {
			w.log.Fatalf("failed to write into %s: incompelete write, bytes written(%d/%d)", w.Name, n, len(bs))
		} else {
			w.log.Tracef("write %d bytes into %s", n, w.Name)
			counter += n
		}
	}
	if err := w.inner.Flush(); err != nil {
		w.log.Fatalf("failed to flush bytes: %s", err)
	}
	w.log.Debugf("write %d total bytes into %s", counter, w.Name)
}
