package asyncio

import (
	"bufio"
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
)

type AsyncWriter struct {
	Name      string
	Delimiter []byte

	C chan [][]byte

	inner *bufio.Writer
	log   log.Ext1FieldLogger
}

func NewAsyncWriter(writer io.Writer) *AsyncWriter {
	w := &AsyncWriter{
		Delimiter: []byte{DefaultDelimiter},

		C: make(chan [][]byte, DefaultCHBuffer),

		inner: bufio.NewWriterSize(writer, DefaultWriterSize),
		log:   log.StandardLogger(),
	}
	return w
}

func (w *AsyncWriter) WithDelimiter(delimiter byte) *AsyncWriter {
	w.Delimiter = []byte{delimiter}
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
	for batch := range w.C {
		for i := range batch {
			// XXX how to optimize?
			if n, err := w.write(batch[i]); err != nil {
				w.log.Fatalf("failed to write: %s", err)
			} else {
				counter += n
			}
		}
	}
	if err := w.inner.Flush(); err != nil {
		w.log.Fatalf("failed to flush bytes: %s", err)
	}
	w.log.Debugf("write %d total bytes", counter)
}

func (w *AsyncWriter) write(b []byte) (int, error) {
	if n, err := w.inner.Write(b); err != nil {
		return n, err
	} else if n != len(b) {
		return n, fmt.Errorf("incompelete payload, bytes written(%d/%d)", n, len(b))
	} else {
		w.log.Debugf("wrote %d bytes payload", n)
		// w.log.Tracef("wrote payload: %v", string(b))
		if dn, err := w.inner.Write(w.Delimiter); err != nil {
			return n + dn, fmt.Errorf("failed to write delimiter: %s", err)
		} else if dn != len(w.Delimiter) {
			return n + dn, fmt.Errorf("incompelete delimiter, bytes written(%d/%d)", dn, len(w.Delimiter))
		} else {
			return n + dn, nil
		}
	}
}
