package async_io

import (
	"bufio"
	"io"
	"os"

	log "github.com/sirupsen/logrus"
)

type asyncIO struct {
	path     string
	readOnly bool
	exit     chan struct{}
}

func newAsyncIO(file string, readOnly bool) *asyncIO {
	return &asyncIO{
		path:     file,
		readOnly: readOnly,
		exit:     make(chan struct{}),
	}
}

func (f *asyncIO) open() (*os.File, error) {
	flag := os.O_RDONLY
	perm := os.FileMode(0444)
	if !f.readOnly {
		flag = os.O_APPEND | os.O_CREATE | os.O_RDWR
		perm = 0644
	}
	fd, err := os.OpenFile(f.path, flag, perm)
	if err != nil {
		return nil, err
	}
	return fd, nil
}

func (f *asyncIO) runReader(delim byte) chan []byte {
	ch := make(chan []byte, 1)
	go func() {
		defer close(ch)
		fd, err := f.open()
		if err != nil {
			log.Fatalf("failed to read from %s: %s", f.path, err)
		}
		defer fd.Close()
		r := bufio.NewReader(fd)
		counter := 0
		for {
			select {
			case <-f.exit:
				return
			default:
				if data, err := r.ReadBytes(delim); err != nil {
					if err == io.EOF {
						log.Debugf("read %d items from %s", counter, fd.Name())
						return
					} else {
						log.Fatalf("failed to read from %s: %s", fd.Name(), err)
					}
				} else {
					log.Tracef("read data={len=%d, offset=%d} from %s", len(data), counter, fd.Name())
					counter++
					ch <- data[:len(data)-1] // remove `delimiter` at the end of bytes
				}
			}
		}
	}()
	return ch
}

func (f *asyncIO) runWriter(ch chan []byte) {
	go func() {
		fd, err := f.open()
		if err != nil {
			log.Fatalf("failed to read from %s: %s", f.path, err)
		}
		defer func() {
			if err := fd.Close(); err != nil {
				log.Fatalf("failed to close file: %s", f.path)
			}
		}()
		w := bufio.NewWriter(fd)
		defer func() {
			if err := w.Flush(); err != nil {
				log.Fatalf("failed to flush bytes: %s", err)
			}
		}()
		for bytes := range ch {
			if n, err := w.Write(bytes); err != nil {
				log.Fatalf("failed to write into %s: %s", fd.Name(), err)
			} else if n != len(bytes) {
				log.Fatalf("failed to write into %s: incompelete write, bytes written(%d/%d)", fd.Name(), n, len(bytes))
			} else {
				log.Tracef("write %d bytes into %s", n, fd.Name())
			}
		}
	}()
}

func (f *asyncIO) close() {
	close(f.exit)
}
