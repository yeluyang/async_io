package iomaster

import (
	queueio "iomaster/pkg/queue_io"
)

type WriteJob struct {
	queues map[string]*queueio.WriterQueue
	Batch  int
	C      chan []byte
}

func NewAsyncWriteJob() *WriteJob {
	return &WriteJob{
		Batch: 1,
		C:     make(chan []byte),
	}
}

func (j *WriteJob) WithBatch(batch int) *WriteJob { j.Batch = batch; return j }

func (j *WriteJob) Queue(queName string) *WriteJob {
	if _, ok := j.queues[queName]; !ok {
		j.queues[queName] = queueio.NewWriteQueue()
	}
	return j
}

func (j *WriteJob) Run() {
	buffer := make([][]byte, 0, j.Batch)

	for _, q := range j.queues {
		go q.Run()
		defer q.Close()
	}

	for data := range j.C {
		buffer = append(buffer, data)
		if len(buffer) >= j.Batch {
			j.broadCast(buffer)
			buffer = make([][]byte, 0, j.Batch)
		}
	}
	if len(buffer) > 0 {
		j.broadCast(buffer)
	}
}

func (j *WriteJob) broadCast(b [][]byte) {
	for i := range j.queues {
		j.queues[i].C <- b
	}
}

func (j *WriteJob) Close() {
	close(j.C)
}
