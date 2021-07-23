package asyncio

import (
	"os"
	"path"
	"strconv"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	log.SetLevel(log.TraceLevel)
}

const (
	_PROJECT_ROOT_ = "../.."
)

func TestASyncReader(t *testing.T) {
	log.Infof("testcase %s start", t.Name())
	f, err := os.Open(path.Join(_PROJECT_ROOT_, "asset/test/simple.txt"))
	if err != nil {
		t.Fatal(err)
	}
	r := NewAsyncReader(f).
		WithName("asset/test/simple.txt").
		WithDelimiter(DefaultDelimiter)
	require.NotNil(t, r)
	r.Run()
	defer r.Close()
	counter := 0
	for b := range r.C {
		counter += 1
		assert.Equal(t, strconv.Itoa(counter), string(b))
	}
	assert.Equal(t, 128, counter)
}
