package websocket

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid"
)

var (
	entropyPool = sync.Pool{
		New: func() interface{} {
			entropy := make([]byte, 16)
			return &entropy
		},
	}
)

func generateId() string {
	entropy := entropyPool.Get().(*[]byte)
	defer entropyPool.Put(entropy)

	_, _ = rand.Read(*entropy)
	id := ulid.MustNew(ulid.Timestamp(time.Now().UTC()), &cryptoReader{buf: *entropy})
	return id.String()
}

// cryptoReader implements io.Reader for ULID
type cryptoReader struct {
	buf []byte
	pos int
}

func (r *cryptoReader) Read(p []byte) (n int, err error) {
	n = copy(p, r.buf[r.pos:])
	r.pos += n
	if r.pos >= len(r.buf) {
		r.pos = 0
		_, err = rand.Read(r.buf)
	}
	return n, err
}

func copyMap[D any](original map[string]D) map[string]D {
	if original == nil {
		return nil
	}
	copied := make(map[string]D)
	for k, v := range original {
		copied[k] = v
	}
	return copied
}
