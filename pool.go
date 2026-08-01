package strand

import (
	"bytes"
	"encoding/json"
	"sync"
)

// Global sync.Pool for byte buffers to minimize heap allocation churn and GC pauses.
var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func getBuf() *bytes.Buffer {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putBuf(buf *bytes.Buffer) {
	if buf.Cap() > 64*1024 {
		return
	}
	bufPool.Put(buf)
}

func marshalPooled(v any) ([]byte, error) {
	return json.Marshal(v)
}

func cloneStatePooled[S any](state S) S {
	var cloned S
	bytes, err := json.Marshal(state)
	if err != nil {
		return state
	}
	_ = json.Unmarshal(bytes, &cloned)
	return cloned
}
