package output

import (
	"encoding/json"
	"io"
	"sync"
)

type JSONLWriter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func NewJSONLWriter(w io.Writer) *JSONLWriter {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return &JSONLWriter{enc: enc}
}

func (j *JSONLWriter) Write(v any) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.enc.Encode(v)
}

func (j *JSONLWriter) Close() error {
	return nil
}
