package checkpoint

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

type State struct {
	LastURL   string    `json:"last_url"`
	Pages     int       `json:"pages"`
	Rows      int       `json:"rows"`
	Skipped   int       `json:"skipped,omitempty"`
	Errors    int       `json:"errors,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type FileCheckpoint struct {
	Path string
}

func NewFileCheckpoint(path string) *FileCheckpoint {
	return &FileCheckpoint{Path: path}
}

// Load returns (state, ok, err). ok=false means checkpoint file doesn't exist.
func (c *FileCheckpoint) Load() (State, bool, error) {
	var st State
	b, err := os.ReadFile(c.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return st, false, nil
		}
		return st, false, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return State{}, true, err
	}
	return st, true, nil
}

func (c *FileCheckpoint) Save(st State) error {
	st.UpdatedAt = time.Now().UTC()
	if err := os.MkdirAll(filepath.Dir(c.Path), 0o755); err != nil && filepath.Dir(c.Path) != "." {
		return err
	}
	tmp := c.Path + ".tmp"
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.Path)
}
