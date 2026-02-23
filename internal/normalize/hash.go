package normalize

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// FileHash computes the hex-encoded SHA-256 of the file at path.
func FileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file for hash: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash file: %w", err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// RowHash computes a stable SHA-256 over the canonical content of a row.
// Fields are sorted by key name then concatenated with null separators.
func RowHash(fields map[string]string) []byte {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(fields[k]))
		h.Write([]byte{0})
	}
	return h.Sum(nil)
}

// RowHashFromValues computes a SHA-256 from ordered values for a simpler calling convention.
func RowHashFromValues(rowNum int64, values ...string) []byte {
	h := sha256.New()
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(rowNum))
	h.Write(buf)
	for _, v := range values {
		h.Write([]byte(strings.TrimSpace(v)))
		h.Write([]byte{0})
	}
	return h.Sum(nil)
}
