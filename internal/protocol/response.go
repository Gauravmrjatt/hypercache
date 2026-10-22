package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// Writer serialises replies for the MVP line protocol.
//
// Reply grammar (one reply per command, always newline terminated):
//
//	OK                     simple success
//	PONG                   PING reply
//	ERR <message>          error
//	(nil)                  missing key
//	<int>                  integer reply (bare decimal)
//	<value>                bulk value on a single line
//	*<n> followed by n lines   array reply
//
// Bulk values and array elements must not contain newline bytes in this
// protocol version; RESP support is planned for that case.
type Writer struct {
	bw *bufio.Writer
}

// NewWriter wraps w with a buffered protocol writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

// Simple writes a single-line status reply.
func (w *Writer) Simple(s string) error {
	if _, err := w.bw.WriteString(s); err != nil {
		return err
	}
	return w.bw.WriteByte('\n')
}

// OK writes "+OK" style success.
func (w *Writer) OK() error {
	return w.Simple("OK")
}

// Pong writes the PING reply.
func (w *Writer) Pong() error {
	return w.Simple("PONG")
}

// Error writes an error reply prefixed with "ERR ".
func (w *Writer) Error(msg string) error {
	return w.Simple("ERR " + msg)
}

// Errorf writes a formatted error reply.
func (w *Writer) Errorf(format string, args ...any) error {
	return w.Error(fmt.Sprintf(format, args...))
}

// Nil writes the missing-value reply.
func (w *Writer) Nil() error {
	return w.Simple("(nil)")
}

// Int writes a bare integer reply.
func (w *Writer) Int(n int64) error {
	return w.Simple(strconv.FormatInt(n, 10))
}

// Bulk writes a raw value followed by a newline.
func (w *Writer) Bulk(v []byte) error {
	if _, err := w.bw.Write(v); err != nil {
		return err
	}
	return w.bw.WriteByte('\n')
}

// Array writes "*<n>" followed by one line per element.
func (w *Writer) Array(items []string) error {
	if err := w.Simple("*" + strconv.Itoa(len(items))); err != nil {
		return err
	}
	for _, item := range items {
		if err := w.Simple(item); err != nil {
			return err
		}
	}
	return nil
}

// Flush pushes buffered bytes to the underlying connection.
func (w *Writer) Flush() error {
	return w.bw.Flush()
}
