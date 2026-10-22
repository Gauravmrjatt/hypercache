package protocol

import (
	"bytes"
	"strings"
	"testing"
)

func TestWriterReplies(t *testing.T) {
	tests := []struct {
		name string
		call func(w *Writer) error
		want string
	}{
		{"ok", func(w *Writer) error { return w.OK() }, "OK\n"},
		{"pong", func(w *Writer) error { return w.Pong() }, "PONG\n"},
		{"nil", func(w *Writer) error { return w.Nil() }, "(nil)\n"},
		{"int", func(w *Writer) error { return w.Int(-2) }, "-2\n"},
		{"error", func(w *Writer) error { return w.Error("boom") }, "ERR boom\n"},
		{"errorf", func(w *Writer) error { return w.Errorf("unknown command '%s'", "x") }, "ERR unknown command 'x'\n"},
		{"bulk", func(w *Writer) error { return w.Bulk([]byte("hello world")) }, "hello world\n"},
		{"bulk empty", func(w *Writer) error { return w.Bulk(nil) }, "\n"},
		{"array", func(w *Writer) error { return w.Array([]string{"a", "b"}) }, "*2\na\nb\n"},
		{"array empty", func(w *Writer) error { return w.Array(nil) }, "*0\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := tt.call(w); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("reply = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWriterMultipleRepliesAreNewlineTerminated(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.OK(); err != nil {
		t.Fatal(err)
	}
	if err := w.Int(42); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); !strings.HasSuffix(got, "\n") || got != "OK\n42\n" {
		t.Errorf("reply stream = %q", got)
	}
}
