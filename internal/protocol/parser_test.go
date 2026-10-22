package protocol

import (
	"errors"
	"testing"
)

func TestParseBasic(t *testing.T) {
	tests := []struct {
		line string
		want Command
	}{
		{"PING", Command{Name: "PING", Args: nil}},
		{"ping", Command{Name: "PING", Args: nil}},
		{"SET user:1 Gaurav", Command{Name: "SET", Args: []string{"user:1", "Gaurav"}}},
		{"GET user:1\r\n", Command{Name: "GET", Args: []string{"user:1"}}},
		{"  SET   a   b  ", Command{Name: "SET", Args: []string{"a", "b"}}},
		{"SET user:1 \"hello world\" EX 60", Command{Name: "SET", Args: []string{"user:1", "hello world", "EX", "60"}}},
		{`SET k "a\"b"`, Command{Name: "SET", Args: []string{"k", `a"b`}}},
		{`SET k "a\\b"`, Command{Name: "SET", Args: []string{"k", `a\b`}}},
		{`SET k "line\nbreak"`, Command{Name: "SET", Args: []string{"k", "line\nbreak"}}},
		{`SET k "tab\there"`, Command{Name: "SET", Args: []string{"k", "tab\there"}}},
		{`SET k ""`, Command{Name: "SET", Args: []string{"k", ""}}},
		{`SET ab"cd"ef x`, Command{Name: "SET", Args: []string{"abcdef", "x"}}},
		{"SET\tk\tv", Command{Name: "SET", Args: []string{"k", "v"}}},
	}

	for _, tt := range tests {
		got, err := Parse(tt.line)
		if err != nil {
			t.Fatalf("Parse(%q) unexpected error: %v", tt.line, err)
		}
		if got.Name != tt.want.Name {
			t.Errorf("Parse(%q) name = %q, want %q", tt.line, got.Name, tt.want.Name)
		}
		if len(got.Args) != len(tt.want.Args) {
			t.Errorf("Parse(%q) args = %q, want %q", tt.line, got.Args, tt.want.Args)
			continue
		}
		for i := range got.Args {
			if got.Args[i] != tt.want.Args[i] {
				t.Errorf("Parse(%q) args[%d] = %q, want %q", tt.line, i, got.Args[i], tt.want.Args[i])
			}
		}
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		line string
		want error
	}{
		{"", ErrEmptyCommand},
		{"   \t ", ErrEmptyCommand},
		{`SET k "unterminated`, ErrUnterminatedQuote},
		{`SET k "bad\q"`, ErrInvalidEscape},
		{`SET k "trailing\`, ErrUnterminatedQuote},
	}

	for _, tt := range tests {
		_, err := Parse(tt.line)
		if !errors.Is(err, tt.want) {
			t.Errorf("Parse(%q) error = %v, want %v", tt.line, err, tt.want)
		}
	}
}

func TestParseRejectsOversizedLine(t *testing.T) {
	line := "GET " + string(make([]byte, MaxLineBytes))
	if _, err := Parse(line); !errors.Is(err, ErrCommandTooLarge) {
		t.Fatalf("expected ErrCommandTooLarge, got %v", err)
	}
}

func TestTokenizeKeepsEmptyTokenDistinct(t *testing.T) {
	tokens, err := Tokenize(`SET k ""`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tokens) != 3 || tokens[2] != "" {
		t.Fatalf("tokens = %#v, want three entries with last empty", tokens)
	}
}
