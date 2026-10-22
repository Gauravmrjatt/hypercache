package protocol

import (
	"errors"
	"fmt"
	"strings"
)

// Parse errors returned by the text protocol.
var (
	ErrEmptyCommand      = errors.New("empty command")
	ErrUnterminatedQuote = errors.New("unterminated quoted string")
	ErrInvalidEscape     = errors.New("invalid escape sequence")
	ErrCommandTooLarge   = errors.New("command line too large")
	ErrUnknownCommand    = errors.New("unknown command")
	ErrWrongNumberOfArgs = errors.New("wrong number of arguments")
	ErrValueNotInteger   = errors.New("value is not an integer or out of range")
	ErrSyntax            = errors.New("syntax error")
	ErrOutOfMemory       = errors.New("out of memory")
	ErrUnsupportedOption = errors.New("unsupported option")
	ErrKeyNotFound       = errors.New("no such key")
)

// MaxLineBytes limits how large a single client command may be.
const MaxLineBytes = 1 << 20 // 1 MiB

// Command is a parsed client request.
type Command struct {
	// Name is the upper-cased command name, for example "SET".
	Name string
	// Args are the remaining whitespace-separated arguments.
	Args []string
}

// Parse turns one raw protocol line into a Command.
//
// Grammar: arguments are separated by spaces or tabs. Double quotes group
// arguments containing whitespace; inside quotes the escapes \" \\ \n \t
// and \r are recognised. Quotes may also appear in the middle of a token
// and concatenate with the surrounding text.
func Parse(line string) (Command, error) {
	if len(line) > MaxLineBytes {
		return Command{}, ErrCommandTooLarge
	}
	tokens, err := Tokenize(line)
	if err != nil {
		return Command{}, err
	}
	if len(tokens) == 0 {
		return Command{}, ErrEmptyCommand
	}
	return Command{Name: strings.ToUpper(tokens[0]), Args: tokens[1:]}, nil
}

// Tokenize splits a protocol line into arguments.
func Tokenize(line string) ([]string, error) {
	line = strings.TrimRight(line, "\r\n")

	var (
		tokens   []string
		cur      strings.Builder
		inQuotes bool
		started  bool
	)
	cur.Grow(16)

	for i := 0; i < len(line); i++ {
		ch := line[i]

		if inQuotes {
			switch ch {
			case '"':
				inQuotes = false
			case '\\':
				i++
				if i >= len(line) {
					return nil, ErrUnterminatedQuote
				}
				switch line[i] {
				case 'n':
					cur.WriteByte('\n')
				case 't':
					cur.WriteByte('\t')
				case 'r':
					cur.WriteByte('\r')
				case '"':
					cur.WriteByte('"')
				case '\\':
					cur.WriteByte('\\')
				default:
					return nil, fmt.Errorf("%w: \\%c", ErrInvalidEscape, line[i])
				}
			default:
				cur.WriteByte(ch)
			}
			continue
		}

		switch ch {
		case '"':
			inQuotes = true
			started = true
		case ' ', '\t', '\r', '\n':
			if started {
				tokens = append(tokens, cur.String())
				cur.Reset()
				started = false
			}
		default:
			cur.WriteByte(ch)
			started = true
		}
	}

	if inQuotes {
		return nil, ErrUnterminatedQuote
	}
	if started {
		tokens = append(tokens, cur.String())
	}
	return tokens, nil
}
