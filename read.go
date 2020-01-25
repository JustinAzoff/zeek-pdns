package main

import (
	"bufio"
	"fmt"
	"io"
)

type Reader interface {
	Next() (Record, error)
}

type Record interface {
	String() string
	GetString(string) string
	GetTimestamp(string) string
	GetStringList(string) []string
	GetFloat(string) float64
	Error() error
	IsMissingFieldError() bool
}

func NewBroReader(r io.Reader) (Reader, error) {
	wrapped := bufio.NewReader(r)
	first_byte, err := wrapped.Peek(1)
	if err != nil {
		return nil, err
	}
	switch first_byte[0] {
	case '#':
		return NewBroAsciiReader(wrapped), nil
	case '{':
		return NewBroJSONReader(wrapped), nil
	default:
		return nil, fmt.Errorf("Unable to determine file type, first byte was %q", first_byte)
	}
}
