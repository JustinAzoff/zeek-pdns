package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
)

type BroJSONReader struct {
	r *bufio.Reader
}

type JSONRecord struct {
	line []byte
	err  error
}

func (r *JSONRecord) String() string {
	return strings.Trim(string(r.line), "\n")
}

func (r *JSONRecord) GetString(field string) string {
	val, err := jsonparser.GetString(r.line, field)
	r.err = err
	return val
}
func (r *JSONRecord) GetStringList(field string) []string {
	var strings []string
	jsonparser.ArrayEach(r.line, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		strings = append(strings, string(value))
		r.err = err
	}, field)
	return strings
}
func (r *JSONRecord) GetFloat(field string) float64 {
	val, err := jsonparser.GetFloat(r.line, field)
	r.err = err
	return val
}

func (r *JSONRecord) IsMissingFieldError() bool {
	return r.err == jsonparser.KeyPathNotFoundError
}

func (r *JSONRecord) Error() error {
	if r.err != nil {
		return errors.Wrap(r.err, fmt.Sprintf("Error parsing %s", r))
	}
	return nil
}

func NewBroJSONReader(r *bufio.Reader) *BroJSONReader {
	return &BroJSONReader{r: r}
}

func (b *BroJSONReader) Next() (Record, error) {
	line, err := b.r.ReadBytes('\n')
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rec := JSONRecord{
		line: line,
	}
	return &rec, nil
}
