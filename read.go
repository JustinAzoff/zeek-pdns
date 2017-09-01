package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
)

func grab_value(line string) string {
	val := strings.Split(line, " ")[1]
	return val
}

func extract_sep(line string) string {
	sep := grab_value(line)
	sepchar, err := hex.DecodeString(sep[2:])
	if err != nil {
		log.Panic(err)
	}
	return string(sepchar)
}

type BroAsciiReader struct {
	r          io.Reader
	br         *bufio.Reader
	sep        string
	fields     []string
	fieldsMap  map[string]int
	types      []string
	timeFields map[int]bool

	newHeaders bool
}

type Record struct {
	line   *string
	cols   *[]string
	fields *map[string]int
	err    error
}

func (r *Record) GetString(field string) string {
	idx, ok := (*r.fields)[field]
	if !ok {
		r.err = fmt.Errorf("Invalid field %s", field)
		return ""
	}
	return (*r.cols)[idx]
}
func (r *Record) GetStringByIndex(index int) string {
	return (*r.cols)[index]
}
func (r *Record) GetFloat(field string) float64 {
	idx, ok := (*r.fields)[field]
	if !ok {
		r.err = fmt.Errorf("Invalid field %s", field)
		return 0.0
	}
	val := (*r.cols)[idx]
	fl, err := strconv.ParseFloat(val, 64)
	if err != nil {
		panic(err)
	}
	return fl
}
func (r *Record) GetFloatByIndex(index int) float64 {
	val := (*r.cols)[index]
	fl, err := strconv.ParseFloat(val, 64)
	if err != nil {
		panic(err)
	}
	return fl
}

func (r *Record) GetFieldIndex(field string) int {
	idx, ok := (*r.fields)[field]
	if ok {
		return idx
	}
	r.err = fmt.Errorf("Invalid field %s", field)
	return -1
}

func NewBroAsciiReader(r io.Reader) *BroAsciiReader {
	br := bufio.NewReader(r)
	tf := make(map[int]bool)
	return &BroAsciiReader{r: r, br: br, timeFields: tf}
}

func (b *BroAsciiReader) Next() (*Record, error) {
	line, err := b.br.ReadString('\n')
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	line = strings.Trim(line, "\n")
	if strings.HasPrefix(line, "#") {
		b.handleHeader(line)
		return b.Next()
	}
	parts := strings.Split(line, "\t")
	rec := Record{
		line:   &line,
		cols:   &parts,
		fields: &b.fieldsMap,
	}
	return &rec, nil
}

func (b *BroAsciiReader) handleHeader(line string) error {
	b.newHeaders = true
	if strings.HasPrefix(line, "#separator") {
		b.sep = extract_sep(line)
	} else if strings.HasPrefix(line, "#fields") {
		b.fields = strings.Split(line, "\t")[1:]
		b.fieldsMap = make(map[string]int)
		for idx, f := range b.fields {
			b.fieldsMap[f] = idx
		}
	} else if strings.HasPrefix(line, "#types") {
		b.types = strings.Split(line, "\t")[1:]
		for idx, typ := range b.types {
			if typ == "time" {
				b.timeFields[idx] = true
			}
		}
	}
	return nil
}
func (b *BroAsciiReader) HeadersChanged() bool {
	return b.newHeaders
}
func (b *BroAsciiReader) HandledHeaders() {
	b.newHeaders = false
}
