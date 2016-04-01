package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/JustinAzoff/flow-indexer/backend"
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

func stripDecimal(value string) string {
	idx := strings.Index(value, ".")
	if idx == -1 {
		return value
	}
	return value[:idx]
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

func (r *Record) GetStringByField(field string) string {
	idx, ok := (*r.fields)[field]
	if ok {
		return (*r.cols)[idx]
	}
	r.err = fmt.Errorf("Invalid field %s", field)
	return ""
}
func (r *Record) GetStringByIndexX(index int) string {
	return (*r.cols)[index]
}
func (r *Record) GetStringByIndex(index int) string {
	line := *r.line
	for i := 0; i < index; i++ {
		pos := strings.Index(line, "\t")
		if pos == -1 {
			return ""
		}
		line = line[pos+1:]
	}
	end := strings.Index(line, "\t")
	if end == -1 {
		end = len(line)
	}
	return line[:end]
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

type uniqueQuery struct {
	query  string
	answer string
	qtype  string
}

type queryStat struct {
	count uint
	first string
	last  string
	ttl   string
}

type aggregationResult struct {
	uniqueQuery
	queryStat
}

func aggregate(fn string) ([]aggregationResult, error) {
	var aggregated []aggregationResult

	queries := make(map[uniqueQuery]*queryStat)

	f, err := backend.OpenDecompress(fn)
	if err != nil {
		return aggregated, err
	}
	br := NewBroAsciiReader(f)

	var answers_field, query_field, qtype_name_field, ts_field, ttl_field int

	for {
		rec, err := br.Next()
		if err != nil {
			return aggregated, err
		}
		if rec == nil {
			break
		}

		if br.HeadersChanged() {
			ts_field = rec.GetFieldIndex("ts")
			answers_field = rec.GetFieldIndex("answers")
			query_field = rec.GetFieldIndex("query")
			qtype_name_field = rec.GetFieldIndex("qtype_name")
			ttl_field = rec.GetFieldIndex("TTLs")
			br.HandledHeaders()
			if rec.err != nil {
				return aggregated, rec.err
			}
		}

		ts := rec.GetStringByIndex(ts_field)
		query := rec.GetStringByIndex(query_field)
		qtype_name := rec.GetStringByIndex(qtype_name_field)
		answers_raw := rec.GetStringByIndex(answers_field)
		ttls_raw := rec.GetStringByIndex(ttl_field)
		if rec.err != nil {
			return aggregated, rec.err
		}
		answers := strings.Split(answers_raw, ",")
		ttls := strings.Split(ttls_raw, ",")

		for idx, answer := range answers {
			ttl := stripDecimal(ttls[idx])
			uquery := uniqueQuery{
				query:  query,
				answer: answer,
				qtype:  qtype_name,
			}
			rec := queries[uquery]
			if rec == nil {
				rec = &queryStat{
					first: ts,
					last:  ts,
					ttl:   ttl,
					count: 1,
				}
				queries[uquery] = rec
			} else {
				rec.count++
				rec.last = ts
				rec.ttl = ttl
			}
		}
	}

	for q, stat := range queries {
		agg := aggregationResult{
			uniqueQuery: q,
			queryStat:   *stat,
		}
		aggregated = append(aggregated, agg)
	}

	return aggregated, nil
}

func main() {
	fn := os.Args[1]
	aggregated, err := aggregate(fn)
	if err != nil {
		log.Fatal(err)
	}
	for _, q := range aggregated {
		fmt.Printf("%-8d %-30s %-4s %-30s %s %s %s\n", q.count, q.query, q.qtype, q.answer, q.ttl, q.first, q.last)
	}

}
