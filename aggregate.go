package main

import (
	"strings"
	"time"

	"github.com/JustinAzoff/flow-indexer/backend"
)

func stripDecimal(value string) string {
	if value == "-" {
		return "0"
	}
	idx := strings.Index(value, ".")
	if idx == -1 {
		return value
	}
	return value[:idx]
}

type DNSRecord struct {
	ts      float64
	query   string
	qtype   string
	answers []string
	ttls    []string
}

type uniqueTuple struct {
	query  string
	answer string
	qtype  string
}
type uniqueIndividual struct {
	value string
	which string // "Q" or "A"
}

type queryStat struct {
	count uint
	first float64
	last  float64
	ttl   string
}

type aggregationResult struct {
	Duration     time.Duration
	TotalRecords uint
	Tuples       []aggregatedTuple
	Individual   []aggregatedIndividual
}

type aggregatedTuple struct {
	uniqueTuple
	queryStat
}
type aggregatedIndividual struct {
	uniqueIndividual
	queryStat
}

type DNSAggregator struct {
	queries      map[uniqueTuple]*queryStat
	values       map[uniqueIndividual]*queryStat
	totalRecords uint
	start        time.Time
}

func NewDNSAggregator() *DNSAggregator {
	queries := make(map[uniqueTuple]*queryStat)
	values := make(map[uniqueIndividual]*queryStat)
	return &DNSAggregator{
		queries: queries,
		values:  values,
		start:   time.Now(),
	}
}

func (d *DNSAggregator) AddRecord(r DNSRecord) {
	d.totalRecords++
	query_value := uniqueIndividual{value: r.query, which: "Q"}

	arec := d.values[query_value]
	if arec == nil {
		arec = &queryStat{
			first: r.ts,
			last:  r.ts,
			count: 1,
		}
		d.values[query_value] = arec
	} else {
		arec.count++
		arec.last = r.ts
	}

	for idx, answer := range r.answers {
		ttl := stripDecimal(r.ttls[idx])
		uquery := uniqueTuple{
			query:  r.query,
			answer: answer,
			qtype:  r.qtype,
		}
		rec := d.queries[uquery]
		if rec == nil {
			rec = &queryStat{
				first: r.ts,
				last:  r.ts,
				ttl:   ttl,
				count: 1,
			}
			d.queries[uquery] = rec
		} else {
			rec.count++
			rec.last = r.ts
			rec.ttl = ttl
		}

		answer_value := uniqueIndividual{value: answer, which: "A"}
		arec := d.values[answer_value]
		if arec == nil {
			arec = &queryStat{
				first: r.ts,
				last:  r.ts,
				ttl:   ttl,
				count: 1,
			}
			d.values[answer_value] = arec
		} else {
			arec.count++
			arec.last = r.ts
			arec.ttl = ttl
		}
	}

}

func (d *DNSAggregator) GetResult() aggregationResult {
	var result aggregationResult
	for q, stat := range d.queries {
		agg := aggregatedTuple{
			uniqueTuple: q,
			queryStat:   *stat,
		}
		result.Tuples = append(result.Tuples, agg)
	}
	for value, stat := range d.values {
		agg := aggregatedIndividual{
			uniqueIndividual: value,
			queryStat:        *stat,
		}
		result.Individual = append(result.Individual, agg)
	}
	result.TotalRecords = d.totalRecords
	result.Duration = time.Since(d.start)
	return result

}

func (d *DNSAggregator) Merge(other *DNSAggregator) {
	for q, stat := range other.queries {
		rec := d.queries[q]
		if rec == nil {
			d.queries[q] = stat
		} else {
			rec.count += stat.count
			if stat.first < rec.first {
				rec.first = stat.first
			}
			if stat.last > rec.last {
				rec.last = stat.last
			}
			rec.ttl = stat.ttl
		}
	}
	for q, stat := range other.values {
		rec := d.values[q]
		if rec == nil {
			d.values[q] = stat
		} else {
			rec.count += stat.count
			if stat.first < rec.first {
				rec.first = stat.first
			}
			if stat.last > rec.last {
				rec.last = stat.last
			}
			rec.ttl = stat.ttl
		}
	}
	return
	return
}

func aggregate(aggregator *DNSAggregator, fn string) error {
	f, err := backend.OpenDecompress(fn)
	if err != nil {
		return err
	}
	br := NewBroAsciiReader(f)

	var answers_field, query_field, qtype_name_field, ts_field, ttl_field int

	for {
		rec, err := br.Next()
		if err != nil {
			return err
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
				return err
			}
		}

		ts := rec.GetFloatByIndex(ts_field)
		query := rec.GetStringByIndex(query_field)
		qtype_name := rec.GetStringByIndex(qtype_name_field)
		answers_raw := rec.GetStringByIndex(answers_field)
		ttls_raw := rec.GetStringByIndex(ttl_field)
		if rec.err != nil {
			return err
		}
		answers := strings.Split(answers_raw, ",")
		ttls := strings.Split(ttls_raw, ",")
		dns_record := DNSRecord{
			ts:      ts,
			query:   query,
			qtype:   qtype_name,
			answers: answers,
			ttls:    ttls,
		}
		aggregator.AddRecord(dns_record)
	}

	return nil
}
