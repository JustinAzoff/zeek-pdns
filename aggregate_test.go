package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"testing"
)

type ByValue []aggregatedIndividual

func (a ByValue) Len() int           { return len(a) }
func (a ByValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByValue) Less(i, j int) bool { return a[i].value < a[j].value }

type ByTuple []aggregatedTuple

func (a ByTuple) Len() int           { return len(a) }
func (a ByTuple) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTuple) Less(i, j int) bool { return a[i].query+a[i].answer < a[j].query+a[j].answer }

func ExampleAggregate() {
	ag := NewDNSAggregator()

	ag.AddRecord(DNSRecord{
		ts:      "10",
		query:   "www.example.com",
		qtype:   "A",
		answers: []string{"1.2.3.4"},
		ttls:    []string{"300"},
	})
	ag.AddRecord(DNSRecord{
		ts:      "20",
		query:   "www.example.com",
		qtype:   "A",
		answers: []string{"1.2.3.4"},
		ttls:    []string{"300"},
	})

	res := ag.GetResult()
	sort.Sort(ByTuple(res.Tuples))
	sort.Sort(ByValue(res.Individual))

	fmt.Printf("Tuples:\n")
	for _, r := range res.Tuples {
		fmt.Printf("%#v\n", r)
	}
	fmt.Printf("\nIndividual:\n")
	for _, r := range res.Individual {
		fmt.Printf("%#v\n", r)
	}
	// Output:
	//Tuples:
	//main.aggregatedTuple{uniqueTuple:main.uniqueTuple{query:"www.example.com", answer:"1.2.3.4", qtype:"A"}, queryStat:main.queryStat{count:0x2, first:"10", last:"20", ttl:"300"}}
	//
	//Individual:
	//main.aggregatedIndividual{uniqueIndividual:main.uniqueIndividual{value:"1.2.3.4", which:"A"}, queryStat:main.queryStat{count:0x2, first:"10", last:"20", ttl:"300"}}
	//main.aggregatedIndividual{uniqueIndividual:main.uniqueIndividual{value:"www.example.com", which:"Q"}, queryStat:main.queryStat{count:0x2, first:"10", last:"20", ttl:""}}
}

func ExampleAggregateMerge() {
	ag := NewDNSAggregator()

	ag.AddRecord(DNSRecord{
		ts:      "10",
		query:   "www.example.com",
		qtype:   "A",
		answers: []string{"1.2.3.4"},
		ttls:    []string{"300"},
	})
	ag.AddRecord(DNSRecord{
		ts:      "200",
		query:   "www.example.com",
		qtype:   "A",
		answers: []string{"1.2.3.4"},
		ttls:    []string{"300"},
	})
	ag2 := NewDNSAggregator()
	ag2.AddRecord(DNSRecord{
		ts:      "30",
		query:   "www.example.com",
		qtype:   "A",
		answers: []string{"1.2.3.4"},
		ttls:    []string{"300"},
	})
	ag2.AddRecord(DNSRecord{
		ts:      "30",
		query:   "www.example.com",
		qtype:   "A",
		answers: []string{"1.2.3.5"},
		ttls:    []string{"300"},
	})
	ag2.AddRecord(DNSRecord{
		ts:      "40",
		query:   "www.example.com",
		qtype:   "A",
		answers: []string{"1.2.3.5"},
		ttls:    []string{"300"},
	})

	ag.Merge(ag2)

	res := ag.GetResult()
	sort.Sort(ByTuple(res.Tuples))
	sort.Sort(ByValue(res.Individual))

	fmt.Printf("Tuples:\n")
	for _, r := range res.Tuples {
		fmt.Printf("%#v\n", r)
	}
	fmt.Printf("\nIndividual:\n")
	for _, r := range res.Individual {
		fmt.Printf("%#v\n", r)
	}
	// Output:
	//Tuples:
	//main.aggregatedTuple{uniqueTuple:main.uniqueTuple{query:"www.example.com", answer:"1.2.3.4", qtype:"A"}, queryStat:main.queryStat{count:0x3, first:"10", last:"200", ttl:"300"}}
	//main.aggregatedTuple{uniqueTuple:main.uniqueTuple{query:"www.example.com", answer:"1.2.3.5", qtype:"A"}, queryStat:main.queryStat{count:0x2, first:"30", last:"40", ttl:"300"}}
	//
	//Individual:
	//main.aggregatedIndividual{uniqueIndividual:main.uniqueIndividual{value:"1.2.3.4", which:"A"}, queryStat:main.queryStat{count:0x3, first:"10", last:"200", ttl:"300"}}
	//main.aggregatedIndividual{uniqueIndividual:main.uniqueIndividual{value:"1.2.3.5", which:"A"}, queryStat:main.queryStat{count:0x2, first:"30", last:"40", ttl:"300"}}
	//main.aggregatedIndividual{uniqueIndividual:main.uniqueIndividual{value:"www.example.com", which:"Q"}, queryStat:main.queryStat{count:0x5, first:"10", last:"200", ttl:""}}

}

func BenchmarkAggregate(b *testing.B) {
	aggregator := NewDNSAggregator()
	var total uint
	for i := 0; i < b.N; i++ {
		err := aggregate(aggregator, "test_data/dns_json.log")
		if err != nil {
			b.Fatal(err)
		}
		aggregated := aggregator.GetResult()
		total += aggregated.TotalRecords
	}
}

func ExampleResultTupleJSONReader() {
	ag := NewDNSAggregator()

	ag.AddRecord(DNSRecord{
		ts:      10,
		query:   "www.example.com",
		qtype:   "A",
		answers: []string{"1.2.3.4"},
		ttls:    []string{"300"},
	})
	ag.AddRecord(DNSRecord{
		ts:      20,
		query:   "www.example.com",
		qtype:   "A",
		answers: []string{"1.2.3.5"},
		ttls:    []string{"300"},
	})

	res := ag.GetResult()
	sort.Sort(ByTuple(res.Tuples))
	reader := res.TupleJSONReader(false)

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
	}
	fmt.Printf("%s", body)
	// Output:
	//{"query":"www.example.com","type":"A","answer":"1.2.3.4","ttl":"300","count":1,"first":10,"last":10}
	//{"query":"www.example.com","type":"A","answer":"1.2.3.5","ttl":"300","count":1,"first":20,"last":20}
}

func ExampleResultIndividualJSONReader() {
	ag := NewDNSAggregator()

	ag.AddRecord(DNSRecord{
		ts:      10,
		query:   "www.example.com",
		qtype:   "A",
		answers: []string{"1.2.3.4"},
		ttls:    []string{"300"},
	})
	ag.AddRecord(DNSRecord{
		ts:      20,
		query:   "www.example.com",
		qtype:   "A",
		answers: []string{"1.2.3.5"},
		ttls:    []string{"300"},
	})

	res := ag.GetResult()
	sort.Sort(ByValue(res.Individual))
	reader := res.IndividualJSONReader(false)

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
	}
	fmt.Printf("%s", body)
	// Output:
	//{"value":"1.2.3.4","which":"A","count":1,"first":10,"last":10}
	//{"value":"1.2.3.5","which":"A","count":1,"first":20,"last":20}
	//{"value":"www.example.com","which":"Q","count":2,"first":10,"last":20}
}
