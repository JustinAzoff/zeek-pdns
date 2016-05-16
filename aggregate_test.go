package main

import "fmt"

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

	fmt.Printf("Tuples:\n")
	for _, r := range res.Tuples {
		fmt.Printf("%#v\n", r)
	}
	fmt.Printf("Individual:\n")
	for _, r := range res.Individual {
		fmt.Printf("%#v\n", r)
	}
	// Output:
	//Tuples:
	//main.aggregatedTuple{uniqueTuple:main.uniqueTuple{query:"www.example.com", answer:"1.2.3.4", qtype:"A"}, queryStat:main.queryStat{count:0x2, first:"10", last:"20", ttl:"300"}}
	//Individual:
	//main.aggregatedIndividual{uniqueIndividual:main.uniqueIndividual{value:"www.example.com", which:"Q"}, queryStat:main.queryStat{count:0x2, first:"10", last:"20", ttl:""}}
	//main.aggregatedIndividual{uniqueIndividual:main.uniqueIndividual{value:"1.2.3.4", which:"A"}, queryStat:main.queryStat{count:0x2, first:"10", last:"20", ttl:"300"}}
}
