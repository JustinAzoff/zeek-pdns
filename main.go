package main

import (
	"log"
	"os"
)

func main() {
	fn := os.Args[1]
	//for _, q := range aggregated {
	//	fmt.Printf("%-8d %-30s %-4s %-30s %s %s %s\n", q.count, q.query, q.qtype, q.answer, q.ttl, q.first, q.last)
	//}
	mystore, err := NewStore("sqlite", "db.sqlite")
	if err != nil {
		log.Fatal(err)
	}

	indexed, err := mystore.IsLogIndexed(fn)
	if err != nil {
		log.Fatal(err)
	}
	if indexed {
		log.Printf("Already indexed: %s", fn)
		return
	}

	aggregated, av, err := aggregate(fn)
	if err != nil {
		log.Fatal(err)
	}
	err = mystore.Update(aggregated, av)
	if err != nil {
		log.Fatal(err)
	}
	err = mystore.SetLogIndexed(fn)
	if err != nil {
		log.Fatal(err)
	}
	recs, err := mystore.FindIndividual("www.reddit.com")
	if err != nil {
		log.Fatal(err)
	}
	for _, rec := range recs {
		log.Printf("%#v", rec)
	}
}
