package main

import (
	"log"
	"os"
)

func main() {
	fn := os.Args[1]
	aggregated, av, err := aggregate(fn)
	if err != nil {
		log.Fatal(err)
	}
	//for _, q := range aggregated {
	//	fmt.Printf("%-8d %-30s %-4s %-30s %s %s %s\n", q.count, q.query, q.qtype, q.answer, q.ttl, q.first, q.last)
	//}
	mystore, err := NewStore("sqlite", "db.sqlite")
	if err != nil {
		log.Fatal(err)
	}
	err = mystore.Update(aggregated, av)
	if err != nil {
		log.Fatal(err)
	}
}
