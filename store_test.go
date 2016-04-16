package main

import (
	"fmt"
	"log"
	"testing"
)

func doTestLogIndexed(t *testing.T, s Store) {
	testFilename := "test.log"
	indexed, err := s.IsLogIndexed(testFilename)
	if err != nil {
		t.Fatal(err)
	}
	if indexed != false {
		t.Errorf("IsLogIndexed(%q) == %t, want false", testFilename, indexed)
	}

	err = s.SetLogIndexed(testFilename)
	if err != nil {
		t.Fatal(err)
	}
	indexed, err = s.IsLogIndexed(testFilename)
	if err != nil {
		t.Fatal(err)
	}
	if indexed != true {
		t.Errorf("IsLogIndexed(%q) == %t, want true", testFilename, indexed)
	}
}

func LoadFile(s Store, fn string) UpdateResult {
	aggregated, err := aggregate(fn)
	if err != nil {
		log.Fatal(err)
	}
	result, err := s.Update(aggregated)
	if err != nil {
		log.Fatal(err)
	}
	return result
}

func doExampleUpdating(s Store, forward bool) {

	var files []string

	if forward {
		files = []string{"test_data/reddit_1.txt", "test_data/reddit_2.txt"}
	} else {
		files = []string{"test_data/reddit_2.txt", "test_data/reddit_1.txt"}
	}

	result_a := LoadFile(s, files[0])
	result_b := LoadFile(s, files[1])

	fmt.Printf("A: Inserted=%d Updated=%d\n", result_a.Inserted, result_a.Updated)
	fmt.Printf("B: Inserted=%d Updated=%d\n", result_b.Inserted, result_b.Updated)

	recs, err := s.FindIndividual("www.reddit.com")
	if err != nil {
		return
	}
	fmt.Printf("Total records: %d\n", len(recs))
	for _, rec := range recs {
		fmt.Printf("%s", rec)
	}

}

func TestLogIndexedSqlite(t *testing.T) {
	store, err := NewStore("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	doTestLogIndexed(t, store)
}

func ExampleUpdatingSqliteForward() {
	store, err := NewStore("sqlite", ":memory:")
	if err != nil {
		return
	}
	doExampleUpdating(store, true)
	// Output:
	//A: Inserted=31 Updated=0
	//B: Inserted=0 Updated=31
	//Total records: 1
	//www.reddit.com	Q	2	2016-04-01 00:03:03	2016-04-01 21:55:04
}

func ExampleUpdatingSqliteReverse() {
	store, err := NewStore("sqlite", ":memory:")
	if err != nil {
		return
	}
	doExampleUpdating(store, false)
	// Output:
	//A: Inserted=31 Updated=0
	//B: Inserted=0 Updated=31
	//Total records: 1
	//www.reddit.com	Q	2	2016-04-01 00:03:03	2016-04-01 21:55:04
}
