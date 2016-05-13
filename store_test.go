package main

import (
	"fmt"
	"log"
	"os"
	"testing"
)

var pgTestUrl = "postgres://postgres:password@localhost/pdns_test?sslmode=disable"

func init() {
	envUrl := os.Getenv("PG_TEST_URL")
	if envUrl != "" {
		pgTestUrl = envUrl
	}
}

func doTestLogIndexed(t *testing.T, s Store) {
	s.Clear()
	testFilename := "test.log"
	indexed, err := s.IsLogIndexed(testFilename)
	if err != nil {
		t.Fatal(err)
	}
	if indexed != false {
		t.Errorf("IsLogIndexed(%q) == %t, want false", testFilename, indexed)
	}

	var ar aggregationResult
	var ur UpdateResult

	err = s.SetLogIndexed(testFilename, ar, ur)
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
	s.Clear()

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
		fmt.Print(err)
		return
	}
	fmt.Printf("Individual records: %d\n", len(recs))
	for _, rec := range recs {
		fmt.Printf("%s\n", rec)
	}
	trecs, err := s.FindTuples("198.41.208.138")
	if err != nil {
		fmt.Print(err)
		return
	}
	fmt.Printf("Tuple records: %d\n", len(trecs))
	for _, rec := range trecs {
		fmt.Printf("%s\n", rec)
	}

}

func TestLogIndexedSqlite(t *testing.T) {
	store, err := NewStore("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	doTestLogIndexed(t, store)
}
func TestLogIndexedPg(t *testing.T) {
	store, err := NewStore("postgresql", pgTestUrl)
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
	//Individual records: 1
	//www.reddit.com	Q	2	2016-04-01 00:03:03	2016-04-01 21:55:04
	//Tuple records: 1
	//www.reddit.com	A	198.41.208.138	2	300	2016-04-01 00:03:03	2016-04-01 21:55:04
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
	//Individual records: 1
	//www.reddit.com	Q	2	2016-04-01 00:03:03	2016-04-01 21:55:04
	//Tuple records: 1
	//www.reddit.com	A	198.41.208.138	2	300	2016-04-01 00:03:03	2016-04-01 21:55:04
}

func ExampleUpdatingPgForward() {
	store, err := NewStore("postgresql", pgTestUrl)
	if err != nil {
		return
	}
	doExampleUpdating(store, true)
	// Output:
	//A: Inserted=31 Updated=0
	//B: Inserted=0 Updated=31
	//Individual records: 1
	//www.reddit.com	Q	2	2016-04-01T00:03:03.743478Z	2016-04-01T21:55:04.609809Z
	//Tuple records: 1
	//www.reddit.com	A	198.41.208.138	2	300	2016-04-01T00:03:03.743478Z	2016-04-01T21:55:04.609809Z
}

func ExampleUpdatingPgReverse() {
	store, err := NewStore("postgresql", pgTestUrl)
	if err != nil {
		return
	}
	doExampleUpdating(store, false)
	// Output:
	//A: Inserted=31 Updated=0
	//B: Inserted=0 Updated=31
	//Individual records: 1
	//www.reddit.com	Q	2	2016-04-01T00:03:03.743478Z	2016-04-01T21:55:04.609809Z
	//Tuple records: 1
	//www.reddit.com	A	198.41.208.138	2	300	2016-04-01T00:03:03.743478Z	2016-04-01T21:55:04.609809Z
}

func BenchmarkUpdateSQLite(b *testing.B) {
	aggregated, err := aggregate("big.log")
	if err != nil {
		log.Fatal(err)
	}
	store, err := NewStore("sqlite", ":memory:")
	if err != nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Update(aggregated)
	}
}

func BenchmarkUpdatePg(b *testing.B) {
	aggregated, err := aggregate("big.log")
	if err != nil {
		log.Fatal(err)
	}
	store, err := NewStore("postgresql", pgTestUrl)
	if err != nil {
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Update(aggregated)
	}
}
