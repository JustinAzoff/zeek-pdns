package main

import (
	"fmt"
	"log"
	"os"
	"testing"
)

var pgTestUrl = "postgres://postgres:password@localhost/pdns_test?sslmode=disable"
var chTestUrl = "tcp://localhost:9000/default"

type storeTest struct {
	storetype string
	uri       string
}

var testStores = []storeTest{
	{"sqlite", ":memory:"},
}

func init() {
	envUrl := os.Getenv("PG_TEST_URL")
	if envUrl != "" {
		pgTestUrl = envUrl
	}
	//testStores = append(testStores, storeTest{"postgresql", pgTestUrl})
	envUrl = os.Getenv("CH_TEST_URL")
	if envUrl != "" {
		chTestUrl = envUrl
	}
	testStores = append(testStores, storeTest{"clickhouse", chTestUrl})
}

func doTestLogIndexed(t *testing.T, s Store) {
	s.Clear()
	s.Init()
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
	aggregator := NewDNSAggregator()
	err := aggregate(aggregator, fn)
	if err != nil {
		log.Fatal(err)
	}
	aggregated := aggregator.GetResult()
	result, err := s.Update(aggregated)
	if err != nil {
		log.Fatal(err)
	}
	return result
}

func doExampleUpdating(s Store, forward bool) {
	s.Clear()
	s.Init()

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

func testLogIndexedSqlite(t *testing.T) {
	store, err := NewStore("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	doTestLogIndexed(t, store)
}
func testLogIndexedPg(t *testing.T) {
	store, err := NewStore("postgresql", pgTestUrl)
	if err != nil {
		t.Fatal(err)
	}
	doTestLogIndexed(t, store)
}
func testLogIndexedCh(t *testing.T) {
	store, err := NewStore("clickhouse", chTestUrl)
	if err != nil {
		t.Fatal(err)
	}
	doTestLogIndexed(t, store)
}

func TestLogIndexed(t *testing.T) {
	t.Run("sqlite", testLogIndexedSqlite)
	t.Run("postgresql", testLogIndexedPg)
	t.Run("clickhouse", testLogIndexedCh)
}

func ExampleUpdatingSqliteForward() {
	store, err := NewStore("sqlite", ":memory:")
	if err != nil {
		fmt.Print(err)
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
		fmt.Print(err)
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
		fmt.Print(err)
		return
	}
	doExampleUpdating(store, true)
	// Output:
	//A: Inserted=31 Updated=0
	//B: Inserted=0 Updated=31
	//Individual records: 1
	//www.reddit.com	Q	2	2016-04-01T00:03:03.75Z	2016-04-01T21:55:04.5Z
	//Tuple records: 1
	//www.reddit.com	A	198.41.208.138	2	300	2016-04-01T00:03:03.75Z	2016-04-01T21:55:04.5Z
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
	//www.reddit.com	Q	2	2016-04-01T00:03:03.75Z	2016-04-01T21:55:04.5Z
	//Tuple records: 1
	//www.reddit.com	A	198.41.208.138	2	300	2016-04-01T00:03:03.75Z	2016-04-01T21:55:04.5Z
}

func ExampleUpdatingClickhouseForward() {
	store, err := NewStore("clickhouse", chTestUrl)
	if err != nil {
		fmt.Print(err)
		return
	}
	doExampleUpdating(store, true)
	// Output:
	//A: Inserted=0 Updated=31
	//B: Inserted=0 Updated=31
	//Individual records: 1
	//www.reddit.com	Q	2	2016-03-31T20:03:03-04:00	2016-04-01T17:55:04-04:00
	//Tuple records: 1
	//www.reddit.com	A	198.41.208.138	2	300	2016-03-31T20:03:03-04:00	2016-04-01T17:55:04-04:00
}

func BenchmarkUpdateSQLite(b *testing.B) {
	aggregator := NewDNSAggregator()
	err := aggregate(aggregator, "big.log")
	if err != nil {
		log.Fatal(err)
	}
	aggregated := aggregator.GetResult()
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
	store, err := NewStore("postgresql", pgTestUrl)
	if err != nil {
		b.Fatalf("NewStore failed: %s", err)
	}

	aggregator := NewDNSAggregator()
	err = aggregate(aggregator, "big.log.gz")
	if err != nil {
		log.Fatal(err)
	}
	aggregated := aggregator.GetResult()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.Update(aggregated)
		if err != nil {
			b.Fatalf("store.Update failed: %s", err)
		}
	}
}

func testFile(t *testing.T, s Store, fn string) error {
	aggregator := NewDNSAggregator()
	err := aggregate(aggregator, fn)
	if err != nil {
		return err
	}
	aggregated := aggregator.GetResult()
	_, err = s.Update(aggregated)
	if err != nil {
		return err
	}
	return nil
}
func TestIndexingFiles(t *testing.T) {
	allFiles := []string{
		"./test_data/nbtstat.log",
		"./test_data/garbage.log",
		"./test_data/bad_ttl.log",
		"./test_data/dns_json_iso8601.json",
	}
	for _, ts := range testStores {
		t.Run(ts.storetype, func(t *testing.T) {
			store, err := NewStore(ts.storetype, ts.uri)
			if err != nil {
				t.Fatalf("can't create store at %s: %v", ts.uri, err)
			}
			for _, fn := range allFiles {
				t.Run(fn, func(t *testing.T) {
					testFile(t, store, fn)
				})
			}
		})
	}
}
