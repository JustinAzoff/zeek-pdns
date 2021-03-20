package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
	testStores = append(testStores, storeTest{"postgresql", pgTestUrl})
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

func LoadFile(t *testing.T, s Store, fn string) UpdateResult {
	aggregator := NewDNSAggregator()
	err := aggregate(aggregator, fn)
	if err != nil {
		t.Fatal(err)
	}
	aggregated := aggregator.GetResult()
	result, err := s.Update(aggregated)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func doTestUpdating(t *testing.T, s Store, forward bool) {
	s.Clear()
	s.Init()

	var files []string

	if forward {
		files = []string{"test_data/reddit_1.txt", "test_data/reddit_2.txt"}
	} else {
		files = []string{"test_data/reddit_2.txt", "test_data/reddit_1.txt"}
	}

	result_a := LoadFile(t, s, files[0])
	result_b := LoadFile(t, s, files[1])

	// Hack for now. Clickhouse store doesn't report inserted vs updated
	//TODO: add a method to Store interface to return a bool for this
	expected_inserted := 31
	expected_updated := 0
	if _, ok := s.(*CHStore); ok {
		expected_inserted = 0
		expected_updated = 31
	}
	assert.EqualValues(t, result_a.Inserted, expected_inserted)
	assert.EqualValues(t, result_a.Updated, expected_updated)

	assert.EqualValues(t, result_b.Inserted, 0)
	assert.EqualValues(t, result_b.Updated, 31)

	recs, err := s.FindIndividual("www.reddit.com")
	if err != nil {
		t.Fatalf("Failed to find: %v", err)
		return
	}
	//www.reddit.com  Q       2       2016-04-01 00:03:03     2016-04-01 21:55:04
	if assert.Equal(t, len(recs), 1) {
		rec := recs[0]
		assert.Equal(t, rec.Value, "www.reddit.com")
		assert.Equal(t, rec.Which, "Q")
		assert.EqualValues(t, rec.Count, 2)
		//This is stupid, but I need to fix things so that they return actual dates
		//and get a handle on the timezone BS.
		//So for now, ignore the ' ' vs 'T' difference, and the hour
		assert.Regexp(t, "2016-04-01...:03:03", rec.First)
		assert.Regexp(t, "2016-04-01...:55:04", rec.Last)
	}
	//www.reddit.com  A       198.41.208.138  2       300     2016-04-01 00:03:03     2016-04-01 21:55:04

	trecs, err := s.FindTuples("198.41.208.138")
	if err != nil {
		t.Fatalf("Failed to find: %v", err)
		return
	}
	if assert.Equal(t, len(trecs), 1) {
		rec := trecs[0]
		assert.Equal(t, rec.Query, "www.reddit.com")
		assert.Equal(t, rec.Type, "A")
		assert.Equal(t, rec.Answer, "198.41.208.138")
		assert.EqualValues(t, rec.Count, 2)
		//This is stupid, but I need to fix things so that they return actual dates
		//and get a handle on the timezone BS.
		//So for now, ignore the ' ' vs 'T' difference, and the hour
		assert.Regexp(t, "2016-04-01...:03:03", rec.First)
		assert.Regexp(t, "2016-04-01...:55:04", rec.Last)
	}

}

// Output:
//A: Inserted=31 Updated=0
//B: Inserted=0 Updated=31
//Individual records: 1
//www.reddit.com	Q	2	2016-04-01 00:03:03	2016-04-01 21:55:04
//Tuple records: 1
//www.reddit.com	A	198.41.208.138	2	300	2016-04-01 00:03:03	2016-04-01 21:55:04

func BenchmarkUpdateSQLite(b *testing.B) {
	aggregator := NewDNSAggregator()
	err := aggregate(aggregator, "big.log")
	if err != nil {
		b.Fatal(err)
	}
	aggregated := aggregator.GetResult()
	if err != nil {
		b.Fatal(err)
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
		b.Fatal(err)
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
		t.Run(fmt.Sprintf("Indexing/%s", ts.storetype), func(t *testing.T) {
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

func doTestStore(t *testing.T, store Store) {
	t.Run("testLogIndexed", func(t *testing.T) {
		doTestLogIndexed(t, store)
	})
	t.Run("forward", func(t *testing.T) {
		doTestUpdating(t, store, true)
	})
	t.Run("reverse", func(t *testing.T) {
		doTestUpdating(t, store, false)
	})
}

func TestStoreIndexing(t *testing.T) {
	for _, ts := range testStores {
		t.Run(ts.storetype, func(t *testing.T) {
			store, err := NewStore(ts.storetype, ts.uri)
			if err != nil {
				t.Fatalf("can't create store at %s: %v", ts.uri, err)
			}
			doTestStore(t, store)
		})
	}
}
