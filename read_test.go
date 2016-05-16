package main

import (
	"testing"
)

func BenchmarkRead(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fn := "test_data/reddit_dns_2016-04-01.log"
		aggregator := NewDNSAggregator()
		aggregate(aggregator, fn)
	}
}
