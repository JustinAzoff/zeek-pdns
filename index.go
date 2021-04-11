package main

import (
	"fmt"
	"log"
)

func index(store Store, filenames []string) error {
	var didWork bool
	store.Begin()
	aggregator := NewDNSAggregator()
	var emptyStoreResult UpdateResult
	aggMap := make(map[string]aggregationResult)
	for _, fn := range filenames {
		indexed, err := store.IsLogIndexed(fn)
		if err != nil {
			return fmt.Errorf("store.IsLogIndexed: %w", err)
		}
		if indexed {
			log.Printf("%s: Already indexed", fn)
			continue
		}

		fileAgg := NewDNSAggregator()
		err = aggregate(fileAgg, fn)
		if err != nil {
			return fmt.Errorf("Error Aggregating %s: %w", fn, err)
		}
		aggregator.Merge(fileAgg)
		aggregated := fileAgg.GetResult()
		log.Printf("%s: Aggregation: Duration=%0.1f TotalRecords=%d SkippedRecords=%d Tuples=%d Individual=%d",
			fn,
			aggregated.Duration.Seconds(),
			aggregated.TotalRecords,
			aggregated.SkippedRecords,
			aggregated.TuplesLen,
			aggregated.IndividualLen,
		)
		aggMap[fn] = aggregated.ShallowCopy()
		didWork = true
	}
	if !didWork {
		return nil
		//TODO: rollback transaction
	}
	aggregated := aggregator.GetResult()
	result, err := store.Update(aggregated)
	if err != nil {
		return fmt.Errorf("store.Update: %w", err)
	}
	log.Printf("batch: Store: Duration=%0.1f Inserted=%d Updated=%d", result.Duration.Seconds(), result.Inserted, result.Updated)
	for fn, aggregated := range aggMap {
		err = store.SetLogIndexed(fn, aggregated, emptyStoreResult)
		if err != nil {
			return fmt.Errorf("store.SetLogIndexed: %w", err)
		}
	}
	err = store.Commit()
	if err != nil {
		return fmt.Errorf("store.Commit: %w", err)
	}
	return nil
}
