package main

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "bro-pdns",
	Short: "Passive DNS Collection for BRO",
	Run:   nil,
}

var IndexCmd = &cobra.Command{
	Use:   "index",
	Short: "Index a dns log file",
	Run: func(cmd *cobra.Command, args []string) {
		mystore, err := NewStore("sqlite", "db.sqlite")
		if err != nil {
			log.Fatal(err)
		}
		mystore.Begin()
		aggregator := NewDNSAggregator()
		for _, fn := range args {
			indexed, err := mystore.IsLogIndexed(fn)
			if err != nil {
				log.Fatal(err)
			}
			if indexed {
				log.Printf("%s: Already indexed", fn)
				continue
			}

			fileAgg := NewDNSAggregator()
			err = aggregate(fileAgg, fn)
			if err != nil {
				log.Fatal(err)
			}
			aggregator.Merge(fileAgg)
			aggregated := fileAgg.GetResult()
			log.Printf("%s: Aggregation: Duration=%0.1f TotalRecords=%d Tuples=%d Individual=%d", fn,
				aggregated.Duration.Seconds(), aggregated.TotalRecords, len(aggregated.Tuples), len(aggregated.Individual))
			var emptyStoreResult UpdateResult
			err = mystore.SetLogIndexed(fn, aggregated, emptyStoreResult)
		}
		aggregated := aggregator.GetResult()
		result, err := mystore.Update(aggregated)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("batch: Store: Duration=%0.1f Inserted=%d Updated=%d", result.Duration.Seconds(), result.Inserted, result.Updated)
		err = mystore.Commit()
		if err != nil {
			log.Fatal(err)
		}
	},
}

var FindCmd = &cobra.Command{
	Use:   "find",
	Short: "find records",
	Run:   nil,
}
var FindTupleCmd = &cobra.Command{
	Use:   "tuple",
	Short: "find dns tuples",
	Run: func(cmd *cobra.Command, args []string) {
		mystore, err := NewStore("sqlite", "db.sqlite")
		if err != nil {
			log.Fatal(err)
		}

		for _, value := range args {
			recs, err := mystore.FindTuples(value)
			if err != nil {
				log.Fatal(err)
			}
			recs.Display()
		}
	},
}

var FindIndividualCmd = &cobra.Command{
	Use:   "individual",
	Short: "find an individual dns value",
	Run: func(cmd *cobra.Command, args []string) {
		mystore, err := NewStore("sqlite", "db.sqlite")
		if err != nil {
			log.Fatal(err)
		}

		for _, value := range args {
			recs, err := mystore.FindIndividual(value)
			if err != nil {
				log.Fatal(err)
			}
			recs.Display()
		}
	},
}
var LikeCmd = &cobra.Command{
	Use:   "like",
	Short: "find records like something",
	Run:   nil,
}
var LikeTupleCmd = &cobra.Command{
	Use:   "tuple",
	Short: "find like dns tuples",
	Run: func(cmd *cobra.Command, args []string) {
		mystore, err := NewStore("sqlite", "db.sqlite")
		if err != nil {
			log.Fatal(err)
		}

		for _, value := range args {
			recs, err := mystore.LikeTuples(value)
			if err != nil {
				log.Fatal(err)
			}
			recs.Display()
		}
	},
}

var LikeIndividualCmd = &cobra.Command{
	Use:   "individual",
	Short: "find like individual dns values",
	Run: func(cmd *cobra.Command, args []string) {
		mystore, err := NewStore("sqlite", "db.sqlite")
		if err != nil {
			log.Fatal(err)
		}

		for _, value := range args {
			recs, err := mystore.LikeIndividual(value)
			if err != nil {
				log.Fatal(err)
			}
			recs.Display()
		}
	},
}

var WebCmd = &cobra.Command{
	Use:   "web",
	Short: "start http API",
	Run: func(cmd *cobra.Command, args []string) {
		mystore, err := NewStore("sqlite", "db.sqlite")
		if err != nil {
			log.Fatal(err)
		}
		startWeb(mystore)
	},
}

func init() {
	RootCmd.AddCommand(IndexCmd)

	RootCmd.AddCommand(FindCmd)
	FindCmd.AddCommand(FindIndividualCmd)
	FindCmd.AddCommand(FindTupleCmd)

	RootCmd.AddCommand(LikeCmd)
	LikeCmd.AddCommand(LikeIndividualCmd)
	LikeCmd.AddCommand(LikeTupleCmd)

	RootCmd.AddCommand(WebCmd)
}

func main() {

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

}
