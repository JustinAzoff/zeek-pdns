package main

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func getStore() Store {
	storeType := viper.GetString("store.type")
	storeUri := viper.GetString("store.uri")
	mystore, err := NewStore(storeType, storeUri)
	if err != nil {
		log.Fatal(err)
	}
	return mystore
}

var RootCmd = &cobra.Command{
	Use:   "bro-pdns",
	Short: "Passive DNS Collection for BRO",
	Run:   nil,
}

var IndexCmd = &cobra.Command{
	Use:   "index",
	Short: "Index a dns log file",
	Run: func(cmd *cobra.Command, args []string) {
		mystore := getStore()
		var didWork bool
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
			log.Printf("%s: Aggregation: Duration=%0.1f TotalRecords=%d SkippedRecords=%d Tuples=%d Individual=%d", fn,
				aggregated.Duration.Seconds(), aggregated.TotalRecords, aggregated.SkippedRecords, len(aggregated.Tuples), len(aggregated.Individual))
			var emptyStoreResult UpdateResult
			//TODO: Since clickhouse doesn't do transactions, this should be done last
			err = mystore.SetLogIndexed(fn, aggregated, emptyStoreResult)
			if err != nil {
				log.Fatal(err)
			}
			didWork = true
		}
		if !didWork {
			return
			//TODO: rollback transaction
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
	Use:   "tuples",
	Short: "find dns tuples",
	Run: func(cmd *cobra.Command, args []string) {
		mystore := getStore()

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
		mystore := getStore()

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
	Use:   "tuples",
	Short: "find like dns tuples",
	Run: func(cmd *cobra.Command, args []string) {
		mystore := getStore()

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
		mystore := getStore()

		for _, value := range args {
			recs, err := mystore.LikeIndividual(value)
			if err != nil {
				log.Fatal(err)
			}
			recs.Display()
		}
	},
}

var DeleteOldCmd = &cobra.Command{
	Use:   "delete-old",
	Short: "delete old records",
	Run: func(cmd *cobra.Command, args []string) {
		mystore := getStore()
		days := viper.GetInt("deleteold.days")
		rows, err := mystore.DeleteOld(int64(days))
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Deleted %d records", rows)
	},
}

var WebCmd = &cobra.Command{
	Use:   "web",
	Short: "start http API",
	Run: func(cmd *cobra.Command, args []string) {
		mystore := getStore()
		bind := viper.GetString("http.listen")
		startWeb(mystore, bind)
	},
}

var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Output version number",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(VERSION)
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

	DeleteOldCmd.Flags().Int64("days", 365, "Age in days of records to be deleted")
	viper.BindPFlag("deleteold.days", DeleteOldCmd.Flags().Lookup("days"))
	viper.BindEnv("deleteold.days", "PDNS_DELETE_OLD_DAYS")
	RootCmd.AddCommand(DeleteOldCmd)

	WebCmd.Flags().String("listen", ":8080", "Address to listen on")
	viper.BindPFlag("http.listen", WebCmd.Flags().Lookup("listen"))
	viper.BindEnv("http.listen", "PDNS_HTTP_LISTEN")

	RootCmd.AddCommand(WebCmd)
	RootCmd.AddCommand(VersionCmd)

	RootCmd.PersistentFlags().String("store", "sqlite", "Backend data store")
	viper.BindPFlag("store.type", RootCmd.PersistentFlags().Lookup("store"))
	viper.BindEnv("store.type", "PDNS_STORE_TYPE")

	RootCmd.PersistentFlags().String("uri", "db.sqlite", "Backend data store URI")
	viper.BindPFlag("store.uri", RootCmd.PersistentFlags().Lookup("uri"))
	viper.BindEnv("store.uri", "PDNS_STORE_URI")

	viper.AutomaticEnv()
}

func main() {

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

}
