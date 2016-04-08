package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Store interface {
	Init() error
	IsLogIndexed(filename string) (bool, error)
	SetLogIndexed(filename string) error
	Update(aggregationResult) (UpdateResult, error)
	FindQueryTuples(query string) (tupleResults, error)
	FindTuples(query string) (tupleResults, error)
	FindIndividual(value string) (individualResults, error)
	LikeTuples(query string) (tupleResults, error)
	LikeIndividual(value string) (individualResults, error)
	Close() error
}

type tupleResult struct {
	Query  string
	Type   string
	Answer string
	Count  uint
	TTL    uint
	First  string
	Last   string
}

type tupleResults []tupleResult

func (tr tupleResults) Display() {
	if len(tr) == 0 {
		return
	}
	header := []string{"Query", "Type", "Answer", "Count", "TTL", "First", "Last"}
	fmt.Println(strings.Join(header, "\t"))
	for _, rec := range tr {
		fmt.Println(rec)
	}
}
func (tr tupleResult) String() string {
	count := fmt.Sprintf("%d", tr.Count)
	ttl := fmt.Sprintf("%d", tr.TTL)
	s := []string{tr.Query, tr.Type, tr.Answer, count, ttl, tr.First, tr.Last}
	return strings.Join(s, "\t")
}

type individualResult struct {
	Value string
	Which string
	Count uint
	First string
	Last  string
}
type individualResults []individualResult

func (ir individualResults) Display() {
	if len(ir) == 0 {
		return
	}
	header := []string{"Value", "Which", "Count", "First", "Last"}
	fmt.Println(strings.Join(header, "\t"))
	for _, rec := range ir {
		fmt.Println(rec)
	}
}
func (ir individualResult) String() string {
	count := fmt.Sprintf("%d", ir.Count)
	s := []string{ir.Value, ir.Which, count, ir.First, ir.Last}
	return strings.Join(s, "\t")
}

type UpdateResult struct {
	Inserted uint
	Updated  uint
	Duration time.Duration
}

var storeFactories = map[string]func(string) (Store, error){
	"sqlite": NewSQLiteStore,
}

func NewStore(storeType string, filename string) (Store, error) {
	storeFactory, ok := storeFactories[storeType]
	if !ok {
		return nil, errors.New("Invalid store type")
	}
	s, err := storeFactory(filename)
	if err != nil {
		return nil, err
	}
	err = s.Init()
	if err != nil {
		return nil, err
	}
	return s, err
}
