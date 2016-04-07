package main

import "errors"

type Store interface {
	Init() error
	IsLogIndexed(filename string) (bool, error)
	SetLogIndexed(filename string) error
	Update([]aggregationResult, []valueAggregationResult) error
	Close() error
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
