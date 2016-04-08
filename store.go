package main

import (
	"errors"
	"time"
)

type Store interface {
	Init() error
	IsLogIndexed(filename string) (bool, error)
	SetLogIndexed(filename string) error
	Update(aggregationResult) (UpdateResult, error)
	FindQueryTuples(query string) ([]tupleResult, error)
	FindTuples(query string) ([]tupleResult, error)
	FindIndividual(value string) ([]individualResult, error)
	LikeTuples(query string) ([]tupleResult, error)
	LikeIndividual(value string) ([]individualResult, error)
	Close() error
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
