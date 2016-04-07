package main

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type SQLCommonStore struct {
	conn *sqlx.DB
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

func (s *SQLCommonStore) IsLogIndexed(filename string) (bool, error) {
	var fn string
	err := s.conn.QueryRow("SELECT filename FROM filenames WHERE filename=?", filename).Scan(&fn)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func (s *SQLCommonStore) SetLogIndexed(filename string) error {
	_, err := s.conn.Exec("INSERT INTO filenames (filename) VALUES (?)", filename)
	return err
}

func (s *SQLCommonStore) FindQueryTuples(query string) ([]tupleResult, error) {
	tr := []tupleResult{}
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query = ?", query)
	return tr, err
}
func (s *SQLCommonStore) FindTuples(query string) ([]tupleResult, error) {
	tr := []tupleResult{}
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query = ? OR answer = ?", query, query)
	return tr, err
}
