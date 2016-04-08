package main

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

func Reverse(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

type SQLCommonStore struct {
	conn *sqlx.DB
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

func reverseQuery(tr tupleResults) {
	for idx, rec := range tr {
		rec.Query = Reverse(rec.Query)
		tr[idx] = rec
	}
}
func reverseValue(tr individualResults) {
	for idx, rec := range tr {
		if rec.Which == "Q" {
			rec.Value = Reverse(rec.Value)
			tr[idx] = rec
		}
	}
}

func (s *SQLCommonStore) FindQueryTuples(query string) (tupleResults, error) {
	tr := []tupleResult{}
	query = Reverse(query)
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query = ?", query)
	reverseQuery(tr)
	return tr, err
}
func (s *SQLCommonStore) FindTuples(query string) (tupleResults, error) {
	tr := []tupleResult{}
	rquery := Reverse(query)
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query = ? OR answer = ?", rquery, query)
	reverseQuery(tr)

	return tr, err
}
func (s *SQLCommonStore) LikeTuples(query string) (tupleResults, error) {
	tr := []tupleResult{}
	rquery := Reverse(query)
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query like ? OR answer like ?", rquery+"%", query+"%")
	reverseQuery(tr)
	return tr, err
}
func (s *SQLCommonStore) FindIndividual(value string) (individualResults, error) {
	rvalue := Reverse(value)
	tr := []individualResult{}
	err := s.conn.Select(&tr, "SELECT * FROM individual WHERE (which='A' AND value = ?) OR (which='Q' AND value =?)", value, rvalue)
	reverseValue(tr)
	return tr, err
}

func (s *SQLCommonStore) LikeIndividual(value string) (individualResults, error) {
	rvalue := Reverse(value)
	tr := []individualResult{}
	err := s.conn.Select(&tr, "SELECT * FROM individual WHERE (which='A' AND value like ?) OR (which='Q' AND value like ?)", value+"%", rvalue+"%")
	reverseValue(tr)
	return tr, err
}
