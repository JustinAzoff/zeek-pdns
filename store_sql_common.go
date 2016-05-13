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

func (s *SQLCommonStore) Clear() error {
	_, err := s.conn.Exec("DELETE FROM filenames;DELETE FROM individual;DELETE FROM tuples;")
	return err
}

func (s *SQLCommonStore) IsLogIndexed(filename string) (bool, error) {
	var fn string
	err := s.conn.QueryRow("SELECT filename FROM filenames WHERE filename=$1", filename).Scan(&fn)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func (s *SQLCommonStore) SetLogIndexed(filename string, ar aggregationResult, ur UpdateResult) error {
	q := `INSERT INTO filenames (filename,
	      aggregation_time, total_records, tuples, individual,
	      store_time, inserted, updated)
	      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`
	_, err := s.conn.Exec(q, filename,
		ar.Duration.Seconds(), ar.TotalRecords, len(ar.Tuples), len(ar.Individual),
		ur.Duration.Seconds(), ur.Inserted, ur.Updated)
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
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query = $1", query)
	reverseQuery(tr)
	return tr, err
}
func (s *SQLCommonStore) FindTuples(query string) (tupleResults, error) {
	tr := []tupleResult{}
	rquery := Reverse(query)
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query = $1 OR answer = $2", rquery, query)
	reverseQuery(tr)

	return tr, err
}
func (s *SQLCommonStore) LikeTuples(query string) (tupleResults, error) {
	tr := []tupleResult{}
	rquery := Reverse(query)
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query like $1 OR answer like $2", rquery+"%", query+"%")
	reverseQuery(tr)
	return tr, err
}
func (s *SQLCommonStore) FindIndividual(value string) (individualResults, error) {
	rvalue := Reverse(value)
	tr := []individualResult{}
	err := s.conn.Select(&tr, "SELECT * FROM individual WHERE (which='A' AND value = $1) OR (which='Q' AND value = $2)", value, rvalue)
	reverseValue(tr)
	return tr, err
}

func (s *SQLCommonStore) LikeIndividual(value string) (individualResults, error) {
	rvalue := Reverse(value)
	tr := []individualResult{}
	err := s.conn.Select(&tr, "SELECT * FROM individual WHERE (which='A' AND value like $1) OR (which='Q' AND value like $2)", value+"%", rvalue+"%")
	reverseValue(tr)
	return tr, err
}
