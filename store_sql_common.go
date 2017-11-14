package main

import (
	"database/sql"
	"errors"
	"time"

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
	conn    *sqlx.DB
	tx      *sql.Tx
	txDepth int
}

func (s *SQLCommonStore) Clear() error {
	_, err := s.conn.Exec("DELETE FROM filenames;DELETE FROM individual;DELETE FROM tuples;")
	return err
}
func (s *SQLCommonStore) Begin() error {
	_, err := s.BeginTx()
	return err
}

func (s *SQLCommonStore) BeginTx() (*sql.Tx, error) {
	if s.tx != nil {
		s.txDepth += 1
		//log.Printf("Returning existing transaction: depth=%d\n", s.txDepth)
		return s.tx, nil
	}
	//log.Printf("new transaction\n")
	tx, err := s.conn.Begin()
	if err != nil {
		return tx, err
	}
	s.tx = tx
	s.txDepth += 1
	return s.tx, nil
}
func (s *SQLCommonStore) Commit() error {
	if s.tx == nil {
		return errors.New("Commit outside of transaction")
	}
	s.txDepth -= 1
	if s.txDepth > 0 {
		//log.Printf("Not commiting stacked transaction: depth=%d\n", s.txDepth)
		return nil // No OP
	}
	//log.Printf("Commiting transaction: depth=%d\n", s.txDepth)
	err := s.tx.Commit()
	s.tx = nil
	return err
}

func (s *SQLCommonStore) IsLogIndexed(filename string) (bool, error) {
	tx, err := s.BeginTx()
	if err != nil {
		return false, err
	}
	defer s.Commit()
	var fn string
	err = tx.QueryRow("SELECT filename FROM filenames WHERE filename=$1", filename).Scan(&fn)
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
	tx, err := s.BeginTx()
	defer s.Commit()
	if err != nil {
		return err
	}
	q := `INSERT INTO filenames (filename,
	      aggregation_time, total_records, skipped_records, tuples, individual,
	      store_time, inserted, updated)
	      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`
	_, err = tx.Exec(q, filename,
		ar.Duration.Seconds(), ar.TotalRecords, ar.SkippedRecords, ar.TuplesLen, ar.IndividualLen,
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
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query = $1 OR answer = $2 ORDER BY query, answer", rquery, query)
	reverseQuery(tr)

	return tr, err
}
func (s *SQLCommonStore) LikeTuples(query string) (tupleResults, error) {
	tr := []tupleResult{}
	rquery := Reverse(query)
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query like $1 OR answer like $2 ORDER BY query, answer", rquery+"%", query+"%")
	reverseQuery(tr)
	return tr, err
}
func (s *SQLCommonStore) FindIndividual(value string) (individualResults, error) {
	rvalue := Reverse(value)
	tr := []individualResult{}
	err := s.conn.Select(&tr, "SELECT * FROM individual WHERE (which='A' AND value = $1) OR (which='Q' AND value = $2) ORDER BY value", value, rvalue)
	reverseValue(tr)
	return tr, err
}

func (s *SQLCommonStore) LikeIndividual(value string) (individualResults, error) {
	rvalue := Reverse(value)
	tr := []individualResult{}
	err := s.conn.Select(&tr, "SELECT * FROM individual WHERE (which='A' AND value like $1) OR (which='Q' AND value like $2) ORDER BY value", value+"%", rvalue+"%")
	reverseValue(tr)
	return tr, err
}

//DeleteOld Deletes records that haven't been seen in DAYS, returns the total records deleted
func (s *SQLCommonStore) DeleteOld(days int64) (int64, error) {
	var deletedRows int64
	cutoff := time.Now().Add(time.Duration(-1*days) * time.Hour * 24)
	res, err := s.conn.Exec("DELETE FROM individual WHERE last < $1", cutoff)
	if err != nil {
		return deletedRows, err
	}
	rows, err := res.RowsAffected()
	deletedRows += rows
	if err != nil {
		return deletedRows, err
	}

	res, err = s.conn.Exec("DELETE FROM tuples WHERE last < $1", cutoff)
	if err != nil {
		return deletedRows, err
	}
	rows, err = res.RowsAffected()
	deletedRows += rows
	return deletedRows, err
}
