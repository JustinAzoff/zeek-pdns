package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
	"github.com/pkg/errors"
)

var chschema = []string{
	`
CREATE TABLE IF NOT EXISTS tuples (
    whatever Date DEFAULT '2000-01-01',
    query String,
    type String,
    answer String,
    ttl AggregateFunction(anyLast, UInt16),
    first AggregateFunction(min, DateTime),
    last AggregateFunction(max, DateTime),
    count AggregateFunction(count, UInt64)
  ) ENGINE = AggregatingMergeTree(whatever, (query, answer), 8192);
`,

	`
CREATE TABLE IF NOT EXISTS individual (
    whatever Date DEFAULT '2000-01-01',
    which Enum8('Q'=0, 'A'=1),
    value String,
    first AggregateFunction(min, DateTime),
    last AggregateFunction(max, DateTime),
    count AggregateFunction(count, UInt64)
  ) ENGINE = AggregatingMergeTree(whatever, (which, value), 8192);
`,
	`
CREATE TABLE IF NOT EXISTS filenames (
	day Date DEFAULT toDate(ts),
	ts DateTime DEFAULT now(),
	filename String,
	aggregation_time Float64,
	total_records UInt64,
	skipped_records UInt64,
	tuples UInt64,
	individual UInt64,
	store_time Float64,
	inserted UInt64,
	updated UInt64
  ) ENGINE = MergeTree(day, (filename), 8192);
`}

const tuples_temp_stmt = `
CREATE TABLE tuples_temp (
    query String,
    type String,
    answer String,
    ttl UInt16,
    first DateTime,
    last DateTime,
    count UInt64
) ENGINE = Log`

type CHStore struct {
	conn *sqlx.DB
}

func NewCHStore(uri string) (Store, error) {
	conn, err := sqlx.Open("clickhouse", uri)
	if err != nil {
		return nil, err
	}
	return &CHStore{conn: conn}, nil
}

func (s *CHStore) Close() error {
	return s.Close()
}

func (s *CHStore) Init() error {
	for _, stmt := range chschema {
		_, err := s.conn.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *CHStore) Clear() error {
	stmts := []string{"DELETE FROM filenames", "DELETE FROM individual", "DELETE FROM tuples"}
	for _, stmt := range stmts {
		_, err := s.conn.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *CHStore) Begin() error {
	return fmt.Errorf("clickhouse doesn't support transactions")
}
func (s *CHStore) Commit() error {
	return fmt.Errorf("clickhouse doesn't support transactions")
}

//DeleteOld Deletes records that haven't been seen in DAYS, returns the total records deleted
func (s *CHStore) DeleteOld(days int64) (int64, error) {
	return 0, fmt.Errorf("clickhouse doesn't support delete")
}

func (s *CHStore) Update(ar aggregationResult) (UpdateResult, error) {
	var result UpdateResult
	start := time.Now()

	s.conn.Exec("DROP TABLE tuples_temp")
	_, err := s.conn.Exec(tuples_temp_stmt)
	if err != nil {
		return result, errors.Wrap(err, "CHStore.Update failed to create temporary tuples table")
	}
	defer func() {
		s.conn.Exec("DROP TABLE tuples_temp")
	}()

	tx, err := s.conn.Begin()
	if err != nil {
		return result, err
	}

	stmt_tuples, err := tx.Prepare("INSERT INTO tuples_temp (query, type, answer, ttl, first, last, count) VALUES (?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return result, err
	}
	for _, q := range ar.Tuples {
		query := Reverse(q.query)
		ttl, err := strconv.ParseInt(q.ttl, 10, 64)
		if err != nil {
			return result, errors.Wrap(err, "CHStore.Update failed")
		}
		if _, err := stmt_tuples.Exec(query, q.qtype, q.answer, ttl, int64(q.first), int64(q.last), int64(q.count)); err != nil {
			return result, errors.Wrap(err, "CHStore.Update failed")
		}

	}

	err = tx.Commit()
	if err != nil {
		return result, errors.Wrap(err, "CHStore.Update failed")
	}

	_, err = s.conn.Query(`INSERT INTO tuples (query, type, answer, ttl, first, last, count) SELECT query, type, answer, anyLastState(ttl), minState(first), maxState(last), countState(count) from tuples_temp group by query, type, answer`)
	if err != nil {
		//return result, errors.Wrap(err, "CHStore.Update failed to insert into tuples")
	}

	result.Duration = time.Since(start)
	return result, nil
}

func (s *CHStore) IsLogIndexed(filename string) (bool, error) {
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
func (s *CHStore) SetLogIndexed(filename string, ar aggregationResult, ur UpdateResult) error {
	tx, _ := s.conn.Begin()
	q := `INSERT INTO filenames (filename,
	      aggregation_time, total_records, skipped_records, tuples, individual,
	      store_time, inserted, updated)
	      VALUES (?,?,?,?,?,?,?,?,?)`
	_, err := tx.Exec(q, filename,
		ar.Duration.Seconds(), uint64(ar.TotalRecords), uint64(ar.SkippedRecords), len(ar.Tuples), len(ar.Individual),
		ur.Duration.Seconds(), uint64(ur.Inserted), uint64(ur.Updated))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *CHStore) FindQueryTuples(query string) (tupleResults, error) {
	tr := []tupleResult{}
	query = Reverse(query)
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query = ?", query)
	reverseQuery(tr)
	return tr, err
}
func (s *CHStore) FindTuples(query string) (tupleResults, error) {
	tr := []tupleResult{}
	rquery := Reverse(query)
	err := s.conn.Select(&tr, "SELECT query, type, answer, minMerge(first) as first, maxMerge(last) as last, countMerge(count) as count from tuples WHERE query = ? OR answer = ? group by query, type, answer ORDER BY query, answer", rquery, query)
	reverseQuery(tr)

	return tr, err
}
func (s *CHStore) LikeTuples(query string) (tupleResults, error) {
	tr := []tupleResult{}
	rquery := Reverse(query)
	err := s.conn.Select(&tr, "SELECT * FROM tuples WHERE query like ? OR answer like ? ORDER BY query, answer", rquery+"%", query+"%")
	reverseQuery(tr)
	return tr, err
}
func (s *CHStore) FindIndividual(value string) (individualResults, error) {
	rvalue := Reverse(value)
	tr := []individualResult{}
	err := s.conn.Select(&tr, `SELECT which, value, minMerge(first) as first, maxMerge(last) as last, countMerge(count) as count from individual
	WHERE (which='A' AND value = ?) OR (which='Q' AND value = ?) group by which, value ORDER BY value`, value, rvalue)
	reverseValue(tr)
	return tr, err
}

func (s *CHStore) LikeIndividual(value string) (individualResults, error) {
	rvalue := Reverse(value)
	tr := []individualResult{}
	err := s.conn.Select(&tr, "SELECT * FROM individual WHERE (which='A' AND value like ?) OR (which='Q' AND value like ?) ORDER BY value", value+"%", rvalue+"%")
	reverseValue(tr)
	return tr, err
}
