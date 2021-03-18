package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
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
    count AggregateFunction(sum, UInt64)
  ) ENGINE = AggregatingMergeTree(whatever, (query, type, answer), 8192);
`,

	`
CREATE TABLE IF NOT EXISTS individual (
    whatever Date DEFAULT '2000-01-01',
    which Enum8('Q'=0, 'A'=1),
    value String,
    first AggregateFunction(min, DateTime),
    last AggregateFunction(max, DateTime),
    count AggregateFunction(sum, UInt64)
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
    ttl String,
    first String,
    last String,
    count UInt64
) ENGINE = Log`

const individual_temp_stmt = `
CREATE TABLE individual_temp (
    which Enum8('Q'=0, 'A'=1),
    value String,
    first String,
    last String,
    count UInt64
) ENGINE = Log`

type CHStore struct {
	conn *sqlx.DB
}

func NewCHStore(uri string) (Store, error) {
	_, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	conn, err := sqlx.Open("clickhouse", uri)
	if err != nil {
		return nil, err
	}
	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	return &CHStore{
		conn: conn,
	}, nil
}

func (s *CHStore) Close() error {
	return s.Close()
}
func (s *CHStore) Exec(stmt string) error {
	_, err := s.conn.Exec(stmt)
	return err
}

func (s *CHStore) Init() error {
	for _, stmt := range chschema {
		err := s.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *CHStore) Clear() error {
	stmts := []string{
		"drop table filenames",
		"drop table individual",
		"drop table tuples",
	}
	for _, stmt := range stmts {
		err := s.Exec(stmt)
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
	//log.Printf("clickhouse doesn't support transactions")
	return nil
}

//DeleteOld Deletes records that haven't been seen in DAYS, returns the total records deleted
func (s *CHStore) DeleteOld(days int64) (int64, error) {
	return 0, fmt.Errorf("clickhouse doesn't support delete")
}

func (s *CHStore) Update(ar aggregationResult) (UpdateResult, error) {
	var result UpdateResult
	var err error
	start := time.Now()

	s.Exec("DROP TABLE tuples_temp")
	s.Exec("DROP TABLE individual_temp")

	err = s.Exec(tuples_temp_stmt)
	if err != nil {
		return result, fmt.Errorf("CHStore.Update failed: %w", err)
	}
	err = s.Exec(individual_temp_stmt)
	if err != nil {
		return result, fmt.Errorf("CHStore.Update failed: %w", err)
	}

	tx, err := s.conn.Begin()
	if err != nil {
		return result, fmt.Errorf("CHStore.Update failed: %w", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO tuples_temp
		(query, type, answer, ttl, first, last, count)
		values (?,?,?,?,?,?,?)`,
	)
	if err != nil {
		return result, fmt.Errorf("CHStore.Update failed: %w", err)
	}
	// Ok, now let's update stuff
	// tuples
	for _, q := range ar.Tuples {
		//Update the tuples table
		query := Reverse(q.query)
		_, err := stmt.Exec(query, q.qtype, q.answer, q.ttl, ToTS(q.first), ToTS(q.last), uint64(q.count))
		if err != nil {
			return result, fmt.Errorf("CHStore.Update failed to run query: %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return result, fmt.Errorf("CHStore.Update failed: %w", err)
	}
	err = s.Exec(`INSERT INTO tuples (query, type, answer, ttl, first, last, count) SELECT
		query, type, answer,
		anyLastState(toUInt16(ttl)),
		minState(toDateTime(toFloat64(first))),
		maxState(toDateTime(toFloat64(last))),
		sumState(count) from tuples_temp group by query, type, answer`,
	)
	if err != nil {
		return result, fmt.Errorf("CHStore.Update failed: %w", err)
	}

	tx, err = s.conn.Begin()
	if err != nil {
		return result, fmt.Errorf("CHStore.Update failed: %w", err)
	}
	// Individuals
	stmt, err = tx.Prepare(`INSERT INTO individual_temp
		(value, which, first, last, count)
		values (?,?,?,?,?)`,
	)
	if err != nil {
		return result, fmt.Errorf("CHStore.Update failed: %w", err)
	}
	for _, q := range ar.Individual {
		//Update the tuples table
		value := q.value
		if q.which == "Q" {
			value = Reverse(value)
		}
		_, err := stmt.Exec(value, q.which, ToTS(q.first), ToTS(q.last), uint64(q.count))
		if err != nil {
			return result, fmt.Errorf("CHStore.Update failed to run query: %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return result, fmt.Errorf("CHStore.Update failed: %w", err)
	}
	err = s.Exec(`INSERT INTO individual (which, value, first, last, count) SELECT which, value,
	minState(toDateTime(toFloat64(first))),
	maxState(toDateTime(toFloat64(last))),
	sumState(count) from individual_temp group by which, value`)
	if err != nil {
		return result, fmt.Errorf("CHStore.Update failed: %w", err)
	}

	result.Updated = uint(ar.TuplesLen + ar.IndividualLen)
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
	err := s.conn.Select(&tr, "SELECT query, type, answer, anyLastMerge(ttl) as ttl, minMerge(first) as first, maxMerge(last) as last, sumMerge(count) as count from tuples WHERE query = ? OR answer = ? group by query, type, answer ORDER BY query, answer", rquery, query)
	reverseQuery(tr)

	return tr, err
}
func (s *CHStore) LikeTuples(query string) (tupleResults, error) {
	tr := []tupleResult{}
	rquery := Reverse(query)
	err := s.conn.Select(&tr, "SELECT query, type, answer, anyLastMerge(ttl) as ttl, minMerge(first) as first, maxMerge(last) as last, sumMerge(count) as count from tuples WHERE query like ? OR answer like ? group by query, type, answer ORDER BY query, answer", rquery+"%", query+"%")
	reverseQuery(tr)
	return tr, err
}
func (s *CHStore) FindIndividual(value string) (individualResults, error) {
	rvalue := Reverse(value)
	tr := []individualResult{}
	err := s.conn.Select(&tr, `SELECT which, value, minMerge(first) as first, maxMerge(last) as last, sumMerge(count) as count from individual WHERE (which='A' AND value = ?) OR (which='Q' AND value = ?) group by which, value ORDER BY value`, value, rvalue)
	reverseValue(tr)
	return tr, err
}

func (s *CHStore) LikeIndividual(value string) (individualResults, error) {
	rvalue := Reverse(value)
	tr := []individualResult{}
	err := s.conn.Select(&tr, `SELECT which, value, minMerge(first) as first, maxMerge(last) as last, sumMerge(count) as count from individual WHERE (which='A' AND value like ?) OR (which='Q' AND value like ?) group by which, value ORDER BY value`, value+"%", rvalue+"%")
	reverseValue(tr)
	return tr, err
}
