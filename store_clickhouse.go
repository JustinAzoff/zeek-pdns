package main

import (
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
)

var chschema = []string{
	`
CREATE TABLE IF NOT EXISTS tuples (
    whatever Date DEFAULT '2000-01-01',
    query String,
    answer String,
    type Enum8('Q'=0, 'A'=1),
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

type CHStore struct {
	conn *sqlx.DB
	*SQLCommonStore
}

func NewCHStore(uri string) (Store, error) {
	conn, err := sqlx.Open("clickhouse", uri)
	if err != nil {
		return nil, err
	}
	common := &SQLCommonStore{conn: conn}
	return &CHStore{conn: conn, SQLCommonStore: common}, nil
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

func (s *CHStore) Update(ar aggregationResult) (UpdateResult, error) {
	var result UpdateResult
	start := time.Now()

	_, err := s.BeginTx()
	if err != nil {
		return result, err
	}
	result.Duration = time.Since(start)
	return result, s.Commit()
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
