package main

import (
	"time"

	"github.com/jmoiron/sqlx"

	_ "github.com/mattn/go-sqlite3"
)

const schema = `
CREATE TABLE IF NOT EXISTS tuples (
	query character varying,
	type character varying,
	answer character varying,
	count integer,
	ttl integer,
	first REAL,
	last REAL,
	PRIMARY KEY (query, type, answer)
) ;
CREATE INDEX IF NOT EXISTS tuples_query ON tuples(query);
CREATE INDEX IF NOT EXISTS tuples_answer ON tuples(answer);
CREATE INDEX IF NOT EXISTS tuples_first ON tuples(first);
CREATE INDEX IF NOT EXISTS tuples_last ON tuples(last);

CREATE TABLE IF NOT EXISTS individual (
	which char(1),
	value character varying,
	count integer,
	first REAL,
	last REAL,
	PRIMARY KEY (which, value)
);
CREATE INDEX IF NOT EXISTS individual_first ON individual(first);
CREATE INDEX IF NOT EXISTS individual_last ON individual(last);

CREATE TABLE IF NOT EXISTS filenames (
	filename character varying PRIMARY KEY UNIQUE NOT NULL,
	time REAL DEFAULT (datetime('now', 'localtime')),
	aggregation_time real,
	total_records int,
	skipped_records int,
	tuples int,
	individual int,
	store_time real,
	inserted int,
	updated int
);
PRAGMA case_sensitive_like=ON;
-- PRAGMA journal_mode=WAL;
-- PRAGMA synchronous=off;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 5000;
`

type SQLiteStore struct {
	conn *sqlx.DB
	*SQLCommonStore
}

func NewSQLiteStore(uri string) (Store, error) {
	conn, err := sqlx.Open("sqlite3", uri)
	if err != nil {
		return nil, err
	}
	common := &SQLCommonStore{conn: conn}
	return &SQLiteStore{conn: conn, SQLCommonStore: common}, nil
}

func (s *SQLiteStore) Close() error {
	return s.Close()
}

func (s *SQLiteStore) Init() error {
	_, err := s.conn.Exec(schema)
	return err
}

func (s *SQLiteStore) Update(ar aggregationResult) (UpdateResult, error) {
	var result UpdateResult
	start := time.Now()

	tx, err := s.BeginTx()
	if err != nil {
		return result, err
	}
	//Setup the 4 different prepared statements
	update_tuples, err := tx.Prepare(`UPDATE tuples SET
		count=count+$1,
		ttl=$2,
		first=min(datetime($3, 'unixepoch'), first),
		last =max(datetime($4, 'unixepoch'), last)
		WHERE query=$5 AND type=$6 AND answer=$7`)
	if err != nil {
		return result, err
	}
	defer update_tuples.Close()
	insert_tuples, err := tx.Prepare(`INSERT INTO tuples (query, type, answer, ttl, count, first, last)
	    VALUES ($1, $2, $3, $4, $5, datetime($6, 'unixepoch'), datetime($7,'unixepoch'))`)
	if err != nil {
		return result, err
	}
	defer insert_tuples.Close()

	update_individual, err := tx.Prepare(`UPDATE individual SET
		count=count+$1,
		first=min(datetime($2, 'unixepoch'), first),
		last =max(datetime($3, 'unixepoch'), last)
		WHERE value=$4 AND which=$5`)
	if err != nil {
		return result, err
	}
	defer update_individual.Close()
	insert_individual, err := tx.Prepare(`INSERT INTO individual (value, which, count, first, last)
	    VALUES ($1, $2, $3, datetime($4, 'unixepoch'), datetime($5,'unixepoch'))`)
	if err != nil {
		return result, err
	}
	defer insert_individual.Close()

	// Ok, now let's update stuff
	for _, q := range ar.Tuples {
		//Update the tuples table
		query := Reverse(q.query)
		res, err := update_tuples.Exec(q.count, q.ttl, q.first, q.last, query, q.qtype, q.answer)
		if err != nil {
			return result, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return result, err
		}
		if rows == 0 {
			_, err := insert_tuples.Exec(query, q.qtype, q.answer, q.ttl, q.count, q.first, q.last)
			if err != nil {
				return result, err
			}
			result.Inserted++
		} else {
			result.Updated++
		}
	}
	for _, q := range ar.Individual {
		value := q.value
		if q.which == "Q" {
			value = Reverse(value)
		}
		res, err := update_individual.Exec(q.count, q.first, q.last, value, q.which)
		if err != nil {
			return result, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return result, err
		}
		if rows == 0 {
			_, err := insert_individual.Exec(value, q.which, q.count, q.first, q.last)
			if err != nil {
				return result, err
			}
			result.Inserted++
		} else {
			result.Updated++
		}
	}
	result.Duration = time.Since(start)
	return result, s.Commit()
}
