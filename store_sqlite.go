package main

import (
	"database/sql"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

const schema = `
CREATE TABLE IF NOT EXISTS tuples (
	query character varying,
	type character varying,
	answer character varying,
	count integer,
	ttl integer,
	first integer,
	last integer,
	PRIMARY KEY (query, type, answer)
) ;
CREATE INDEX IF NOT EXISTS tuples_query ON tuples(query);
CREATE INDEX IF NOT EXISTS tuples_answer ON tuples(answer);
CREATE INDEX IF NOT EXISTS tuples_first ON tuples(first);
CREATE INDEX IF NOT EXISTS tuples_last ON tuples(last);

CREATE TABLE IF NOT EXISTS individual (
	value character varying PRIMARY KEY UNIQUE NOT NULL,
	count integer,
	first integer,
	last integer
);
CREATE INDEX IF NOT EXISTS individual_first ON individual(first);
CREATE INDEX IF NOT EXISTS individual_last ON individual(last);
`

type SQLiteStore struct {
	conn *sql.DB
}

func NewSQLiteStore(uri string) (Store, error) {
	conn, err := sql.Open("sqlite3", uri)
	if err != nil {
		return nil, err
	}
	return &SQLiteStore{conn: conn}, nil
}

func (s *SQLiteStore) Close() error {
	return s.Close()
}

func (s *SQLiteStore) Init() error {
	_, err := s.conn.Exec(schema)
	return err
}

func (s *SQLiteStore) Update(records []aggregationResult) error {
	tx, err := s.conn.Begin()
	if err != nil {
		return err
	}
	update_tuples, err := tx.Prepare(`UPDATE tuples SET count=?, ttl=?, last=? WHERE query=? AND type=? AND answer=?`)
	if err != nil {
		return err
	}
	defer update_tuples.Close()
	insert_tuples, err := tx.Prepare(`INSERT INTO tuples (query, type, answer, ttl, count, first, last) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer insert_tuples.Close()

	update_individual, err := tx.Prepare(`UPDATE individual SET count=?, last=? WHERE value=?`)
	if err != nil {
		return err
	}
	defer update_individual.Close()
	insert_individual, err := tx.Prepare(`INSERT INTO individual (value, count, first, last) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer insert_individual.Close()

	var inserts, updates uint64
	for _, q := range records {
		//fmt.Printf("%-8d %-30s %-4s %-30s %s %s %s\n", q.count, q.query, q.qtype, q.answer, q.ttl, q.first, q.last)
		//Update the tuples table
		res, err := update_tuples.Exec(q.count, q.ttl, q.last, q.query, q.qtype, q.answer)
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			_, err := insert_tuples.Exec(q.query, q.qtype, q.answer, q.count, q.ttl, q.first, q.last)
			if err != nil {
				return err
			}
			inserts++
		} else {
			updates++
		}
		//Update the invidual table for each of query and answer
		for _, value := range []string{q.query, q.answer} {
			res, err := update_individual.Exec(q.count, q.last, value)
			if err != nil {
				return err
			}
			rows, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if rows == 0 {
				_, err := insert_individual.Exec(value, q.count, q.first, q.last)
				if err != nil {
					return err
				}
				inserts++
			} else {
				updates++
			}
		}
	}
	log.Printf("Inserts=%d Updates=%d", inserts, updates)
	return tx.Commit()
}
