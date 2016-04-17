package main

import (
	"time"

	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
)

const pgschema = `
CREATE TABLE IF NOT EXISTS tuples (
	query text,
	type text,
	answer text,
	count integer,
	ttl integer,
	first timestamp,
	last timestamp,
	PRIMARY KEY (query, type, answer)
) ;
CREATE INDEX IF NOT EXISTS tuples_query ON tuples(query);
CREATE INDEX IF NOT EXISTS tuples_answer ON tuples(answer);
CREATE INDEX IF NOT EXISTS tuples_first ON tuples(first);
CREATE INDEX IF NOT EXISTS tuples_last ON tuples(last);

CREATE TABLE IF NOT EXISTS individual (
	which char(1),
	value text,
	count integer,
	first timestamp,
	last timestamp,
	PRIMARY KEY (which, value)
);
CREATE INDEX IF NOT EXISTS individual_first ON individual(first);
CREATE INDEX IF NOT EXISTS individual_last ON individual(last);

CREATE TABLE IF NOT EXISTS filenames (
	filename text PRIMARY KEY UNIQUE NOT NULL,
	time timestamp DEFAULT now()
);
CREATE OR REPLACE FUNCTION update_individual(w char(1), v text, c integer,f timestamp,l timestamp) RETURNS CHAR(1) AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE individual SET count=count+c,
        first=least(f, first),
        last =greatest(l, last)
        WHERE value=v AND which=w;
        IF found THEN
            RETURN 'U';
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO individual (value, which, count, first, last) VALUES (v,w,c,f,l);
            RETURN 'I';
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_tuples(q text, ty text, a text, tt integer, c integer ,f timestamp,l timestamp) RETURNS CHAR(1) AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE tuples SET count=count+c,
        ttl=tt,
        first=least(f, first),
        last =greatest(l, last)
        WHERE query=q AND  type=ty AND answer=a;
        IF found THEN
            RETURN 'U';
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO tuples (query, type, answer, ttl, count, first, last) VALUES (q, ty, a, tt, c, f, l);
            RETURN 'I';
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

`

type PGStore struct {
	conn *sqlx.DB
	*SQLCommonStore
}

func NewPGStore(uri string) (Store, error) {
	conn, err := sqlx.Open("postgres", uri)
	if err != nil {
		return nil, err
	}
	common := &SQLCommonStore{conn: conn}
	return &PGStore{conn: conn, SQLCommonStore: common}, nil
}

func (s *PGStore) Close() error {
	return s.Close()
}

func (s *PGStore) Init() error {
	_, err := s.conn.Exec(pgschema)
	return err
}

func (s *PGStore) Update(ar aggregationResult) (UpdateResult, error) {
	var result UpdateResult
	start := time.Now()

	tx, err := s.conn.Begin()
	if err != nil {
		return result, err
	}
	//Setup the 2 different prepared statements
	update_tuples, err := tx.Prepare("SELECT update_tuples($1, $2, $3, $4, $5, to_timestamp($6)::timestamp, to_timestamp($7)::timestamp)")
	if err != nil {
		return result, err
	}
	defer update_tuples.Close()

	update_individual, err := tx.Prepare("SELECT update_individual($1, $2, $3, to_timestamp($4)::timestamp, to_timestamp($5)::timestamp)")
	if err != nil {
		return result, err
	}
	defer update_individual.Close()

	// Ok, now let's update stuff
	for _, q := range ar.Tuples {
		//Update the tuples table
		query := Reverse(q.query)
		res, err := update_tuples.Query(query, q.qtype, q.answer, q.ttl, q.count, q.first, q.last)
		if err != nil {
			return result, err
		}
		res.Next()
		var update_result string
		res.Scan(&update_result)
		res.Close()
		if update_result == "I" {
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
		res, err := update_individual.Query(q.which, value, q.count, q.first, q.last)
		if err != nil {
			return result, err
		}
		res.Next()
		var update_result string
		res.Scan(&update_result)
		res.Close()
		if update_result == "I" {
			result.Inserted++
		} else {
			result.Updated++
		}
	}
	result.Duration = time.Since(start)
	return result, tx.Commit()
}
