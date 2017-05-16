package main

import (
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/lib/pq"
)

const pgschema = `
set synchronous_commit to off;
CREATE TABLE IF NOT EXISTS tuples (
	query text,
	type text,
	answer text,
	count bigint,
	ttl integer,
	first timestamp,
	last timestamp,
	PRIMARY KEY (query, type, answer)
) ;

CREATE TABLE IF NOT EXISTS individual (
	which char(1),
	value text,
	count bigint,
	first timestamp,
	last timestamp,
	PRIMARY KEY (which, value)
);
CREATE TABLE IF NOT EXISTS filenames (
	filename text PRIMARY KEY UNIQUE NOT NULL,
	time timestamp DEFAULT now(),
	aggregation_time real,
	total_records int,
	skipped_records int,
	tuples int,
	individual int,
	store_time real,
	inserted int,
	updated int
);
CREATE OR REPLACE FUNCTION update_individual(w char(1), v text, c bigint, f timestamp,l timestamp) RETURNS CHAR(1) AS
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


CREATE OR REPLACE FUNCTION update_tuples(q text, ty text, a text, tt integer, c bigint, f timestamp,l timestamp) RETURNS CHAR(1) AS
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

CREATE OR REPLACE FUNCTION load_tuples(OUT inserted bigint, OUT updated bigint) AS $$
DECLARE
    rec RECORD;
    upsert_query text;
    result char;
BEGIN
    inserted = 0;
    updated = 0;
    upsert_query := 'select update_tuples($1, $2, $3, $4, $5, $6, $7)';
    FOR rec IN SELECT query, type, answer, count, ttl, first, last FROM tuples_staging
    LOOP
        execute upsert_query into result USING rec.query, rec.type, rec.answer, rec.ttl, rec.count, to_timestamp(rec.first)::timestamp, to_timestamp(rec.last)::timestamp;
        IF result = 'I' THEN
            inserted = inserted + 1;
        ELSE
            updated = updated + 1;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION load_individual(OUT inserted bigint, OUT updated bigint) AS $$
DECLARE
    rec RECORD;
    upsert_query text;
    result char;
BEGIN
    inserted = 0;
    updated = 0;
    upsert_query := 'select update_individual($1, $2, $3, $4, $5)';
    FOR rec IN SELECT which, value, count, first, last FROM individual_staging
    LOOP
        execute upsert_query into result USING rec.which, rec.value, rec.count, to_timestamp(rec.first)::timestamp, to_timestamp(rec.last)::timestamp;
        IF result = 'I' THEN
            inserted = inserted + 1;
        ELSE
            updated = updated + 1;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
`

var x = `

CREATE INDEX tuples_query ON tuples(query varchar_pattern_ops);
CREATE INDEX tuples_answer ON tuples(answer varchar_pattern_ops);
-- CREATE INDEX tuples_first ON tuples(first);
-- CREATE INDEX tuples_last ON tuples(last);
CREATE INDEX individual_value ON individual(value varchar_pattern_ops);
-- CREATE INDEX individual_first ON individual(first);
-- CREATE INDEX individual_last ON individual(last);
`

const pgschemaTempTables = `
DROP TABLE IF EXISTS individual_staging;
CREATE TEMPORARY TABLE individual_staging (
	which char(1),
	value text,
	count bigint,
	first double precision,
	last double precision
);
DROP TABLE IF EXISTS tuples_staging;
CREATE TEMPORARY TABLE tuples_staging (
	query text,
	type text,
	answer text,
	count bigint,
	ttl integer,
	first double precision,
	last double precision
);`

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
	// Ignore a duplicte table error message
	if pqerr, ok := err.(*pq.Error); ok {
		if pqerr.Code == "42P07" {
			return nil
		}
	}

	return err
}

func (s *PGStore) Update(ar aggregationResult) (UpdateResult, error) {
	var result UpdateResult
	var resultT UpdateResult
	var resultI UpdateResult
	start := time.Now()

	tx, err := s.BeginTx()
	if err != nil {
		return result, err
	}
	//Setup
	_, err = tx.Exec(pgschemaTempTables)
	if err != nil {
		return result, err
	}

	//Update tuples
	stmt, err := tx.Prepare(pq.CopyIn("tuples_staging", "query", "type", "answer", "count", "ttl", "first", "last"))
	if err != nil {
		return result, err
	}

	for _, q := range ar.Tuples {
		query := Reverse(q.query)
		_, err = stmt.Exec(query, q.qtype, q.answer, q.count, q.ttl, q.first, q.last)
		if err != nil {
			return result, err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return result, err
	}

	err = stmt.Close()
	if err != nil {
		return result, err
	}
	err = tx.QueryRowx("SELECT inserted, updated from load_tuples()").StructScan(&resultT)
	if err != nil {
		return result, err
	}

	//Update individual
	stmt, err = tx.Prepare(pq.CopyIn("individual_staging", "which", "value", "count", "first", "last"))
	if err != nil {
		return result, err
	}

	for _, q := range ar.Individual {
		value := q.value
		if q.which == "Q" {
			value = Reverse(value)
		}
		_, err = stmt.Exec(q.which, value, q.count, q.first, q.last)
		if err != nil {
			return result, err
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return result, err
	}

	err = stmt.Close()
	if err != nil {
		return result, err
	}
	err = tx.QueryRowx("SELECT inserted, updated from load_individual()").StructScan(&resultI)
	if err != nil {
		return result, err
	}

	result.Inserted = resultT.Inserted + resultI.Inserted
	result.Updated = resultT.Updated + resultI.Updated
	result.Duration = time.Since(start)
	return result, s.Commit()
}
