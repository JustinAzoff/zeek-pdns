package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
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

func genFullBatchSelect(tmpl string, batchSize int) string {
	var queries []string
	numParams := strings.Count(tmpl, "$")
	arg := 1
	for i := 0; i < batchSize; i++ {
		var args []interface{}
		for p := 0; p < numParams; p++ {
			args = append(args, arg)
			arg++
		}
		queries = append(queries, fmt.Sprintf(tmpl, args...))
	}
	fullq := fmt.Sprintf("SELECT %s", strings.Join(queries, " || "))
	return fullq
}

var BATCHSIZE = 50

func (s *PGStore) Update(ar aggregationResult) (UpdateResult, error) {
	var result UpdateResult
	start := time.Now()

	tx, err := s.conn.Begin()
	if err != nil {
		return result, err
	}
	//Setup the 2 different prepared statements
	updateTupleTmpl := "update_tuples($%d, $%d, $%d, $%d, $%d, to_timestamp($%d)::timestamp, to_timestamp($%d)::timestamp)"
	updateTupleBatch, err := tx.Prepare(genFullBatchSelect(updateTupleTmpl, BATCHSIZE))
	if err != nil {
		return result, err
	}
	defer updateTupleBatch.Close()

	updateIndividualTmpl := "update_individual($%d, $%d, $%d, to_timestamp($%d)::timestamp, to_timestamp($%d)::timestamp)"
	updateIndividualeBatch, err := tx.Prepare(genFullBatchSelect(updateIndividualTmpl, BATCHSIZE))
	if err != nil {
		return result, err
	}
	defer updateIndividualeBatch.Close()

	var arguments []interface{}
	batchCounter := 0

	runBatch := func(tmpl string, preparedBatch *sql.Stmt, arguments []interface{}, batchSize int) {
		if batchSize == 0 {
			return
		}
		var stmt *sql.Stmt
		if batchSize == BATCHSIZE {
			stmt = preparedBatch
		} else {
			stmt, err = tx.Prepare(genFullBatchSelect(tmpl, batchSize))
			defer stmt.Close()
		}
		res, err := stmt.Query(arguments...)
		//log.Printf("Fullq is: %s", fullq)
		//log.Printf("Arguments is: %#v", arguments)
		if err != nil {
			log.Fatal(err)
		}
		res.Next()
		var update_result string
		res.Scan(&update_result)
		res.Close()
		for _, ch := range update_result {
			if ch == 'I' {
				result.Inserted++
			} else {
				result.Updated++
			}
		}
	}

	// Ok, now let's update stuff
	for _, q := range ar.Tuples {
		//Update the tuples table
		query := Reverse(q.query)
		arguments = append(arguments, query, q.qtype, q.answer, q.ttl, q.count, q.first, q.last)
		batchCounter++
		if batchCounter == BATCHSIZE {
			runBatch(updateTupleTmpl, updateTupleBatch, arguments, batchCounter)
			arguments = arguments[:0]
			batchCounter = 0
		}
	}
	runBatch(updateTupleTmpl, updateTupleBatch, arguments, batchCounter)
	arguments = arguments[:0]
	batchCounter = 0
	for _, q := range ar.Individual {
		value := q.value
		if q.which == "Q" {
			value = Reverse(value)
		}
		arguments = append(arguments, q.which, value, q.count, q.first, q.last)
		batchCounter++
		if batchCounter == BATCHSIZE {
			runBatch(updateIndividualTmpl, updateIndividualeBatch, arguments, batchCounter)
			arguments = arguments[:0]
			batchCounter = 0
		}
	}
	runBatch(updateIndividualTmpl, updateIndividualeBatch, arguments, batchCounter)
	result.Duration = time.Since(start)
	return result, tx.Commit()
}
