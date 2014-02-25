#!/usr/bin/env python
import sqlite3

class SqliteStore:
    def __init__(self):
        conn = sqlite3.connect("/bro/logs/dns.db")
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        self.conn = conn
        self.c = conn.cursor()

    def search(self, q):
        c = self.c
        c.execute("select * from dns where answer=? or query=?", (q,q))
        records = c.fetchall()
        if records:
            return records

        q = '%' + q + '%'
        c.execute("select * from dns where answer LIKE ? or query LIKE ?", (q,q))
        records = c.fetchall()
        return records

from bottle import route, run, template

@route('/dns/<q>')
def dns(q):
    db = SqliteStore()
    records = db.search(q)
    records = map(dict, records)
    return { "records": records }

if __name__ == "__main__":
    run(host='0.0.0.0', port=8080)
