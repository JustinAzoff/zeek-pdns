#!/usr/bin/env python
from collections import defaultdict
import sys
import datetime
import sqlite3

def reader(f):
    line = ''
    headers = {}
    it = iter(f)
    while not line.startswith("#types"):
        line = next(it).rstrip()
        k,v = line[1:].split(None, 1)
        headers[k] = v

    sep = headers['separator'].decode("string-escape")

    for k,v in headers.items():
        if sep in v:
            headers[k] = v.split(sep)

    headers['separator'] = sep
    fields = headers['fields']
    types = headers['types']
    set_sep = headers['set_separator']

    vectors = [field for field, type in zip(fields, types) if type.startswith("vector[")]

    for row in it:
        if row.startswith("#close"): break
        parts = row.rstrip().split(sep)
        rec = dict(zip(fields, parts))
        for f in vectors:
            rec[f] = rec[f].split(set_sep)
        yield rec

ts = datetime.datetime.fromtimestamp

class SqliteStore:
    def __init__(self):
        conn = sqlite3.connect("/bro/logs/dns.db")
        c = conn.cursor()

        # Create table
        try :
            c.execute('''CREATE TABLE dns (
                query text,
                type text,
                answer text,
                count UNSIGNED BIG INT,
                ttl INT,
                first text,
                last text)''')

            c.execute('''Create unique index if not exists record on dns(query,type,answer)''')
            c.execute('''CREATE INDEX if not exists idx_answer on dns(answer)''')
            c.execute('''CREATE INDEX if not exists idx_query on dns(query)''')
            conn.commit()
        except sqlite3.OperationalError:
            pass
        self.conn = conn
        self.c = conn.cursor()

    def upsert_record(self, q,t,a,ttl,time,count):
        c = self.c
        n = ts(float(time)).strftime("%Y-%m-%d %H:%M:%S")
        ttl = ttl != "-" and int(float(ttl)) or None
        c.execute("update dns set last=?, ttl=?, count=count+? where query=? and type=? and answer=?", (n, ttl, count, q,t,a))
        if c.rowcount:
            return
        c.execute("insert into dns (query, type, answer, ttl, count, first, last) VALUES (?, ?, ?, ?, ?, ?, ?)", (q, t, a, ttl, count, n, n))

        #possibly do what http://stackoverflow.com/questions/418898/sqlite-upsert-not-insert-or-replace shows

    def begin(self):
        self.c.execute("begin");

    def commit(self):
        self.conn.commit()

def process(f):
    pairs = defaultdict(int)
    ttls = {}
    times = {}
    for rec in reader(open(f)):
        #print "process", rec['query'], rec['qtype_name'], rec['answers']
        q = rec['ans_query'][0] #this is a vector right now..
        t = rec['qtype_name']
        for a, ttl in zip(rec['answers'], rec['TTLs']):
            tup = (q,t,a)
            pairs[tup] += 1
            ttls[tup] = ttl
            times[tup] = rec["ts"]

    store = SqliteStore()
    store.begin()

    for tup, count in pairs.iteritems():
        (q,t,a) = tup
        #print "q=%s t=%s a=%s c=%s" % (q,t,a,count)
        ttl = ttls[tup]
        time = times[tup]
        store.upsert_record(q,t,a,ttl,time,count)
    store.commit()
    print "processed %d records" % len(pairs)

def main():
    f = sys.argv[1]
    process(f)

if __name__ == "__main__":
    main()
