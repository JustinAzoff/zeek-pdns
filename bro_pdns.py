#!/usr/bin/env python
from collections import defaultdict
import sys
import datetime
from sqlalchemy import create_engine

from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime
metadata = MetaData()

dns_table = Table('dns', metadata,
    Column('query', String, primary_key=True, index=True),
    Column('type', String, primary_key=True),
    Column('answer', String, primary_key=True, index=True),
    Column('count', Integer),
    Column('ttl', Integer),
    Column('first', DateTime),
    Column('last', DateTime),
)


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

class SQLStore:
    def __init__(self):
        self.engine = engine = create_engine("sqlite:///dns.db")
        metadata.create_all(engine)
        self.conn = engine.connect()

        self._select = dns_table.select()
        self._insert = dns_table.insert()
        self._update = dns_table.update()

    def upsert_record(self, query, type, answer, ttl, time,count):
        d = dns_table.c
        n = ts(float(time))
        ttl = ttl != "-" and int(float(ttl)) or None
        q = self._update.where( (d.query == query) & (d.type == type) & (d.answer == answer)).values(
            count=d.count+1,
            last=n,
            ttl=ttl
        )
        ret = self.conn.execute(q)
        if ret.rowcount:
            return
        self.conn.execute(self._insert.values(query=query, type=type, answer=answer, ttl=ttl, count=count, first=n, last=n))

    def begin(self):
        self._trans = self.conn.begin()

    def commit(self):
        self._trans.commit()

    def search(self, q):
        d = dns_table.c
        records = self.engine.execute(
            self._select.where((d.query == q) | (d.answer == q))
        ).fetchall()
        if records:
            return records

        q = '%' + q + '%'
        records = self.engine.execute(
            self._select.where(d.query.like(q) | d.answer.like(q))
        ).fetchall()
        return records


def aggregate_file(f):
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


    for tup, count in pairs.iteritems():
        (q,t,a) = tup
        #print "q=%s t=%s a=%s c=%s" % (q,t,a,count)
        ttl = ttls[tup]
        time = times[tup]
        yield {
            "query": q,
            "type": t,
            "answer": a,
            "ttl": ttl,
            "time": time,
            "count": count,
        }

def process_fn(f):
    store = SQLStore()
    store.begin()

    for n, rec in enumerate(aggregate_file(f)):
        store.upsert_record(**rec)

    store.commit()
    print "processed %d records" % n

def process():
    f = sys.argv[2]
    process_fn(f)

#web stuff
from bottle import route, run, template, Bottle

def fixup(record):
    r = dict(record)
    for x in 'first', 'last':
        r[x] = str(r[x])
    return r

app = Bottle()
@app.route('/dns/<q>')
def dns_search(q):
    records = app.db.search(q)
    records = map(fixup, records)
    return { "records": records }

def serve():
    app.db = SQLStore()
    app.run(host='0.0.0.0', port=8081)

MAPPING = {
    "process": process,
    "serve": serve,
}

if __name__ == "__main__":
    try :
        action = sys.argv[1]
        func = MAPPING[action]
    except (IndexError, KeyError):
        print "Usage: %s [process foo.log] | [serve]" % sys.argv[0]
        sys.exit(1)

    func()
