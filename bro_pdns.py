#!/usr/bin/env python
from collections import defaultdict
import glob
from multiprocessing.dummy import Pool as thread_pool
import os
import sys
import datetime
import time
from sqlalchemy import create_engine

try:
    import ujson as json
except ImportError:
    import json

from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime
#web
from bottle import route, run, template, Bottle

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
    loads = json.loads
    return map(loads, f)

ts = datetime.datetime.fromtimestamp

class SQLStore:
    def __init__(self, db_uri=None):
        uri = db_uri
        if not uri:
            uri = os.getenv("BRO_PDNS_DB")
        if not uri:
            raise RuntimeError("db_uri is not set. set BRO_PDNS_DB environment variable perhaps?")

        self.engine = engine = create_engine(uri)
        metadata.create_all(engine)
        self.conn = engine.connect()

        self._select = dns_table.select()
        self._insert = dns_table.insert()
        self._update = dns_table.update()

    def close(self):
        self.conn.close()

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
        return records

    def search_full(self, q):
        d = dns_table.c
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
        try :
            q = rec['ans_query'][0] #this is a vector right now..
            t = rec['qtype_name']
            for a, ttl in zip(rec['answers'], rec['TTLs']):
                tup = (q,t,a)
                pairs[tup] += 1
                ttls[tup] = ttl
                times[tup] = rec["ts"]
        except KeyError:
            #something missing, nothing to do...
            pass


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

SIZE_TIMEOUT = 5
def is_growing(f):
    size = os.stat(f).st_size
    time.sleep(0.1)
    for x in range(SIZE_TIMEOUT):
        time.sleep(1)
        newsize = os.stat(f).st_size
        if newsize != size:
            return True
    return False

def window(i,slice=5):
    for x in xrange(0,len(i),slice):
        a=x
        b=x+slice
        yield i[a:b]

def load_records(records):
    store = SQLStore()
    store.begin()

    for rec in records:
        store.upsert_record(**rec)
    store.commit()
    store.close()
    return len(records)

def process_fn(f):
    thread_count = int(os.getenv("BRO_PDNS_THREADS", "1"))
    processed = 0

    aggregated = list(aggregate_file(f))
    batches = window(aggregated, 10000)

    pool = thread_pool(thread_count)

    processed = sum(pool.imap(load_records, batches, chunksize=1))

    print "%d" % processed

def process():
    f = sys.argv[2]
    process_fn(f)

def watch():
    pattern = sys.argv[2]
    while True:
        files = glob.glob(pattern)
        not_growing = (f for f in files if not is_growing(f))
        for fn in not_growing:
            process_fn(fn)
            os.unlink(fn)
        if not files:
            print 'here'
            time.sleep(5)

#api

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

@app.route('/dns/full/<q>')
def dns_search(q):
    records = app.db.search_full(q)
    records = map(fixup, records)
    return { "records": records }

def serve():
    app.db = SQLStore()
    app.run(host='0.0.0.0', port=8081)

MAPPING = {
    "process": process,
    "watch": watch,
    "serve": serve,
}

if __name__ == "__main__":
    try :
        action = sys.argv[1]
        func = MAPPING[action]
    except (IndexError, KeyError):
        print "Usage: %s [process foo.log] | [watch '/path/to/dns*.log'] | [serve]" % sys.argv[0]
        sys.exit(1)

    func()
