Passive DNS for Zeek
===================

This is an extremely simple implementation of a passive DNS collection system
that utilizes Zeek for DNS log collection.

Passive DNS collection can be used for various security or troubleshooting
purposes.  Many queries to raw DNS logs can be done faster by using 
the aggregated data in the passive DNS database, which is more compact.

This tool uses the Zeek DNS logs to build a database of unique query+type+answer
tuples.

It produces a table like this:

    pdns=# select * from dns where answer='74.125.225.18' order by last desc limit 4;
          query       | type |    answer     | count | ttl |     first     |    last
    ------------------+------+---------------+-------+-----+---------------+------------
     www.google.com   | A    | 74.125.225.18 |  7517 | 198 | 2014-09-03 .. | 2014-10-30 ..
     t0.gstatic.com   | A    | 74.125.225.18 |   266 | 300 | 2014-09-03 .. | 2014-10-30 ..
     googlegroups.com | A    | 74.125.225.18 |   266 | 300 | 2014-09-03 .. | 2014-10-30 ..
     t3.gstatic.com   | A    | 74.125.225.18 |   291 | 300 | 2014-09-03 .. | 2014-10-30 ..

This is helpful because the PTR record itself for 74.125.225.18 is ord08s12-in-f18.1e100.net.

Examples of questions this database can answer faster than the raw logs:

 * Did anything ever resolve example.com, and if so, when was the first time?
 * What IPs has example.com resolved to?
 * What other names resolve to this IP?

Requirements
------------

* go compiler ( to build )
* postgresql ( optional )
* clickhouse ( optional )

Build
-----

    $ go build

Index logs
----------

    # for postgresql
    export PDNS_STORE_TYPE="postgresql"
    export PDNS_STORE_URI="postgres://pdns:foo@localhost/pdns?sslmode=disable"

    # for clickhouse
    export PDNS_STORE_TYPE="clickhouse"
    export PDNS_STORE_URI="tcp://localhost:9000/?database=pdns"

    # for built in sqlite
    export PDNS_STORE_TYPE="sqlite"
    export PDNS_STORE_URI="/path/to/passivedns.sqlite"

    # then finally index logs
    find /usr/local/zeek/logs -name 'dns*' | sort -n | xargs -n 50 zeek-pdns index

Query Database
--------------

    # suffix search:
    $ zeek-pdns like tuples google.com
    $ zeek-pdns like individual google.com

    # exact match
    $ zeek-pdns find tuples google.com
    $ zeek-pdns find individual google.com

Start HTTP server
-----------------

    $ zeek-pdns web

Query HTTP API
--------------

    $ curl localhost:8080/dns/like/tuples/google.com
    $ curl localhost:8080/dns/like/individual/google.com
    $ curl localhost:8080/dns/find/tuples/google.com
    $ curl localhost:8080/dns/find/individual/google.com
