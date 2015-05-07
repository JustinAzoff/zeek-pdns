Passive DNS for Bro
===================

This is an extremely simple implementation of a passive DNS collection system
that utilizes Bro for DNS log collection.

Passive DNS collection can be used for various security or troubleshooting
purposes.  Many queries to raw DNS logs can be done faster by using 
the aggregated data in the passive DNS database, which is more compact.

This tool uses the Bro DNS logs to build a database of unique query+type+answer
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

* Bro 2.x
* Python >= 2.6
  * Bottle
  * SQLAlchemy
* An SQL database supported by SQLAlchemy.  SQLite works, but is not recommended.

Usage
-----

In local.bro:

    @load ./bro-pdns

    #any URI supported by sqlalchemy 
    #see http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html
    # i.e. redef PDNS::uri = "postgres://pdns:password@dbhost/pdns";
    redef PDNS::uri = "sqlite:////tmp/dns.db";

To run the http api server:

    $ BRO_PDNS_DB=sqlite:////tmp/dns.db /path/to/bro_pdns.py serve

Usage:

    $ curl http://localhost:8081/dns/1.2.3.4
