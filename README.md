Passive DNS for Bro
===================

This is an extremely simple implementation of a passive dns collection system that utilizes Bro for the DNS log collection.

Passive DNS collection can be used for various security or troubleshooting related purposes.  Likely many things that you would currently search the raw DNS logs for, you could do faster by using the aggregated data in the passive DNS database.

This tool uses the Bro DNS logs to build a database of unique query+type+answer tuples.  This database is much more compact than the raw DNS logs, and querying it is much faster.

It produces a table like this:

    pdns=# select * from dns where answer='74.125.225.18' order by last desc limit 4;
          query       | type |    answer     | count | ttl |     first     |    last
    ------------------+------+---------------+-------+-----+---------------+------------
     www.google.com   | A    | 74.125.225.18 |  7517 | 198 | 2014-09-03 .. | 2014-10-30 ..
     t0.gstatic.com   | A    | 74.125.225.18 |   266 | 300 | 2014-09-03 .. | 2014-10-30 ..
     googlegroups.com | A    | 74.125.225.18 |   266 | 300 | 2014-09-03 .. | 2014-10-30 ..
     t3.gstatic.com   | A    | 74.125.225.18 |   291 | 300 | 2014-09-03 .. | 2014-10-30 ..

Requirements
------------

* Bro 2.x
* Python >= 2.6
  * bottle
  * sqlalchemy
* A sql database supported by sqlalchemy.  SQLite works, but not recommended.

Usage
-----

in local.bro:

    @load ./passive-dns

    #any URI supported by sqlalchemy 
    #see http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html
    redef PDNS::uri = "sqlite:////tmp/dns.db";

to run the http api server:

    $ BRO_PDNS_DB=sqlite:////tmp/dns.db /path/to/bro_pdns.py serve

Usage:

    $ curl http://localhost:8081/dns/1.2.3.4
