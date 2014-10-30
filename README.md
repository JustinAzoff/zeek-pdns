Passive DNS for Bro
===================

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
