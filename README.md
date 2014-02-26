Usage
=====

in local.bro:

    @load ./passive-dns
    redef PDNS::tool = "/path/to/bro_pdns.py";

    #any URI supported by sqlalchemy 
    #see http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html
    redef PDNS::uri = "sqlite:////tmp/dns.db";

to run the http api server:

    BRO_PDNS_DB=sqlite:////tmp/dns.db /path/to/bro_pdns.py serve

Usage:

    curl localhost:8081/dns/1.2.3.4
