# rnx-pgdiff
    -- Rednaxel PostgreSQL Diff Tool - v1.05
    Syntax: rnx_pg_diff -m IP [options]
      -m IP, --master=IP    Master server IP address.
    Options:
      -u, --username        Username.
      -p, --password        Password.
      -d, --database        Database.
      -s, --schema          Schema (default=public).
      -h, --help            Prints this message.
      -q, --queries         Prints internal SQL queries.
      -v, --verbose         Prints detailed output.
      -x, --exec            Executes the DDL commands on slave.


Example
    $ rnx_pg_diff -m 192.168.1.10 -u rednaxel -d rnge3 -s rnx -v
