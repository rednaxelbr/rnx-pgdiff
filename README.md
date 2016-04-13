# rnx-pgdiff

This tool connects to 2 databases (master and slave), reverse-engineer their database structures and, with the --exec parameter, executes DLL commands on the slave to make them equal. It can create tables, add columns, add constraints, create or replace procedures and create or replace views.

    -- Rednaxel PostgreSQL Diff Tool - v1.05
    Syntax: rnx_pg_diff -m IP [options]
      -m IP, --master=IP    Master server IP address.
      -l IP, --local=IP     Slave server IP address (default=localhost).
    Options:
      -u, --username        Username.
      -p, --password        Password.
      -d, --database        Database (default=username).
      -s, --schema          Schema (default=public).
      -h, --help            Prints this message.
      -q, --queries         Prints internal SQL queries.
      -v, --verbose         Prints detailed output.
      -x, --exec            Executes the DDL commands on slave.


Example 1: Linux server, slave = localhost, using .pgpass to omit password

    $ ./rnx_pg_diff -m 192.168.1.10 -u rednaxel -d rnge3 -s rnx -v
    
Example 2: Windows workstation, requires slave IP and password

    .\rnx_pg_diff.exe --local=192.168.1.12 -m 192.168.1.10 -p mypassword -u rednaxel -d rnge3 -s rnx -v
