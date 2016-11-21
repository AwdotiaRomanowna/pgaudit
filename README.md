A simple trigger based audit solution for PostgreSQL >= 9.3.
* With month based partitioning
* All data are kept in a separate audit schema
* All tables in audit schema are inherited from abstract table
* Tables are created automatically after first DML operation in month
* Information about updated row will only be written if anything changed
* Only the OLD value of UPDATE statement will be written to audit table. The main reason was not to lose the data during deploy to already running systems. You can change this behaviour.

Overview
========
The solution is based on dynamic triggers created for every table that undergoes audit. For every audited table a new table in schema audit is created.
All tables in schema audit inherit from an abstract table, which contains tree columns:
* event_time - when event happened (timestamp with time zone); start time of the transaction, not the start time of the statement;
* executed_by - user that has modified the row,
* operation - name of DML operation: INSERT, UPDATE or DELETE that caused the change.

At the time of first DML operation new table, that inherits from the main table, with suffix '_YYYY_MM' is created.

Installation
============
* Just copy&paste the content of pgaudit.sql file to the pgAdmin SQL Editor or psql, or run:
    psql -f pgaudit.sql db_name

Configuration
=============
* Adjust the search path parameter of the functions, i.e. if you want to audit tables only in four schemas (public, audit, dictionaries, crm):
    ALTER FUNCTION audit.insert() SET search_path=public, audit, dictionaries, crm;
* Insert to audit.config all the tables you want to be audited:
    INSERT INTO audit.config(
            schema_name, table_name, enabled)
    VALUES ('public','my_table',TRUE);
* Change the first line in a pgaudit.sql file if you want another user to become an owner of audit schema and all the objects within it:
    SET ROLE TO another_audit_owner;

Caveats
=======
* every DDL change requires analogical change in history schema, i.e. command:
    ALTER TABLE public.distributors ADD COLUMN address varchar(30);
    must be followed by:
    ALTER TABLE audit.public_distributors ADD COLUMN address varchar(30);

