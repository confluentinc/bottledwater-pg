-- Complain if script is sourced in psql, rather than via CREATE EXTENSION.
\echo Use "CREATE EXTENSION samza_postgres" to load this file. \quit

CREATE OR REPLACE FUNCTION samza_table_schema(name) RETURNS text
    AS 'samza_postgres', 'samza_table_schema' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION samza_table_export(name) RETURNS setof bytea
    AS 'samza_postgres', 'samza_table_export' LANGUAGE C VOLATILE STRICT;
