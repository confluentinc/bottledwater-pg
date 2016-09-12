-- Complain if script is sourced in psql, rather than via CREATE EXTENSION.
\echo Use "CREATE EXTENSION bottledwater" to load this file. \quit

CREATE OR REPLACE FUNCTION bottledwater_key_schema(name) RETURNS text
    AS 'bottledwater', 'bottledwater_key_schema' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION bottledwater_row_schema(name) RETURNS text
    AS 'bottledwater', 'bottledwater_row_schema' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION bottledwater_frame_schema() RETURNS text
    AS 'bottledwater', 'bottledwater_frame_schema' LANGUAGE C VOLATILE STRICT;

DROP DOMAIN IF EXISTS bottledwater_error_policy;
CREATE DOMAIN bottledwater_error_policy AS text
    CONSTRAINT bottledwater_error_policy_valid CHECK (VALUE IN (
        -- these values should match the constants defined in protocol.h
        'log',
        'exit'
    ));

CREATE OR REPLACE FUNCTION bottledwater_export(
        table_pattern text    DEFAULT '%',
        schema_pattern  text     DEFAULT '%',
        allow_unkeyed boolean DEFAULT false,
        error_policy bottledwater_error_policy DEFAULT 'exit',
        order_by text DEFAULT ''
    ) RETURNS setof bytea
    AS 'bottledwater', 'bottledwater_export' LANGUAGE C VOLATILE STRICT;
