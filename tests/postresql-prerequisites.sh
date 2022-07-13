#!/bin/sh

sudo -u postgres bash -c "\psql -c \"
CREATE ROLE dblib_test WITH \
	LOGIN \
	NOSUPERUSER \
	NOCREATEDB \
	NOCREATEROLE \
	INHERIT \
	NOREPLICATION \
	CONNECTION LIMIT -1 \
	PASSWORD 'dblib_test'; \
\""

sudo -u postgres bash -c "\psql -c \"
CREATE DATABASE dblib_test_db \
    WITH  \
    OWNER = dblib_test \
    ENCODING = 'UTF8' \
    CONNECTION LIMIT = -1; \
\""
