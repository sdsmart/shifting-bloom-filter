/* TODO */
CREATE TYPE cms;

CREATE FUNCTION cms_input(cstring)
    RETURNS cms
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION cms_output(cms)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION cms_receive(internal)
    RETURNS cms
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;
   
CREATE FUNCTION cms_send(cms)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE cms (
    input = cms_input,
    output = cms_output,
    receive = cms_receive,
    send = cms_send,
    storage = extended
);

CREATE FUNCTION new_cms(double precision default 0.001, double precision default 0.99)
    RETURNS cms
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;
	    
CREATE FUNCTION insert_cms(cms, text)
    RETURNS cms
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;	
	
CREATE FUNCTION query_cms(cms, text)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;


/* TODO */
CREATE TYPE bf;

CREATE FUNCTION bf_input(cstring)
    RETURNS bf
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION bf_output(bf)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION bf_receive(internal)
    RETURNS bf
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;
   
CREATE FUNCTION bf_send(bf)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE bf (
    input = bf_input,
    output = bf_output,
    receive = bf_receive,
    send = bf_send,
    storage = extended
);

CREATE FUNCTION new_bf(integer, integer)
    RETURNS bf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;
	    
CREATE FUNCTION insert_bf(bf, cstring)
    RETURNS bf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;	
	
CREATE FUNCTION query_bf(bf, cstring)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;


/* TODO */
CREATE TYPE shbf;

CREATE FUNCTION shbf_input(cstring)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION shbf_output(shbf)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION shbf_receive(internal)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;
   
CREATE FUNCTION shbf_send(shbf)
    RETURNS bytea
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE shbf (
    input = shbf_input,
    output = shbf_output,
    receive = shbf_receive,
    send = shbf_send,
    storage = extended
);


/* TODO */
CREATE FUNCTION new_shbf_m(integer, integer)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;
	    
CREATE FUNCTION insert_shbf_m(shbf, cstring)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;	
	
CREATE FUNCTION query_shbf_m(shbf, cstring)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;


/* TODO */
CREATE FUNCTION new_shbf_a(integer, integer)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;
	    
CREATE FUNCTION insert_shbf_a(shbf, cstring, integer, integer)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;	
	
CREATE FUNCTION query_shbf_a(shbf, cstring)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;


/* TODO */
CREATE FUNCTION new_shbf_x(integer, integer, integer)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;
	    
CREATE FUNCTION insert_shbf_x(shbf, cstring, integer)
    RETURNS shbf
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE;	
	
CREATE FUNCTION query_shbf_x(shbf, cstring)
    RETURNS integer
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

